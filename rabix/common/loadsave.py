import os
import json
import copy
import hashlib
import logging
import collections

from rabix.common import six
# noinspection PyUnresolvedReferences
from rabix.common.six.moves.urllib import parse as urlparse
from rabix.common.protocol import MAPPINGS
from rabix.common.util import NormDict
from rabix.common.errors import ResourceUnavailable, ValidationError

# requests is not used if installing as sdk-lib.
try:
    import requests
except ImportError:
    requests = None

log = logging.getLogger(__name__)


class JsonLoader(object):
    def __init__(self):
        normalize = lambda url: urlparse.urlsplit(url).geturl()
        self.fetched = NormDict(normalize)
        self.resolved = NormDict(normalize)
        self.resolving = NormDict(normalize)

    def load(self, url):
        base_url = 'file://%s/' % os.path.abspath('.')
        document = self.resolve_ref({'$ref': url}, base_url)
        return self.classify(document)

    def classify(self, document):
        if isinstance(document, list):
            return [self.classify(x) for x in document]
        if isinstance(document, dict):
            new = {k: self.classify(v) for k, v in six.iteritems(document)}
            if '$$type' in new:
                return MAPPINGS[new.pop('$$type')].from_dict(new)
            else:
                return new
        return document

    def resolve_ref(self, obj, base_url):
        url = urlparse.urljoin(base_url, obj['$ref'])
        if url in self.resolved:
            return self.resolved[url]
        if url in self.resolving:
            raise ValidationError('Circular reference for url %s' % url)
        self.resolving[url] = True
        doc_url, pointer = urlparse.urldefrag(url)
        document = self.fetch(doc_url)
        fragment = copy.deepcopy(self.resolve_pointer(document, pointer))
        self.verify_checksum(obj.get('checksum'), fragment)
        result = self.resolve_all(fragment, doc_url)
        self.resolved[url] = result
        del self.resolving[url]
        return result

    def resolve_all(self, document, base_url):
        if isinstance(document, list):
            iterator = enumerate(document)
        elif isinstance(document, dict):
            if '$ref' in document:
                return self.resolve_ref(document, base_url)
            iterator = six.iteritems(document)
        else:
            return document
        for key, val in iterator:
            document[key] = self.resolve_all(val, base_url)
        return document

    def fetch(self, url):
        if url in self.fetched:
            return self.fetched[url]
        split = urlparse.urlsplit(url)
        scheme, path = split.scheme, split.path

        if scheme in ['http', 'https'] and requests:
            resp = requests.get(url)
            try:
                resp.raise_for_status()
            except Exception as e:
                raise ResourceUnavailable(url, cause=e)
            result = resp.json()
        elif scheme == 'file':
            try:
                with open(path) as fp:
                    result = json.load(fp)
            except (OSError, IOError) as e:
                raise ResourceUnavailable(url, cause=e)
        else:
            raise ValueError('Unsupported scheme: %s' % scheme)
        self.fetched[url] = result
        return result

    def verify_checksum(self, checksum, document):
        if not checksum:
            return
        try:
            hash_method, hexdigest = checksum.split('$')
        except ValueError:
            raise ValidationError('Bad checksum format: %s' % checksum)
        if hexdigest != self.checksum(document, hash_method):
            raise ValidationError('Checksum does not match: %s' % checksum)

    def checksum(self, document, method='sha1'):
        if method not in ('md5', 'sha1'):
            raise NotImplementedError(
                'Unsupported hash method: %s' % method
            )
        normalized = json.dumps(document, sort_keys=True, separators=(',', ':'))
        return getattr(hashlib, method)(normalized).hexdigest()

    def resolve_pointer(self, document, pointer):
        parts = urlparse.unquote(pointer.lstrip('/')).split('/') \
            if pointer else []
        for part in parts:
            if isinstance(document, collections.Sequence):
                try:
                    part = int(part)
                except ValueError:
                    pass
            try:
                document = document[part]
            except:
                raise ValueError('Unresolvable JSON pointer: %r' % pointer)
        return document

loader = JsonLoader()


def to_json(obj, fp=None):
    default = lambda o: (o.__json__() if callable(getattr(o, '__json__', None))
                         else six.text_type(o))
    kwargs = dict(default=default, indent=2, sort_keys=True)
    return json.dump(obj, fp, **kwargs) if fp else json.dumps(obj, **kwargs)


def from_url(url):
    return loader.load(url)
