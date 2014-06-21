import os
import json
import copy
import logging
import collections
from rabix.common.errors import ResourceUnavailable

from rabix.common import six
# noinspection PyUnresolvedReferences
from rabix.common.six.moves.urllib import parse as urlparse
from rabix.common.protocol import MAPPINGS

# requests is not used if installing as sdk-lib.
try:
    import requests
except ImportError:
    requests = None

log = logging.getLogger(__name__)


class NormDict(dict):
    def __init__(self, normalize=unicode):
        super(NormDict, self).__init__()
        self.normalize = normalize

    def __getitem__(self, key):
        return super(NormDict, self).__getitem__(self.normalize(key))

    def __setitem__(self, key, value):
        return super(NormDict, self).__setitem__(self.normalize(key), value)

    def __delitem__(self, key):
        return super(NormDict, self).__delitem__(self.normalize(key))


class JsonLoader(object):
    def __init__(self):
        self.fetched = NormDict(lambda url: urlparse.urlsplit(url).geturl())
        self.resolved = NormDict(lambda url: urlparse.urlsplit(url).geturl())

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
        doc_url, pointer = urlparse.urldefrag(url)
        document = self.fetch(doc_url)
        fragment = copy.deepcopy(self.resolve_pointer(document, pointer))
        result = self.resolve_all(fragment, doc_url)
        self.resolved[url] = result
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

    @staticmethod
    def resolve_pointer(document, pointer):
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
