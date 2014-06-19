import logging
import functools
import os
import hashlib
import json
import urlparse

# requests is not used if installing as sdk-lib.
try:
    import requests
except ImportError:
    requests = None

from rabix.common.errors import ResourceUnavailable, ValidationError
from rabix.common.protocol import MAPPINGS

log = logging.getLogger(__name__)


def object_hook(obj, resolve_refs=True, parent_url='.'):
    """Used as json.load(s) object_hook for {"$type": "<type>", ...} dicts"""
    if '$$type' not in obj:
        return obj
    if obj.get('$$type', '').startswith('ref/'):
        return resolve_ref(obj, parent_url) if resolve_refs else obj
    return MAPPINGS[obj.pop('$$type')].from_dict(obj)


def from_json(str_or_fp, resolve_refs=True, parent_url='.'):
    """Load json and make classes from certain dicts (see classify() docs)"""
    hook = functools.partial(object_hook, resolve_refs=resolve_refs,
                             parent_url=parent_url)
    if isinstance(str_or_fp, basestring):
        return json.loads(str_or_fp, object_hook=hook)
    return json.load(str_or_fp, object_hook=hook)


def to_json(obj, fp=None):
    default = lambda o: (o.__json__() if callable(getattr(o, '__json__', None))
                         else unicode(o))
    kwargs = dict(default=default, indent=2, sort_keys=True)
    return json.dump(obj, fp, **kwargs) if fp else json.dumps(obj, **kwargs)


def resolve_ref(obj, parent_url='.'):
    url, checksum = obj.get('url'), obj.get('checksum')
    if not url:
        raise ValueError('Cannot resolve ref %s: url must not be empty.' % obj)
    if url.startswith('file://'):
        url = url[len('file://'):]
    url = urlparse.urljoin(parent_url, url)
    log.info('Fetching reference %s' % url)
    if url.startswith('http://') or url.startswith('https://'):
        r = requests.get(url)
        if not r.ok:
            r.raise_for_status()
        return check_ref(r.text, checksum, url, parent_url)
    if '://' not in url:
        if not os.path.isfile(url):
            raise ResourceUnavailable(
                'File not found: %s' % os.path.abspath(url)
            )
        with open(url) as fp:
            contents = fp.read()
        return check_ref(contents, checksum, url, parent_url)
    raise ValueError('Unsupported schema for URL %s' % url)


def check_ref(text, checksum, url, parent_url):
    if checksum and hashlib.md5(text).hexdigest() != checksum:
        raise ValidationError('Checksum not a match for url %s' % url)
    return from_json(text, resolve_refs=True, parent_url=parent_url)


def from_url(url):
    if url.startswith('file://'):
        url = url[len('file://'):]
    if '://' not in url:
        if not os.path.isfile(url):
            raise ResourceUnavailable(
                'File not found: %s' % os.path.abspath(url)
            )
        with open(url) as fp:
            contents = fp.read()
        return from_json(contents, parent_url=url)
    return resolve_ref({'$$type': 'ref/', 'url': url}, parent_url=url)
