import functools
import os
import hashlib
import json
import logging
import urlparse
import requests

from rabix.common.errors import ResourceUnavailable, ValidationError

log = logging.getLogger(__name__)
MAPPINGS = {}


class JobError(RuntimeError):
    def __json__(self):
        return {'$$type': 'error', 'message': self.message or str(self)}

    @classmethod
    def from_dict(cls, d):
        return cls(d.get('message', ''))


class Resources(object):
    CPU_NEGLIGIBLE, CPU_SINGLE, CPU_ALL = 0, 1, -1

    def __init__(self, mem_mb=100, cpu=CPU_ALL, high_io=False):
        self.mem_mb = mem_mb
        self.cpu = cpu
        self.high_io = high_io

    __str__ = __unicode__ = __repr__ = lambda self: 'Resources(%s, %s, %s)' % (self.mem_mb, self.cpu, self.high_io)

    def __call__(self, obj):
        obj._requirements = self
        return obj

    def __json__(self):
        return {
            '$$type': 'resources',
            'mem_mb': self.mem_mb,
            'cpu': self.cpu,
            'high_io': self.high_io,
        }

    @classmethod
    def from_dict(cls, d):
        return cls(**d)


class Outputs(object):
    def __init__(self, outputs_dict=None):
        self.outputs = outputs_dict
        for k, v in self.outputs.items():
            if isinstance(v, basestring):
                self.outputs[k] = [v]
            elif v is None:
                self.outputs[k] = []
            else:
                self.outputs[k] = list(v)

    __str__ = __unicode__ = __repr__ = lambda self: 'Outputs[%s]' % self.outputs

    def __json__(self):
        return {
            '$$type': 'outputs',
            'outputs': self.outputs,
        }

    @classmethod
    def from_dict(cls, d):
        return cls(outputs_dict=d.get('outputs', {}))


class BaseJob(object):
    def __init__(self, job_id=None, args=None, resources=None, context=None):
        self.job_id = job_id
        self.args = args or {}
        self.resources = resources or Resources()
        self.context = context or {}

    __str__ = __unicode__ = __repr__ = lambda self: 'BaseJob[%s]' % self.job_id

    def __json__(self):
        return {
            '$$type': 'job',
            'job_id': self.job_id,
            'args': self.args,
            'resources': self.resources,
        }


class Job(BaseJob):
    def __init__(self, wrapper_id, job_id=None, args=None, resources=None, context=None):
        BaseJob.__init__(self, job_id, args, resources, context)
        self.wrapper_id = wrapper_id

    __str__ = __unicode__ = __repr__ = lambda self: 'Job[%s]' % self.job_id

    def __json__(self):
        return dict(BaseJob.__json__(self), wrapper_id=self.wrapper_id)

    @classmethod
    def from_dict(cls, obj):
        return cls(wrapper_id=obj['wrapper_id'], job_id=obj.get('job_id'), args=obj.get('args'),
                   resources=obj.get('resources'), context=obj.get('context'))


def object_hook(obj, resolve_refs=True, parent_url='.'):
    """ Used as json.load(s) object_hook for {"$type": "<type>", ...} dicts """
    if '$$type' not in obj:
        return obj
    if obj.get('$$type', '').startswith('ref/'):
        return resolve_ref(obj, parent_url) if resolve_refs else obj
    return MAPPINGS[obj.pop('$$type')].from_dict(obj)


def from_json(str_or_fp, resolve_refs=True, parent_url='.'):
    """ Load json and make classes from certain dicts (see classify() docs) """
    hook = functools.partial(object_hook, resolve_refs=resolve_refs, parent_url=parent_url)
    if isinstance(str_or_fp, basestring):
        return json.loads(str_or_fp, object_hook=hook)
    return json.load(str_or_fp, object_hook=hook)


def to_json(obj, fp=None):
    default = lambda o: o.__json__() if callable(getattr(o, '__json__', None)) else unicode(o)
    kwargs = dict(default=default, indent=2, sort_keys=True)
    return json.dump(obj, fp, **kwargs) if fp else json.dumps(obj, **kwargs)


def resolve_ref(obj, parent_url='.'):
    t, url, checksum = obj['$$type'][4:], obj.get('url'), obj.get('checksum')
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
        return check_ref(r.text, t, checksum, url, parent_url)
    if '://' not in url:
        if not os.path.isfile(url):
            raise ResourceUnavailable('File not found: %s' % os.path.abspath(url))
        with open(url) as fp:
            return check_ref(fp.read(), t, checksum, url, parent_url)
    raise ValueError('Unsupported schema for URL %s' % url)


def check_ref(text, _, checksum, url, parent_url):
    if checksum and hashlib.md5(text).hexdigest() != checksum:
        raise ValidationError('Checksum not a match for url %s' % url)
    return from_json(text, resolve_refs=True, parent_url=parent_url)


def from_url(url):
    return resolve_ref({'$$type': 'ref/', 'url': url}, parent_url=url)


MAPPINGS.update({
    'job': Job,
    'resources': Resources,
    'outputs': Outputs,
    'error': JobError,
})
