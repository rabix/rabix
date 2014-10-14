import logging

import six

log = logging.getLogger(__name__)


class JobError(RuntimeError):
    def __json__(self):
        return {
            '$$type': 'error',
            'message': self.message or six.text_type(self)
        }

    @classmethod
    def from_dict(cls, d):
        return cls(d.get('message', ''))


class Resources(object):
    CPU_NEGLIGIBLE, CPU_SINGLE, CPU_ALL = 0, 1, -1

    def __init__(self, mem_mb=100, cpu=CPU_ALL, high_io=False):
        self.mem_mb = mem_mb
        self.cpu = cpu
        self.high_io = high_io

    __str__ = __unicode__ = __repr__ = lambda self: (
        'Resources(%s, %s, %s)' % (self.mem_mb, self.cpu, self.high_io)
    )

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
            if isinstance(v, six.string_types):
                self.outputs[k] = [v]
            elif v is None:
                self.outputs[k] = []
            else:
                self.outputs[k] = list(v)

    __str__ = __unicode__ = __repr__ = lambda self: (
        'Outputs[%s]' % self.outputs
    )

    def __json__(self):
        return {
            '$$type': 'outputs',
            'outputs': self.outputs,
        }

    @classmethod
    def from_dict(cls, d):
        return cls(outputs_dict=d.get('outputs', {}))
