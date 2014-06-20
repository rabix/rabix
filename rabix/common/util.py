import six
import copy
import signal
import random
import itertools


def wrap_in_list(val, *append_these):
    """
    >>> wrap_in_list(1, 2)
    [1, 2]
    >>> wrap_in_list([1, 2], 3, 4)
    [1, 2, 3, 4]
    >>> wrap_in_list([1, 2], [3, 4])
    [1, 2, [3, 4]]
    """
    wrapped = val if isinstance(val, list) else [val]
    return wrapped + list(append_these)


def import_name(name):
    name = str(name)
    if '.' not in name:
        return __import__(name)
    chunks = name.split('.')
    var_name = chunks[-1]
    module_name = '.'.join(chunks[:-1])
    fromlist = chunks[:-2] if len(chunks) > 2 else []
    module = __import__(module_name, fromlist=fromlist)
    if not hasattr(module, var_name):
        raise ImportError('%s not found in %s' % (var_name, module_name))
    return getattr(module, var_name)


class DotAccessDict(dict):
    def __getattr__(self, item):
        return self.get(item, None)

    def __setattr__(self, key, value):
        self[key] = value

    def __copy__(self):
        return self.__class__(self)

    def __deepcopy__(self, memo):
        return self.__class__(copy.deepcopy(dict(self), memo))


def intersect_dicts(d1, d2):
    """
    >>> intersect_dicts({'a': 1, 'b': 2}, {'a': 1, 'b': 3})
    {'a': 1}
    >>> intersect_dicts({'a': 1, 'b': 2}, {'a': 0})
    {}
    """
    return {k: v for k, v in six.iteritems(d1) if v == d2.get(k)}


class SignalContextProcessor(object):
    def __init__(self, handler, *signals):
        self.handler = handler
        self.signals = signals
        self.old_handlers = {}

    def __enter__(self):
        self.old_handlers = {
            sig: signal.signal(sig, self.handler) for sig in self.signals
        }

    def __exit__(self, *_):
        for sig in self.signals:
            signal.signal(sig, self.old_handlers[sig])

handle_signal = SignalContextProcessor


def get_import_name(cls):
    return '.'.join([cls.__module__, cls.__name__])


def rnd_name(syllables=5):
    return ''.join(itertools.chain(*zip(
        (random.choice('bcdfghjklmnpqrstvwxz') for _ in range(syllables)),
        (random.choice('aeiou') for _ in range(syllables)))))
