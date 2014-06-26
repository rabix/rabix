import os
import copy
import signal
import random
import itertools
import json
import collections
import logging

from rabix import CONFIG
from rabix.common import six

log = logging.getLogger(__name__)


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


def update_dict(cfg, new_cfg):
    for key, val in six.iteritems(new_cfg):
        t = cfg
        if '.' in key:
            for k in key.split('.'):
                if k == key.split('.')[-1]:
                    if isinstance(val, collections.Mapping):
                        t = t.setdefault(k, {})
                        update_dict(t, new_cfg[key])
                    else:
                        t[k] = val
                else:
                    if not isinstance(t.get(k), collections.Mapping):
                        t[k] = {}
                    t = t.setdefault(k, {})
        else:
            if isinstance(val, collections.Mapping):
                t = t.setdefault(key, {})
                update_dict(t, new_cfg[key])
            else:
                t[key] = val


def update_config(cfg=CONFIG, path='rabix.conf'):
    if not os.path.isfile(path):
        log.error('Config not found: %s', path)
        return
    with open(path) as f:
        try:
            f_cfg = json.load(f)
        except Exception as e:
            log.info('Invalid json %s: %s', path, e)
            return
    update_dict(cfg, f_cfg)


class DotAccessDict(dict):
    def __getattr__(self, item):
        return self.get(item, None)

    def __setattr__(self, key, value):
        self[key] = value

    def __copy__(self):
        return self.__class__(self)

    def __deepcopy__(self, memo):
        return self.__class__(copy.deepcopy(dict(self), memo))


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
        (random.choice('aeiouy') for _ in range(syllables)))))
