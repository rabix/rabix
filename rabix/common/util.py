import random
import itertools
import collections
import logging
import six
import json
import six.moves.urllib.parse as urlparse

from os.path import abspath, isabs, join

from rabix.common.errors import RabixError

log = logging.getLogger(__name__)


###
# collections
###

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


def dot_update_dict(dst, src):
    for key, val in six.iteritems(src):
        t = dst
        if '.' in key:
            for k in key.split('.'):
                if k == key.split('.')[-1]:
                    if isinstance(val, collections.Mapping):
                        t = t.setdefault(k, {})
                        dot_update_dict(t, src[key])
                    else:
                        t[k] = val
                else:
                    if not isinstance(t.get(k), collections.Mapping):
                        t[k] = {}
                    t = t.setdefault(k, {})
        else:
            if isinstance(val, collections.Mapping):
                t = t.setdefault(key, {})
                dot_update_dict(t, src[key])
            else:
                t[key] = val
    return dst


def map_rec_collection(f, col):
    if isinstance(col, list):
        return [map_rec_collection(f, e) for e in col]
    if isinstance(col, dict):
        return {k: map_rec_collection(f, v) for k, v in six.iteritems(col)}
    return f(col)


def map_rec_list(f, lst):
    if isinstance(lst, list):
        return [map_rec_list(f, e) for e in lst]
    return f(lst)


def map_or_apply(f, lst):
    if isinstance(lst, list):
        return [f(e) for e in lst]
    return f(lst)


###
# reflection
###

def import_name(name):
    name = six.text_type(name)
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


def getmethod(o, method_name, default=None):
    attr = getattr(o, method_name)
    if callable(attr):
        return attr
    return default


###
# misc.
###

def rnd_name(syllables=5):
    return ''.join(itertools.chain(*zip(
        (random.choice('bcdfghjklmnpqrstvwxz') for _ in range(syllables)),
        (random.choice('aeiouy') for _ in range(syllables)))))


def log_level(int_level):
    if int_level <= 0:
        level = logging.WARN
    elif int_level == 1:
        level = logging.INFO
    elif int_level >= 2:
        level = logging.DEBUG
    else:
        raise RabixError("Invalid log level: %s" % int_level)
    return level


def sec_files_naming_conv(path, ext):
    """
    1. If 'ext' begins with one or more caret (^) characters, for each caret,
       remove the last file extension from the path (the last period . and all
       following characters). If there are no file extensions, the path is
       unchanged.
    2. Append the remainder of the 'ext' to the end of the file path.

    >>> sec_files_naming_conv("path", ".ext")
    'path.ext'

    >>> sec_files_naming_conv("path", "^.ext")
    'path.ext'

    >>> sec_files_naming_conv("path.orig", "^.ext")
    'path.ext'

    >>> sec_files_naming_conv("path.orig.tar.gz", "^^.ext")
    'path.orig.ext'

    :param path:
    :param ext:
    :return:
    """
    ext_clean = ext.lstrip('^')
    carets = len(ext) - len(ext_clean)
    if carets > 0:
        exploded = path.split('.')
        part_count = len(exploded)
        if carets >= part_count:
            path = exploded[0]
        else:
            path = '.'.join(exploded[:(part_count - carets)])

    return path + ext_clean


def to_json(obj, fp=None):
    default = lambda o: getmethod(o, '__json__',
                                  lambda v: six.string_types(v))()
    kwargs = dict(default=default, indent=2, sort_keys=True)
    return json.dump(obj, fp, **kwargs) if fp else json.dumps(obj, **kwargs)


def is_url(path):
    return urlparse.urlparse(path).scheme != ""


def to_abspath(path, base=None):
    if not base:
        return abspath(path)
    elif isabs(path) or is_url(path):
        return path
    else:
        return abspath(join(base, path))


def result_str(job_id, outputs):
    result_str = '\nResult for job %s:\n' % job_id
    for name, out in six.iteritems(outputs):
        files = out
        if isinstance(out, list):
            files = ' '.join(['%s' % o for o in out])
        result_str = result_str + '%s: %s\n' % (name, files)
    return result_str
