from rabix import CONFIG
from rabix.common.util import import_name

_engine = None


def get_engine(**kwargs):
    global _engine
    if _engine:
        return _engine
    options = CONFIG['engine'].get('options', {})
    _engine = import_name(CONFIG['engine']['class'])(**dict(options, **kwargs))
    return _engine
