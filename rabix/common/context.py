import six

from rabix.common.errors import ValidationError


class Context(object):

    def __init__(self, type_map, executor):
        self.type_map = type_map
        self.executor = executor

    def add_type(self, name, constructor):
        self.type_map[name] = constructor

    def from_dict(self, d):
        if d is None:
            return None
        if isinstance(d, list):
            return [self.from_dict(e) for e in d]
        if not isinstance(d, dict):
            return d
        if '@type' not in d:
            return {k: self.from_dict(v) for k, v in six.iteritems(d)}
        type_name = d['@type']
        constructor = self.type_map.get(type_name)
        if not constructor:
            raise ValidationError("Unknown type: %s" % type_name)
        return constructor(self, d)
