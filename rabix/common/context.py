import six


class Context(object):

    def __init__(self, executor, type_map=None):
        self.type_map = type_map or {}
        self.executor = executor
        self.requirements = []
        self.hints = []
        self._index = {}

    def add_type(self, name, constructor):
        self.type_map[name] = constructor

    def build_from_document(self, d):
        for req in d.get('requirements', []):
            self.requirements.append(self.from_dict(req))

        for req in d.get('hints', []):
            self.hints.append(self.from_dict(req))

    def index(self, obj):
        id = None
        if isinstance(obj, dict):
            id = obj.get('id')
        elif hasattr(obj, 'id'):
            id = obj.id

        if not id:
            raise ValueError('%s not indexable' % obj)

        self._index[id] = obj

    def resolve(self, id):
        return self._index.get(id)

    def get_requirement(self, t):
        return [r for r in self.requirements if isinstance(r, t)]

    def get_hint(self, t):
        return [r for r in self.hints if isinstance(r, t)]

    def get_hint_or_requirement(self, t):
        return self.get_hint(t) + self.get_requirement(t)

    def from_dict(self, d):
        if d is None:
            return None
        if isinstance(d, list):
            return [self.from_dict(e) for e in d]
        if not isinstance(d, dict):
            return d

        # handle dicts
        if 'class' not in d:
            result = {k: self.from_dict(v) for k, v in six.iteritems(d)}
        else:
            type_name = d['class']
            constructor = self.type_map.get(type_name)
            if not constructor:
                result = d
            else:
                result = constructor(self, d)

        if 'id' in d:
            self._index[d['id']] = result

        return result

    def to_primitive(self, o):
        if o is None:
            return None
        if isinstance(o, dict):
            return {k: self.to_primitive(v) for k, v in six.iteritems(o)}
        if isinstance(o, list):
            return [self.to_primitive(e) for e in o]
        if hasattr(o, 'to_dict'):
            return o.to_dict(self)
        if isinstance(o, (int, float, bool, six.string_types)):
            return o
        raise RuntimeError("can't transform %s to dict" % o)
