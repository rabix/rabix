import itertools
import re
import os
import json
import logging
import tempfile
import shutil
import copy

from rabix.common import six
from rabix.common.util import DotAccessDict, intersect_dicts

log = logging.getLogger(__name__)


def default_attr_name(name):
    if not isinstance(name, six.string_types):
        name = str(name)
    if name.startswith('-') or name.isupper():
        return name
    return name.replace('_', ' ').capitalize()


def validator(func):
    def f(val, *args, **kwargs):
        if not kwargs.pop('list'):
            return func(val, *args, **kwargs)
        for v in val:
            func(v, *args, **kwargs)
    return f


@validator
def validate_type(val, t_tuple, **_):
    if val is None:
        return
    if not isinstance(val, t_tuple):
        raise ValueError(
            'Expected %s, got %s' % (
                ' or '.join([t.__name__ for t in t_tuple]),
                type(val).__name__
            )
        )


@validator
def validate_range(val, min=None, max=None, **_):
    if val is None:
        return
    if min is not None and val < min:
        raise ValueError('Value cannot be less than %s. Got %s.' % (min, val))
    if max is not None and val > max:
        raise ValueError('Value cannot be more than %s. Got %s.' % (max, val))


@validator
def validate_pattern(val, pattern, **_):
    if val is None or pattern is None:
        return
    if not re.match(pattern, val):
        raise ValueError(
            'Value "%s" does not match pattern "%s."' % (val, pattern)
        )


@validator
def validate_enum(val, valid_vals, **_):
    if val is None:
        return
    if val not in valid_vals:
        raise ValueError('Invalid value. Expected %s. Got %s.' %
                         (' or '.join(valid_vals), val))


@validator
def validate_file_type(val, types, **_):
    if val is None:
        return
    file_type, types = val.meta.file_type, types
    if types and file_type not in types:
        raise ValueError('File type is %s. Expected %s' %
                         (file_type, ' or '.join(types)))


@validator
def validate_file_exists(val, **_):
    if val is None:
        return
    if not os.path.exists(val.file):
        raise ValueError('File does not exist: %s' % val.file)


@validator
def validate_struct(val, **_):
    if val is None:
        return
    errors = val._validate()
    if errors:
        raise ValueError(str(errors))


class BaseAttr(object):
    _count = itertools.count(0)  # Total number of schema attributes defined

    def __init__(self, name=None, description='', required=False, list=False,
                 **extra):
        self._order = next(BaseAttr._count)
        self.type = None
        self.name = name
        self.description = description
        self.required = required
        self.list = list
        self._extra = extra

    @property
    def extra(self):
        return self._extra

    def __call__(self, attr_name, data=None):
        if attr_name.startswith('_'):
            raise ValueError(
                'Input, output and parameter IDs cannot start with "_" (%s)' %
                attr_name
            )
        if not self.name:
            self.name = default_attr_name(attr_name)
        if not self.description:
            self.description = self.name
        return data

    def validate(self, instance):
        if self.list and not isinstance(instance, (tuple, list, IOList)):
            raise ValueError('Value must be tuple or list.')
        if not self.required:
            return
        if instance is None:
            raise ValueError('You must specify a value.')
        if self.list and not instance:
            raise ValueError('You must specify a value.')

    def _get_schema_list(self):
        schema = {
            'type': 'array' if self.required else ['array', 'null'],
            'items': self._get_schema_single(as_item=True),
        }
        if self.required:
            schema['minItems'] = 1
        return schema

    def _get_schema_single(self, as_item=False):
        t = self.type if self.required or as_item else [self.type, 'null']
        return {'type': t}

    def get_schema(self):
        schema = self._get_schema_list() \
            if self.list else self._get_schema_single()
        schema.update({
            'title': self.name,
            'description': self.description
        })
        return schema


class BaseParam(BaseAttr):
    def __init__(self, name=None, description='', required=False, default=None,
                 category=None, list=False, **extra):
        BaseAttr.__init__(self, name, description, required, list, **extra)
        default = [] if list and default is None else default
        self.default = default() if callable(default) else default
        self.category = category

    def __call__(self, attr_name, data=None):
        BaseAttr.__call__(self, attr_name, data)
        data = self.default if data is None else data
        if self.list and not isinstance(data, (list, tuple)):
            data = [] if data is None else [data]
        return data


class IntAttr(BaseParam):
    def __init__(self, name=None, description='', required=False, default=None,
                 category=None, list=False, min=None, max=None, **extra):
        BaseParam.__init__(self, name, description, required, default,
                           category, list, **extra)
        self.type = 'integer'
        self.min = min
        self.max = max

    def validate(self, instance):
        BaseParam.validate(self, instance)
        validate_type(instance, six.integer_types, list=self.list)
        validate_range(instance, self.min, self.max, list=self.list)

    def _get_schema_single(self, as_item=False):
        schema = super(IntAttr, self)._get_schema_single(as_item)
        if self.min is not None:
            schema['minimum'] = self.min
        if self.max is not None:
            schema['maximum'] = self.max
        return schema


class RealAttr(IntAttr):
    def __init__(self, name=None, description='', required=False, default=None,
                 category=None, list=False, min=None, max=None, **extra):
        IntAttr.__init__(
            self, name, description, required, default,
            category, list, min, max, **extra
        )
        self.type = 'number'

    def validate(self, instance):
        BaseParam.validate(self, instance)
        validate_type(instance, (float,) + six.integer_types, list=self.list)
        validate_range(instance, self.min, self.max, list=self.list)


class StringAttr(BaseParam):
    def __init__(self, name=None, description='', required=False, default=None,
                 category=None, list=False, pattern=None, **extra):
        BaseParam.__init__(self, name, description, required, default,
                           category, list, **extra)
        self.type = 'string'
        self.pattern = pattern

    def validate(self, instance):
        BaseParam.validate(self, instance)
        validate_type(instance, (six.string_types,), list=self.list)
        validate_pattern(instance, self.pattern, list=self.list)

    def _get_schema_single(self, as_item=False):
        schema = super(StringAttr, self)._get_schema_single(as_item)
        if self.pattern:
            schema['pattern'] = self.pattern
        return schema


class BoolAttr(BaseParam):
    def __init__(self, name=None, description='', required=False, default=None,
                 category=None, list=False, **extra):
        BaseParam.__init__(self, name, description, required, default,
                           category, list, **extra)
        self.type = 'boolean'

    def validate(self, instance):
        BaseParam.validate(self, instance)
        validate_type(instance, (bool,), list=self.list)


class EnumAttr(BaseParam):
    def __init__(self,  values, name=None, description='', required=False,
                 default=None, category=None, list=False, **extra):
        BaseParam.__init__(self, name, description, required, default,
                           category, list, **extra)
        self.type = 'enum'
        self.values = [self._expand_value(v) for v in values]

    def _expand_value(self, val):
        error = ValueError(
            'Please specify a 3-tuple with id, name and description.'
        )
        d = default_attr_name
        if not isinstance(val, (tuple, list)):
            return val, d(val), d(val)
        l = len(val)
        if not l:
            raise error
        elif l == 1:
            return val[0], d(val[0]), d(val[0])
        elif l == 2:
            if not isinstance(val[1], six.string_types):
                raise error
            return val[0], val[1], d(val[0])
        elif l == 3:
            if (not isinstance(val[1], six.string_types) or
                    not isinstance(val[2], six.string_types)):
                raise error
            return val
        else:
            raise error

    def validate(self, instance):
        BaseParam.validate(self, instance)
        validate_enum(instance, [v[0] for v in self.values], list=self.list)

    def _get_schema_single(self, as_item=False):
        schema = super(EnumAttr, self)._get_schema_single(as_item)
        schema.pop('type')
        schema['enum'] = self.values + ([None] if not self.required else [])
        return schema


class StructAttr(BaseParam):
    def __init__(self, schema, item_label=None, name=None, description='',
                 required=False, default=None, category=None, list=False,
                 **extra):
        BaseParam.__init__(self, name, description, required, default,
                           category, list, **extra)
        self.schema = schema
        self.item_label = item_label or schema.__name__
        self.type = 'object'

    def validate(self, instance):
        BaseParam.validate(self, instance)
        validate_type(instance, (self.schema,), list=self.list)
        validate_struct(instance, list=self.list)

    def __call__(self, attr_name, data=None):
        to_dict = lambda o: o.__json__() if hasattr(o, '__json__') else o or {}
        BaseParam.__call__(self, attr_name, data)
        if not self.list:
            return self.schema(**to_dict(data))
        if not data:
            return []
        return [self.schema(**to_dict(item)) for item in data]

    def _get_schema_single(self, as_item=False):
        schema = self.schema._get_schema()
        if not as_item and not self.required:
            schema['type'] = ['object', None]
        return schema


class SchemaBased(object):
    def __init__(self, **kwargs):
        for attr, attr_def in six.iteritems(self._attr_defs()):
            setattr(self, attr, attr_def(attr, kwargs.get(attr)))

    @classmethod
    def _attr_defs(cls):
        result = {}
        for key in dir(cls):
            val = getattr(cls, key)
            if isinstance(val, BaseAttr):
                result[key] = val
        return result

    @classmethod
    def _get_schema(cls):
        adefs = cls._attr_defs()
        properties = {
            key: val.get_schema() for key, val in six.iteritems(adefs)
        }
        required = [key for key, val in six.iteritems(adefs) if val.required]
        return {
            'type': 'object',
            'required': required,
            'properties': properties,
        }

    def _validate(self, assert_=False):
        errors = {}
        for attr, attr_def in six.iteritems(self._attr_defs()):
            try:
                attr_def.validate(getattr(self, attr))
            except (ValueError, TypeError) as e:
                errors[attr] = str(e)
        if assert_:
            # noinspection PyStringFormat
            assert not errors, 'Following errors encountered: %s' % errors
        return errors

    def _definitions(self):
        result = {}
        for id, attr in six.iteritems(self.__class__._attr_defs()):
            new = copy.deepcopy(attr)
            new.value = getattr(self, id)
            new.id = id
            result[id] = new
        return result

    def __json__(self):
        return {
            k: v
            for k, v in six.iteritems(self.__dict__)
            if k[0] != '_' and v is not None
        }

    __unicode__ = __repr__ = __str__ = lambda self: str(self.__json__())

    def __iter__(self):
        for k, v in six.iteritems(self._definitions()):
            yield v

    def __getitem__(self, item):
        return self.__json__()[item]


class IODef(SchemaBased):
    def __setattr__(self, key, value):
        attr_def = getattr(self.__class__, key)
        if not attr_def:
            raise AttributeError(
                '%s does not define %s' % (self.__class__.__name__, key)
            )
        if value is None:
            return object.__setattr__(self, key, None)
        if attr_def.list and isinstance(value, list):
            return SchemaBased.__setattr__(self, key, IOList(value))
        if not attr_def.list and not isinstance(value, IOValue):
            return SchemaBased.__setattr__(self, key, IOValue(value))
        if isinstance(value, (IOValue, IOList)):
            return SchemaBased.__setattr__(self, key, value)
        raise ValueError('Unsupported type: %s' % type(value))

    def _save_meta(self):
        for v in self:
            if v.value:
                v.value._save_meta()

    def _load_meta(self):
        for v in self:
            val = getattr(self, v.id)
            if val and hasattr(val, '_load_meta'):
                val._load_meta()


class IOValue(six.text_type):
    meta = None

    def __new__(cls, value=None):
        if isinstance(value, list):
            value = value[0] if value else ''
        value = os.path.abspath(value) if value else value
        obj = (six.text_type.__new__(cls) if not value
               else six.text_type.__new__(cls, value))
        obj.meta = DotAccessDict()
        return obj

    file = property(lambda self: self)

    def _load_meta(self):
        metadata_file = self + '.meta'
        if not os.path.exists(metadata_file):
            log.warning('No metadata file found: %s', metadata_file)
            self.meta = DotAccessDict()
            return
        log.debug('Loading metadata from %s', metadata_file)
        with open(metadata_file) as f:
            content = json.load(f)
        if not isinstance(content, dict):
            log.error('Metadata not a dict: %s', content)
            self.meta = DotAccessDict()
        else:
            self.meta = DotAccessDict(**content)

    def _save_meta(self):
        if not self.file:
            return
        metadata_file = self.file + '.meta'
        log.debug('Saving metadata to %s', metadata_file)
        if not isinstance(self.meta, dict):
            log.error('Metadata not a dict: %s', self.meta)
            self.meta = {}
        if not os.path.exists(metadata_file):
            with open(metadata_file, 'w') as f:
                json.dump(self.meta or {}, f)
            return
        tmp = tempfile.mktemp()
        with open(tmp, 'w') as f:
            json.dump(self.meta or {}, f)
        shutil.move(tmp, metadata_file)

    @property
    def size(self):
        return os.stat(self).st_size if os.path.exists(self) else 0

    def make_metadata(self, **kwargs):
        result = copy.deepcopy(self.meta)
        result.update(**kwargs)
        return result


class IOAttr(BaseAttr):
    def __init__(self, name=None, description='', required=False,
                 list=False, **extra):
        BaseAttr.__init__(self, name, description, required, list=list, **extra)
        self.type = 'string'

    def __call__(self, attr_name, data=None):
        data = BaseAttr.__call__(self, attr_name, data)
        return IOList(data) if self.list else (IOValue(data) if data else None)

    def validate(self, instance):
        BaseAttr.validate(self, instance)
        if instance:
            validate_file_exists(instance, list=self.list)
        elif self.required:
            raise ValueError('This input is required')


class IOList(object):
    def __init__(self, value=None):
        value = value or []
        if isinstance(value, six.string_types):
            value = [value]
        self._values = [IOValue(v) for v in value or []]

    def __len__(self):
        return len(self._values)

    def __iter__(self):
        return iter(self._values)

    def __getitem__(self, i):
        return self._values[i]

    def __json__(self):
        return self._values

    __str__ = __unicode__ = __repr__ = lambda self: str(self._values)

    def add_file(self, path=None):
        """Add new file to list of outputs and return the object."""
        value = IOValue(path)
        self._values.append(value)
        return value

    def make_metadata(self, **kwargs):
        """
        Intersect metadata dictionaries of each file in list
        and return the new metadata object.
        """
        if not self._values:
            return DotAccessDict(**kwargs)
        if len(self._values) == 1:
            return self[0].make_metadata(**kwargs)
        return DotAccessDict(
            reduce(intersect_dicts, [v.meta for v in self._values]), **kwargs
        )

    def _save_meta(self):
        for val in self:
            val._save_meta()

    def _load_meta(self):
        for val in self:
            val._load_meta()
