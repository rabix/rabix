import os
import six
import json
import logging
import time
import datetime

from uuid import uuid4
from jsonschema.validators import Draft4Validator
from six.moves.urllib.parse import urlparse, urlunparse, unquote
from base64 import b64decode

from rabix.common.errors import ValidationError


log = logging.getLogger(__name__)


class App(object):

    def __init__(self, app_id, inputs, outputs, app_description=None,
                 annotations=None, platform_features=None):
        self.id = app_id
        self.inputs = inputs
        self.outputs = outputs
        self.app_description = app_description
        self.annotations = annotations
        self.platform_features = platform_features
        self._inputs = {io.id: io for io in inputs}
        self._outputs = {io.id: io for io in outputs}

    def install(self, *args, **kwargs):
        pass

    def run(self, job):
        raise NotImplementedError("Method 'run' is not implemented"
                                  " in the App class")

    def get_input(self, name):
        return self._inputs.get(name)

    def get_output(self, name):
        return self._outputs.get(name)

    def validate_inputs(self, input_values):
        for inp in self.inputs:
            if inp.id in input_values:
                if not inp.validate(input_values[inp.id]):
                    return False
            elif inp.required:
                return False
        return True

    def job_dump(self, job, dirname):
        with open(os.path.join(dirname, 'job.cwl.json'), 'w') as f:
            job_dict = job.to_dict()
            json.dump(job_dict, f)
            log.info('File %s created.', job.id)

    def to_dict(self, context):
        return {
            '@id': self.id,
            '@type': 'App',
            'inputs': [context.to_dict(inp) for inp in self.inputs],
            'outputs': [context.to_dict(outp) for outp in self.outputs],
            'appDescription': self.app_description,
            'annotations': self.annotations,
            'platformFeatures': self.platform_features
        }


class URL(object):

    def __init__(self, url):
        (self.scheme, self.netloc, self.path,
         self.params, self.query, self.fragment) = urlparse(url, 'file')

        self.content_type = None
        self.charset = None
        self.data = None
        if self.scheme == 'data':
            meta, data = self.path.split(',')
            meta_parts = meta.split(';')
            self.content_type = meta_parts[0]
            b64 = 'base64' in meta_parts
            if b64:
                self.data = b64decode(data)
            else:
                self.data = unquote(data)

    def islocal(self):
        return self.scheme == 'file'

    def geturl(self):
        return urlunparse(
            (self.scheme, self.netloc, self.path,
             self.params, self.query, self.fragment)
        )

    def isdata(self):
        return self.scheme == 'data'

    def __str__(self):
        if self.islocal():
            return self.path
        else:
            return self.geturl()


class File(object):

    name = 'file'

    @classmethod
    def match(cls, val):
        return isinstance(val, dict) and 'path' in val

    def __init__(self, path, size=None, meta=None, secondary_files=None):
        self.size = size
        self.meta = meta or {}
        self.secondary_files = secondary_files or []
        self.url = None

        if isinstance(path, dict):
            self.from_dict(path)
        else:
            self.path = path

    def from_dict(self, val):
        size = val.get('size')
        if size is not None:
            size = int(size)

        path = val.get('path')
        if path is None:
            raise ValidationError(
                "Not a valid 'File' object: %s" % str(val)
            )

        self.size = self.size or size
        self.meta = self.meta or val.get('metadata')
        self.secondary_files = self.secondary_files or \
            [File(sf) for sf in val.get('secondaryFiles', [])]
        self.path = path

    def to_dict(self, context=None):
        return {
            "@type": "File",
            "path": self.path,
            "size": self.size,
            "metadata": self.meta,
            "secondaryFiles": [sf.to_dict(context)
                               for sf in self.secondary_files]
        }

    @property
    def path(self):
        return six.text_type(self.url)

    def __str__(self):
        return self.path

    def __repr__(self):
        return "File(" + six.text_type(self.to_dict()) + ")"

    @path.setter
    def path(self, val):
        self.url = URL(val) if isinstance(val, six.string_types) else val


def make_constructor(schema):

        one_of = schema.get('oneOf')
        if one_of:
            return OneOfConstructor(one_of)

        type_name = schema.get('type')

        if not type_name:
            return lambda x: x

        if type_name == 'array':
            item_constructor = make_constructor(schema.get('items', {}))
            return ArrayConstructor(item_constructor)

        if type_name == 'object':
            return ObjectConstructor(schema.get('properties', {}))

        if type_name == 'file' or type_name == 'directory':
            return File

        return PrimitiveConstructor(type_name)


class ArrayConstructor(object):

    def __init__(self, item_constructor):
        self.item_constructor = item_constructor
        self.name = 'array'

    def __call__(self, val):
        return [self.item_constructor(v) for v in val]

    def match(self, val):
        return isinstance(val, list) and all(
            [self.item_constructor.match(v) for v in val])


class ObjectConstructor(object):

    def __init__(self, properties):
        self.name = 'object'
        self.properties = {
            k: make_constructor(v)
            for k, v in six.iteritems(properties)
        }

    def __call__(self, val):
        return {
            k: self.properties.get(k, lambda x: x)(v)
            for k, v in six.iteritems(val)
        }

    def match(self, val):
        return isinstance(val, dict) and all(
            [k in self.properties and self.properties[k].match(v)
             for k, v in six.iteritems(val)])


class PrimitiveConstructor(object):

    CONSTRUCTOR_MAP = {
        'integer': int,
        'number': float,
        'boolean': bool,
        'string': six.text_type
    }

    def __init__(self, type_name):
        self.name = type_name
        self.type = PrimitiveConstructor.CONSTRUCTOR_MAP.get(type_name)

    def __call__(self, val):
        return self.type(val)

    def match(self, val):
        return isinstance(val, self.type)


class OneOfConstructor(object):

    def __init__(self, options):
        self.options = [make_constructor(opt) for opt in options]

    def match(self, val):
        return next((x for x in self.options if x.match(val)), None)

    def __call__(self, val):
        opt = self.match(val)
        if not opt:
            raise ValueError(
                "Value '%s' doesn't match any of the constructors"
                % str(val)
            )
        return opt(val)


class IO(object):

    def __init__(self, port_id, validator=None, constructor=None,
                 required=False, annotations=None, depth=0):
        self.id = port_id
        self.validator = Draft4Validator(validator)
        self.required = required
        self.annotations = annotations
        self.constructor = constructor or str
        self.depth = depth

    def validate(self, value):
        return self.validator.validate(value)

    def to_dict(self, ctx=None):
        return {
            '@id': self.id,
            '@type': 'IO',
            'depth': self.depth,
            'schema': self.validator.schema,
            'required': self.required,
            'annotations': self.annotations
        }

    @classmethod
    def from_dict(cls, context, d):

        item_schema = d.get('schema', {})
        type_name = item_schema.get('type')
        depth = 0
        while type_name == 'array':
            depth += 1
            item_schema = item_schema.get('items', {})
            type_name = item_schema.get('type')

        constructor = make_constructor(item_schema)

        return cls(d.get('@id', six.text_type(uuid4())),
                   validator=context.from_dict(d.get('schema')),
                   constructor=constructor,
                   required=d['required'],
                   annotations=d['annotations'],
                   depth=depth)


class Job(object):

    def __init__(self, job_id, app, inputs, allocated_resources, context):
        self.id = job_id or self.mk_work_dir(app.id)
        self.app = app
        self.inputs = inputs
        self.allocated_resources = allocated_resources
        self.context = context
        pass

    def run(self):
        return self.app.run(self)

    def to_dict(self, context=None):
        ctx = context or self.context
        return {
            '@id': self.id,
            '@type': 'Job',
            'app': self.app.to_dict(ctx),
            'inputs': ctx.to_dict(self.inputs),
            'allocatedResources': ctx.to_dict(self.allocated_resources)
        }

    @staticmethod
    def mk_work_dir(name):
        ts = time.time()
        path = '_'.join([name, datetime.datetime.fromtimestamp(ts).strftime('%H%M%S')])
        try_path = path
        if os.path.exists(path):
            num = 0
            try_path = path
            while os.path.exists(try_path):
                try_path = '_'.join([path, six.text_type(num)])
                num += 1
        return try_path

    def __repr__(self):
        return "Job(%s)" % self.to_dict()

    @classmethod
    def from_dict(cls, context, d):
        app = context.from_dict(d['app'])
        return cls(
            d.get('@id') if d.get('@id') else cls.mk_work_dir(app.id),
            app,
            context.from_dict(d['inputs']),
            d.get('allocatedResources'),
            context
        )


class Resource(object):
    pass
