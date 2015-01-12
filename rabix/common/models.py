import os
import six
import json
import logging
import time
import datetime

from uuid import uuid4
from os.path import abspath, isabs, isfile, exists, join
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

    def __init__(self, path, size=None, meta=None, secondary_files=None):
        self.url = URL(path) if isinstance(path, six.string_types) else path
        self.size = size
        self.meta = meta or {}
        self.secondary_files = secondary_files or []

    def to_dict(self, context):
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
        self.url = val


def make_constructor(schema):
        constructor_map = {
            'integer': int,
            'number': float,
            'boolean': bool,
            'string': six.text_type,
            'file': FileConstructor,
            'directory': FileConstructor
        }

        type_name = schema.get('type')

        if not type_name:
            return lambda x: x

        if type_name == 'array':
            item_constructor = make_constructor(schema.get('items', {}))
            return ArrayConstructor(item_constructor)

        if type_name == 'object':
            return ObjectConstructor(schema.get('properties', {}))

        return constructor_map[type_name]


class ArrayConstructor(object):

    def __init__(self, item_constructor):
        self.item_constructor = item_constructor

    def __call__(self, val):
        return [self.item_constructor(v) for v in val]


class ObjectConstructor(object):

    def __init__(self, properties):
        self.properties = {
            k: make_constructor(v)
            for k, v in six.iteritems(properties)
        }

    def __call__(self, val):
        return {
            k: self.properties.get(k, lambda x: x)(v)
            for k, v in six.iteritems(val)
        }


def FileConstructor(val):
    if isinstance(val, six.string_types):
        val = {'path': val}

    size = val.get('size')
    if size is not None:
        size = int(size)

    path = val.get('path')
    if path is None:
        raise ValidationError(
            "Not a valid 'File' object: %s" % str(val)
        )

    return File(path=path,
               size=size,
               meta=val.get('meta'),
               secondary_files=[FileConstructor(sf)
                                for sf in val.get('secondaryFiles', [])])


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

        if not constructor:
            print(d)

        return cls(d.get('@id', str(uuid4())),
                   validator=context.from_dict(d.get('schema')),
                   constructor=constructor,
                   required=d['required'],
                   annotations=d['annotations'],
                   depth=depth)


class Job(object):

    def __init__(self, job_id, app, inputs, allocated_resources, context):
        # if not app.validate_inputs(inputs):
        #     raise ValidationError("Invalid inputs for application %s" % app.id)
        self.id = job_id
        self.app = app
        self.inputs = inputs
        self.allocated_resources = allocated_resources
        self.context = context

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
                try_path = '_'.join([path, str(num)])
                num += 1
        return try_path

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
