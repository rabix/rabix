import os
import six
import json
import logging
import time
import datetime

from uuid import uuid4
from jsonschema.validators import Draft4Validator
from six.moves.urllib.parse import urlparse, urlunparse, unquote, urljoin
from base64 import b64decode
from os.path import isabs

from rabix.common.errors import ValidationError, RabixError
from rabix.common.util import map_rec_list, wrap_in_list


log = logging.getLogger(__name__)


class Process(object):

    def __init__(self, process_id, inputs, outputs, requirements, hints,
                 label, description, scatter, scatter_method):
        self.id = process_id
        self.inputs = inputs
        self.outputs = outputs
        self.requirements = requirements
        self.hints = hints
        self.label = label
        self.description = description
        self.scatter = wrap_in_list(scatter)
        self.scatter_method = scatter_method
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

    def construct_inputs(self, inputs):
        return self.construct(self.inputs, inputs)

    def construct_outputs(self, outputs):
        return self.construct(self.outputs, outputs)

    @staticmethod
    def construct(defs, vals):
        return {
            input.id: map_rec_list(input.constructor, vals.get(input.id))
            for input in defs
            if vals.get(input.id) is not None
        }

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
            'id': self.id,
            'class': 'Process',
            'inputs': context.to_dict(self.inputs),
            'outputs': context.to_dict(self.outputs),
            'requirements': self.requirements,
            'hints': self.hints
        }


class ExternalProcess(Process):

    def __init__(self, process_id, inputs, outputs, requirements, hints, label,
                 description, scatter, scatter_method, impl):
        super(ExternalProcess, self).__init__(
            process_id, inputs, outputs, requirements, hints, label,
            description, scatter, scatter_method)
        self.impl = impl

    def run(self, job):
        return self.impl.run(job)

    def to_dict(self, context):
        proc = super(ExternalProcess, self).to_dict(context)
        proc['class'] = 'External'
        proc['impl'] = self.impl
        return proc


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

    def join(self, base):
        base += '' if base.endswith('/') else '/'
        return URL(urljoin(base, str(self)))

    def remap(self, mappings):
        if not self.islocal():
            raise RabixError("Can't remap non-local paths")
        if not isabs(self.path):
            raise RabixError("Can't remap non-absolute paths")
        for k, v in six.iteritems(mappings):
            if self.path.startswith(k):
                ls = self.path[len(k):]
                return URL(urljoin(v, ls))

        return self

    def __str__(self):
        if self.islocal():
            return self.path
        else:
            return self.geturl()

    def __repr__(self):
        return "URL(%s)" % self.geturl()


class File(object):

    name = 'File'

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
        elif isinstance(path, File):
            self.size, self.meta, self.secondary_files, self.url = \
                path.size, path.meta, path.secondary_files, path.url
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
            "class": "File",
            "path": self.path,
            "size": self.size,
            "metadata": self.meta,
            "secondaryFiles": [sf.to_dict(context)
                               for sf in self.secondary_files]
        }

    @property
    def path(self):
        return six.text_type(self.url)

    def rebase(self, base):
        self.url = self.url.join(base)
        for sf in self.secondary_files:
            sf.rebase(base)
        return self

    def remap(self, mappings):
        self.url = self.url.remap(mappings)
        for sf in self.secondary_files:
            sf.remap(mappings)
        return self

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

    def __repr__(self):
        return "ArrayConstructor(%s)" % repr(self.item_constructor)


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

    def __repr__(self):
        return "ObjectConstructor(%s)" % repr(self.properties)


class PrimitiveConstructor(object):

    CONSTRUCTOR_MAP = {
        'integer': int,
        'number': float,
        'boolean': bool,
        'string': six.text_type
    }

    MATCH_MAP = dict(CONSTRUCTOR_MAP)
    MATCH_MAP.update({
        'string': six.string_types,
        'number': (int, float)
    })

    def __init__(self, type_name):
        self.name = type_name
        self._match = PrimitiveConstructor.MATCH_MAP.get(type_name)
        self.type = PrimitiveConstructor.CONSTRUCTOR_MAP.get(type_name)

    def __call__(self, val):
        return self.type(val)

    def match(self, val):
        matches = isinstance(val, self._match)
        return matches

    def __repr__(self):
        return self.name


class OneOfConstructor(object):

    def __init__(self, options):
        self.name = 'oneOf'
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

    def __repr__(self):
        return "OneOf(%s)" % repr(self.options)


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
            'id': self.id,
            'type': self.validator.schema,
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

        return cls(d.get('id', six.text_type(uuid4())),
                   validator=context.from_dict(d.get('schema')),
                   constructor=constructor,
                   required=d['required'],
                   annotations=d['annotations'],
                   depth=depth)

    def __repr__(self):
        return "IO(%s)" % vars(self)


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
            'id': self.id,
            'class': 'Job',
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
            d.get('id') if d.get('id') else cls.mk_work_dir(app.id),
            app,
            context.from_dict(d['inputs']),
            d.get('allocatedResources'),
            context
        )
