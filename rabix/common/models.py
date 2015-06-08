import os
import json
import logging
import time
import datetime
from uuid import uuid4

import six

# noinspection PyUnresolvedReferences
from six.moves.urllib.parse import urlparse, urlunparse, unquote, urljoin
from base64 import b64decode
from os.path import isabs
from avro.schema import Names, make_avsc_object

from rabix.common.errors import ValidationError, RabixError
from rabix.common.util import map_rec_list


log = logging.getLogger(__name__)


def process_builder(context, d):
    inputs = d.get('inputs')
    outputs = d.get('outputs')

    schemas = []
    for s in context.get_requirement(SchemaDefRequirement):
        schemas.extend(s.types)

    for i in inputs:
        i['type'] = make_avro(i['type'], schemas)

    for o in outputs:
        o['type'] = make_avro(o['type'], schemas)

    return context.from_dict(d)


class Process(object):

    def __init__(
            self, process_id, inputs, outputs,
            requirements, hints, label, description
    ):
        self.id = process_id
        self.inputs = inputs
        self.outputs = outputs
        self.requirements = requirements
        self.hints = hints
        self.label = label
        self.description = description
        self._inputs = {io.id: io for io in inputs}
        self._outputs = {io.id: io for io in outputs}

    def install(self, *args, **kwargs):
        pass

    def run(self, job):
        raise NotImplementedError(
            "Method 'run' is not implemented in the App class"
        )

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
            job_dict = job.to_primitive()
            json.dump(job_dict, f)
            log.info('File %s created.', job.id)

    def to_dict(self, context):
        return {
            'id': self.id,
            'class': 'Process',
            'inputs': context.to_primitive(self.inputs),
            'outputs': context.to_primitive(self.outputs),
            'requirements': context.to_primitive(self.requirements),
            'hints': context.to_primitive(self.hints),
            'label': self.label,
            'description': self.description
        }

    @staticmethod
    def kwarg_dict(d):
        return {
            'process_id': d['id'],
            'inputs': d['inputs'],
            'outputs': d.get('outputs'),
            'requirements': d.get('requirements'),
            'hints': d.get('hints'),
            'label': d.get('label'),
            'description': d.get('description')
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
            "secondaryFiles": [sf.to_primitive(context)
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

FILE_SCHEMA = {
    'type': 'record',
    'name': 'File',
    'fields': [
        {
            'name': 'path',
            'type': 'string'
        },
        {
            'name': 'size',
            'type': ['long', 'null']
        },
        {
            'name': 'secondaryFiles',
            'type': ['null', {'type': 'array', 'items': 'File'}]
        },
        {
            'name': 'checksum',
            'type': ['null', 'string']
        }
    ]
}


def make_avro(schema, named_defs):
    names = Names()
    make_avsc_object(FILE_SCHEMA, names)
    for d in named_defs:
        make_avsc_object(d, names)

    return make_avsc_object(schema, names)


class Expression(object):
    pass


class Parameter(object):

    def __init__(
            self, id, validator=None, required=False, annotations=None, depth=0
    ):
        self.id = id
        self.validator = validator
        self.required = required
        self.annotations = annotations
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

        parameter_type = d.get('type', None)
        required = True
        if isinstance(parameter_type, list):
            non_null = []
            for t in parameter_type:
                if t == 'null':
                    required = False
                else:
                    non_null.append(t)

            if len(non_null) == 1:
                parameter_type = non_null[0]
            else:
                parameter_type = non_null

        # no do..while loops in python
        depth = -1
        type_name = 'array'
        while type_name == 'array':
            depth += 1
            parameter_type = parameter_type.get('items')
            if isinstance(parameter_type, dict):
                type_name = parameter_type.get('type')
            else:
                type_name = None

        return cls(d.get('id', six.text_type(uuid4())),
                   validator=d.get('type'),
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
            'app': self.app.to_primitive(ctx),
            'inputs': ctx.to_primitive(self.inputs),
            'allocatedResources': ctx.to_primitive(self.allocated_resources)
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
        app = process_builder(context, d['app'])
        return cls(
            d.get('id') if d.get('id') else cls.mk_work_dir(app.id),
            app,
            context.from_dict(d['inputs']),
            d.get('allocatedResources'),
            context
        )


class CreateFileRequirement(object):

    def __init__(self, file_defs):
        pass


class EnvVarRequirement(object):
    pass


class ExpressionEngineRequirement(object):
    pass


class SchemaDefRequirement(object):

    def __init__(self, types):
        self.types = types

    @classmethod
    def from_dict(cls, context, d):
        return cls(d.get('types', []))

    def to_dict(self, context):
        return {
            'type': 'SchemaDefRequirement',
            'types': [t.to_json() for t in self.types]
        }
