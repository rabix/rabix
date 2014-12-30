import os
import six
import json
import logging
import time
import datetime

from uuid import uuid4
from jsonschema.validators import Draft4Validator
from six.moves.urllib.parse import urlparse


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

    def job_dump(self, job, dirname, context):
        with open(os.path.join(dirname, 'job.cwl.json'), 'w') as f:
            json.dump(job.to_dict(context), f)
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


class File(object):

    def __init__(self, path, size=None, meta=None, secondary_files=None):
        self.url = path
        self.size = size
        self.meta = meta or {}
        self.secondary_files = secondary_files or []

    def to_dict(self, context):
        if isinstance(self.url, str):
            path = self.url
        else:
            path = self.url.geturl()
        return {
            "@type": "File",
            "path": path,
            "size": self.size,
            "metadata": self.meta,
            "secondaryFiles": [sf.to_dict(context)
                               for sf in self.secondary_files]
        }

    @classmethod
    def from_dict(cls, d):

        if isinstance(d, six.string_types):
            d = {'path': d}

        size = d.get('size')
        if size is not None:
            size = int(size)

        return cls(path=d.get('path'),
                   size=size,
                   meta=d.get('meta'),
                   secondary_files=[File.from_dict(sf)
                                    for sf in d.get('secondaryFiles', [])])


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
        constructor_map = {
            'integer': int,
            'number': float,
            'boolean': bool,
            'object': dict,
            'string': str,
            'file': File.from_dict,
            'directory': File.from_dict
        }
        item_schema = d.get('schema', {})
        type_name = item_schema.get('type')
        depth = 0
        while type_name == 'array':
            depth += 1
            item_schema = item_schema.get('items', {})
            type_name = item_schema.get('type')

        return cls(d.get('@id', str(uuid4())),
                   validator=context.from_dict(d.get('schema')),
                   constructor=constructor_map[type_name],
                   required=d['required'],
                   annotations=d['annotations'],
                   depth=depth)


class Job(object):

    def __init__(self, job_id, app, inputs, allocated_resources):
        # if not app.validate_inputs(inputs):
        #     raise ValidationError("Invalid inputs for application %s" % app.id)
        self.id = job_id
        self.app = app
        self.inputs = inputs
        self.allocated_resources = allocated_resources

    def run(self):
        return self.app.run(self)

    def to_dict(self, context):
        return {
            '@id': self.id,
            '@type': 'Job',
            'app': self.app.to_dict(context),
            'inputs': context.to_dict(self.inputs),
            'allocatedResources': self.allocated_resources
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
            d.get('@id') if d.get('@id') else cls.mk_work_dir(app.id), app,
            context.from_dict(d['inputs']), d.get('allocatedResources')
        )


class Resource(object):
    pass
