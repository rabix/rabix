import os
import six
import json
import logging
from copy import copy
from uuid import uuid4
from jsonschema.validators import Draft4Validator


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

    def mk_work_dir(self, path):
        if os.path.exists(path):
            num = 0
            while os.path.exists(path):
                path = '_'.join([path, str(num)])
                num = num + 1
        os.mkdir(path)
        return path

    def job_dump(self, job, dirname):
        with open(os.path.join(dirname, 'job.cwl.json'), 'w') as f:
            json.dump(job.to_dict(), f)
            log.info('File %s created.', job.id)

    def to_dict(self):
        return {
            '@id': self.id,
            '@type': 'App',
            'inputs': [inp.to_dict() for inp in self.inputs],
            'outputs': [outp.to_dict() for outp in self.outputs],
            'appDescription': self.app_description,
            'annotations': self.annotations,
            'platformFeatures': self.platform_features
        }


class File(object):

    def __init__(self, path, size=None, meta=None, secondaryFiles=None):
        self.path = path
        self.size = size
        self.meta = meta
        self.secondaryFiles = secondaryFiles


class IO(object):

    def __init__(self, context, port_id, validator=None, constructor=None,
                 required=False, annotations=None, items=None):
        self.id = port_id
        self.validator = Draft4Validator(validator)
        self.required = required
        self.annotations = annotations
        self.constructor = constructor or str
        if self.constructor == 'array':
            self.itemType = items['type']
            if self.itemType == 'object':
                required = items.get('required', [])
                self.objects = [IO(context, k, v,
                                   constructor=v['type'],
                                   required=k in required,
                                   annotations=v['adapter'],
                                   items=v.get('items'))
                                for k, v in
                                six.iteritems(items['properties'])]

    @property
    def depth(self):
        if self.constructor != 'array':
            return 0
        elif self.itemType != 'object':
            return 1
        else:
            return 1 + max([k.depth for k in self.objects])

    def validate(self, value):
        return self.validator.validate(value)

    def to_dict(self):
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
            'array': list,
            'object': dict,
            'string': str,
            'file': File
        }
        return cls(d.get('@id', str(uuid4())),
                   validator=context.from_dict(d.get('schema')),
                   constructor=constructor_map[
                       d.get('schema', {}).get('type')],
                   required=d['required'],
                   annotations=d['annotations'])


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

    def to_dict(self):
        return {
            '@id': self.id,
            '@type': 'Job',
            'app': self.app.to_dict(),
            'inputs': self.inputs,
            'allocatedResources': self.allocated_resources
        }

    def __str__(self):
        return str(self.to_dict())

    __repr__ = __str__

    @classmethod
    def from_dict(cls, context, d):
        return cls(
            d.get('@id', str(uuid4())), context.from_dict(d['app']),
            context.from_dict(d['inputs']), d.get('allocatedResources')
        )


class Resource(object):
    pass
