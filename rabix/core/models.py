import os
import json
import logging

from rabix.common.util import wrap_in_list, map_rec_list

__author__ = 'luka'


log = logging.getLogger(__name__)


class Process(object):

    def __init__(self, id, inputs, outputs, requirements, hints,
                 label, description, scatter, scatter_method):
        self.id = id
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

    def install(self, *args, **kwargs):
        self.impl.install(*args, **kwargs)

    def to_dict(self, context):
        proc = super(ExternalProcess, self).to_dict(context)
        proc['class'] = 'External'
        proc['impl'] = self.impl.to_dict(context)
        return proc

    @classmethod
    def from_dict(cls, context, d):
        impl_ref = d['impl']
        kwargs = {
            'id': d['id'],
            'inputs': context.from_dict(d.get('inputs', [])),
            'outputs': context.from_dict(d.get('outputs', [])),
            'requirements': context.from_dict(d.get('requirements', [])),
            'hints': context.from_dict(d.get('hints', [])),
            'label': (d.get('label', id)),
            'description': context.from_dict(d.get('description', '')),
            'scatter': wrap_in_list(d.get('scatter', [])),
            'scatter_method': context.from_dict(d.get('scatter_method')),
            'impl': None
        }
        return cls(**kwargs)


class Parameter(object):

    def __init__(self, id, type, streamable, description):
        self.id = id
        self.type = type
        self.streamable = streamable
        self.description = description

    def to_dict(self, context):
        return {

        }

    @classmethod
    def from_dict(cls):
        pass

InputParameter = Parameter
OutputParameter = Parameter
