import copy

from uuid import uuid4

from rabix.common.models import App
from rabix.common.io import InputCollector


class Resources(object):

    def __init__(self, cpu, mem):
        self.cpu = cpu
        self.mem = mem

    def to_dict(self):
        return {
            "@type": "Resources",
            "cpu": self.cpu,
            "mem": self.mem
        }

    @classmethod
    def from_dict(cls, d):
        cls(d.get('cpu', 0), d.get('mem', 0))


class Container(object):

    def __init__(self):
        pass

    def install(self):
        pass

    def ensure_files(self, job):
        inputs = job.app.inputs.io
        input_values = job.inputs
        remaped_job = copy.deepcopy(job)
        self._resolve(inputs, input_values, remaped_job)
        pass

    def _resolve(self, inputs, input_values, remaped_job):
        is_single = lambda i: i.constructor in ['directory', 'file']
        is_array = lambda i: i.constructor == 'array' and any([i.itemType == 'directory', i.itemType == 'file'])
        is_object = lambda i: i.constructor == 'array' and i.itemType == 'object'

        if inputs:
            single = filter(is_single, [i for i in inputs])
            lists = filter(is_array, [i for i in inputs])
            objects = filter(is_object, [i for i in inputs])
            for inp in single:
                self._resolve_single(inp, inputs[inp], input_values.get(
                    inp), remaped_job)
            for inp in lists:
                self._resolve_list(inp, inputs[inp], input_values.get(
                    inp), remaped_job)
            for obj in objects:
                if input_values.get(obj):
                    for num, o in enumerate(input_values[obj]):
                        self._resolve(inputs[obj]['items']['properties'], o,
                                      remaped_job[obj][num])

    def _resolve_single(self, inp, input, input_value, remaped_job):
        pass

    def _resolve_list(self, inp, input, input_value, remaped_job):
        pass

    def _run(self):
        pass


class Requirements(object):

    def __init__(self, container=None, resources=None, platform_features=None):
        self.container = container
        self.resources = resources
        self.platform_features = platform_features

    def to_dict(self):
        return {
            "@type": "Requirements",
            "environment": {"container": self.container.to_dict()},
            "resources": self.resources.to_dict(),
            "platformFeatures": self.platform_features
        }

    @classmethod
    def from_dict(cls, context, d):
        return cls(
            context.from_dict(d.get('environment', {}).get('container')),
            context.from_dict(d.get('resources')),
            context.from_dict(d.get('platformFeatures'))
        )


class CliApp(App):

    def __init__(self, app_id, inputs, outputs,
                 app_description=None,
                 annotations=None,
                 platform_features=None,
                 adapter=None,
                 software_description=None,
                 requirements=None):
        super(CliApp, self).__init__(
            app_id, inputs, outputs,
            app_description=app_description,
            annotations=annotations,
            platform_features=platform_features
        )
        self.adapter = adapter
        self.software_description = software_description
        self.requirements = requirements

    def run(self, job):
        # self.get_inputs(job)
        if self.requirements.container:
            return self.requirements.container.run(self, job)

    def install(self):
        if self.requirements and self.requirements.container:
            self.requirements.container.install()

    def to_dict(self):
        d = super(CliApp, self).to_dict()
        d.update({
            "@type": "CommandLineTool",
            'adapter': self.adapter,
            'annotations': self.annotations,
            'platform_features': self.platform_features,
            'inputs': self.inputs.schema,
            'outputs': self.outputs.schema
        })
        return d

    @classmethod
    def from_dict(cls, context, d):
        return cls(d.get('@id', str(uuid4())),
                   context.from_dict(d['inputs']),
                   context.from_dict(d['outputs']),
                   app_description=d.get('appDescription'),
                   annotations=d.get('annotations'),
                   platform_features=d.get('platform_features'),
                   adapter=context.from_dict(d.get('adapter')),
                   software_description=d.get('softwareDescription'),
                   requirements=Requirements.from_dict(
                       context, d.get('requirements', {})))
