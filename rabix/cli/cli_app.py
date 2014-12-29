import os
import six
import json
import stat

from rabix.cli.adapter import CLIJob
from rabix.common.models import App, File
from rabix.common.io import InputCollector


class Resources(object):

    def __init__(self, cpu, mem):
        self.cpu = cpu
        self.mem = mem

    def to_dict(self, serializer=None):
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
        self.inputCollector = InputCollector()

    def install(self, job):
        pass

    def set_config(self, *args, **kwargs):
        pass

    def ensure_files(self, job, job_dir):
        '''
        Resolve paths of all input files
        '''
        inputs = job.app.inputs.io
        self.inputCollector.set_dir(job_dir)
        input_values = job.inputs
        self._resolve(inputs, input_values, job.inputs)

    def _resolve(self, inputs, input_values, job):
        is_single = lambda i: i.constructor == File.from_dict and i.depth == 0
        is_array = lambda i: i.constructor == File.from_dict and i.depth > 0
        # is_object = lambda i: i.constructor == 'array' and i.itemType == 'object'

        if inputs:
            single = filter(is_single, [i for i in inputs])
            lists = filter(is_array, [i for i in inputs])
            # objects = filter(is_object, [i for i in inputs])
            for inp in single:
                ival = input_values.get(inp.id)
                if ival:
                    job[inp.id] = self._resolve_file(
                        inp.annotations.get('secondaryFiles'),
                        ival
                    )

            for inp in lists:
                ivals = input_values.get(inp.id)
                if ivals:
                    job[inp.id] = [self._resolve_file(
                        inp.annotations.get('secondaryFiles'),
                        ival
                    ) for ival in ivals]
            # for obj in objects:
            #     if input_values.get(obj.id):
            #         for num, o in enumerate(input_values[obj.id]):
            #             self._resolve(obj.objects, o, job[obj.id][num])

    def _resolve_file(self, secondary_files, input_value):

        # if input_value['path'].endswith('.rbx.json'):
        #     job[inp.id] = from_url(input_value['path'])
        # else:
        url = input_value.url
        if url.scheme == 'file':
            url = url.path
        else:
            url = url.geturl()
        return self.inputCollector.download(url, secondary_files)


class Requirements(object):

    def __init__(self, container=None, resources=None, platform_features=None):
        self.container = container
        self.resources = resources
        self.platform_features = platform_features

    def to_dict(self, context):
        return {
            "@type": "Requirements",
            "environment": {"container": self.container.to_dict(context)},
            "resources": context.to_dict(self.resources),
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
    WORKING_DIR = '/work'

    def __init__(self, app_id, inputs, outputs, context,
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
        self.context = context

    def run(self, job, job_dir=None):
        job_dir = job_dir or job.id
        os.mkdir(job_dir)
        os.chmod(job_dir, os.stat(job_dir).st_mode | stat.S_IROTH |
                 stat.S_IWOTH)
        if self.requirements.container:
            self.ensure_files(job, job_dir)
            self.install(job=job)
            self.job_dump(job, job_dir, self.context)
            self.set_config(job=job, job_dir=job_dir)
            adapter = CLIJob(job.to_dict(self.context), job.app)
            cmd_line = adapter.cmd_line()
            self.requirements.container.run(cmd_line)
            with open(os.path.abspath(job_dir) + '/result.cwl.json', 'w') as f:
                outputs = adapter.get_outputs(os.path.abspath(job_dir))
                for k, v in six.iteritems(outputs):
                    if v:
                        with open(v['path'] + '.rbx.json', 'w') as rx:
                            json.dump(v, rx)
                json.dump(outputs, f)
                return outputs

    def install(self, *args, **kwargs):
        if self.requirements and self.requirements.container:
            self.requirements.container.install(*args, **kwargs)

    def ensure_files(self, job, job_dir):
        if self.requirements and self.requirements.container:
            self.requirements.container.ensure_files(job, job_dir)

    def set_config(self, *args, **kwargs):
        if self.requirements and self.requirements.container:
            self.requirements.container.set_config(*args, **kwargs)

    def to_dict(self, context=None):
        d = super(CliApp, self).to_dict(context)
        d.update({
            "@type": "CommandLine",
            'adapter': self.adapter,
            'annotations': self.annotations,
            'platform_features': self.platform_features,
            'inputs': self.inputs.schema,
            'outputs': self.outputs.schema,
            'requirements': self.requirements.to_dict(context)
        })
        return d

    @classmethod
    def from_dict(cls, context, d):
        return cls(d.get('@id', d.get('name')),
                   context.from_dict(d['inputs']),
                   context.from_dict(d['outputs']),
                   context,
                   app_description=d.get('appDescription'),
                   annotations=d.get('annotations'),
                   platform_features=d.get('platform_features'),
                   adapter=context.from_dict(d.get('adapter')),
                   software_description=d.get('softwareDescription'),
                   requirements=Requirements.from_dict(
                       context, d.get('requirements', {})))
