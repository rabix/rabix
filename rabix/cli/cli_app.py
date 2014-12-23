import os
import six
import json
import stat
import time
import datetime
from uuid import uuid4
from rabix.cli.adapter import CLIJob
from rabix.common.models import App
from rabix.common.ref_resolver import from_url
from rabix.common.io import InputCollector


def job_id(name):
    ts = time.time()
    return '_'.join([name, datetime.datetime.fromtimestamp(ts).strftime('%H%M%S')])


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
        is_single = lambda i: i.constructor in ['directory', 'file']
        is_array = lambda i: i.constructor == 'array' and any([
            i.itemType == 'directory', i.itemType == 'file'])
        is_object = lambda i: i.constructor == 'array' and i.itemType == 'object'

        if inputs:
            single = filter(is_single, [i for i in inputs])
            lists = filter(is_array, [i for i in inputs])
            objects = filter(is_object, [i for i in inputs])
            for inp in single:
                self._resolve_single(inp, input_values.get(inp.id), job)
            for inp in lists:
                self._resolve_list(inp, input_values.get(inp.id), job)
            for obj in objects:
                if input_values.get(obj.id):
                    for num, o in enumerate(input_values[obj.id]):
                        self._resolve(obj.objects, o, job[obj.id][num])

    def _resolve_single(self, inp, input_value, job):

        if input_value:
            # if input_value['path'].endswith('.rbx.json'):
            #     job[inp.id] = from_url(input_value['path'])
            # else:
            secondaryFiles = inp.annotations.get('secondaryFiles')
            job[inp.id] = self.inputCollector.download(
                input_value['path'], secondaryFiles)

    def _resolve_list(self, inp, input_value, job):
        if input_value:
            secondaryFiles = inp.annotations.get(
                'secondaryFiles')
            for num, inv in enumerate(input_value):
                if input_value[num]['path'].endswith('.rbx.json'):
                    job[inp.id][num] = from_url(input_value[num][
                        'path'])
                else:
                    job[inp.id][num] = self.inputCollector.download(
                        input_value[num]['path'], secondaryFiles)


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
    WORKING_DIR = '/work'

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

    def run(self, job, job_dir=None):
        job_dir = job_dir or job.app.id
        job_dir = self.mk_work_dir(job_dir)
        os.chmod(job_dir, os.stat(job_dir).st_mode | stat.S_IROTH |
                 stat.S_IWOTH)
        if self.requirements.container:
            self.ensure_files(job, job_dir)
            self.install(job=job)
            self.job_dump(job, job_dir)
            self.set_config(job=job, job_dir=job_dir)
            adapter = CLIJob(job.to_dict(), job.app)
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

    def to_dict(self):
        d = super(CliApp, self).to_dict()
        d.update({
            "@type": "CommandLine",
            'adapter': self.adapter,
            'annotations': self.annotations,
            'platform_features': self.platform_features,
            'inputs': self.inputs.schema,
            'outputs': self.outputs.schema
        })
        return d

    @classmethod
    def from_dict(cls, context, d):
        return cls(d.get('@id', d.get('@id') if d.get('@id') else job_id(d.get('name'))),
                   context.from_dict(d['inputs']),
                   context.from_dict(d['outputs']),
                   app_description=d.get('appDescription'),
                   annotations=d.get('annotations'),
                   platform_features=d.get('platform_features'),
                   adapter=context.from_dict(d.get('adapter')),
                   software_description=d.get('softwareDescription'),
                   requirements=Requirements.from_dict(
                       context, d.get('requirements', {})))
