import os
import subprocess
import six
import json
import stat
import copy
import logging
import shutil

from avro.schema import NamedSchema

from rabix.cli.adapter import CLIJob
from rabix.common.errors import RabixError, ValidationError
from rabix.common.models import (
    Process, File, InputParameter, OutputParameter, construct_files,
    Job)
from rabix.common.io import InputCollector
from rabix.common.util import map_or_apply, map_rec_collection
from rabix.expressions import ValueResolver


log = logging.getLogger(__name__)


def flatten_files(files):
    flattened = []
    for file in files:
        flattened.append(file)
        flattened.extend(file.secondary_files)
    return flattened


def collect_prefixes(paths):
    """
    Determine minimal set of paths that needs to be bound to docker
    container, such that only directories actually containing files are
    bound (otherwise trivial minimal set would be '/').

    >>> list(collect_prefixes(['/a/b/c', '/a/b']))
    ['/a/b/']
    >>> sorted(list(collect_prefixes(['/a/b/c', '/a/b/d'])))
    ['/a/b/c/', '/a/b/d/']
    >>> sorted(list(collect_prefixes(['/a/b', '/c/d'])))
    ['/a/b/', '/c/d/']
    >>> sorted(list(collect_prefixes(['/a/b/', '/a/b/c', '/c/d'])))
    ['/a/b/', '/c/d/']

    :param paths: list of directory names containing files
    :return: set of paths guaranteed to end with forward slash
    """
    prefix_tree = {}
    paths_parts = [path.split('/') for path in paths]
    for path in paths_parts:
        path = [part for part in path if part]
        cur = prefix_tree
        last = len(path) - 1
        for idx, part in enumerate(path):
            if part not in cur:
                cur[part] = (idx == last, {})
            elif idx == last:
                cur[part] = (True, cur[part][1])
            term, cur = cur[part]
            if term:
                break

    prefixes = set()

    def collapse(prefix, tree):
        for k, (term, v) in six.iteritems(tree):
            p = prefix + k + '/'
            if term:
                prefixes.add(p)
            else:
                collapse(p, v)

    collapse('/', prefix_tree)

    return prefixes


def collect_files(inputs):
    files = []

    def append_file(v):
        if isinstance(v, File):
            files.append(v)

    map_rec_collection(append_file, inputs)
    return files


def file_or_val(val):
    try:
        return File(val)
    except ValidationError:
        return val


def convert_to_files(outputs):

    for k, v in outputs.iteritems():
        if isinstance(v, dict):
            try:
                outputs[k] = File(v)
            except ValidationError:
                convert_to_files(v)
        else:
            outputs[k] = map_or_apply(file_or_val, v)


class Container(object):

    def __init__(self):
        self.input_collector = None

    def install(self, job):
        pass

    def get_mapping(self, paths):
        return {}

    def ensure_files(self, job, job_dir):
        """
        Download remote files and find secondary files according to annotations
        """
        inputs = job.app.inputs
        self.input_collector = InputCollector(job_dir)
        input_values = job.inputs
        self._resolve(inputs, input_values, job.inputs)

    def _resolve(self, inputs, input_values, job):

        if inputs:
            file_ins = [i for i in inputs
                        if isinstance(i.validator, NamedSchema) and
                        i.validator.name == 'File']
            for f in file_ins:
                val = input_values.get(f.id)
                if val:
                    job[f.id] = map_or_apply(
                        lambda e: self.input_collector.download(
                            e.url, f.input_binding.get('secondaryFiles')
                            if f.input_binding is not None else None
                        ),
                        val)


class CommandLineTool(Process):
    WORKING_DIR = '/work'

    def __init__(
            self, process_id, inputs, outputs, requirements, hints,
            label, description, base_command, arguments=None,
            stdin="", stdout=""):
        super(CommandLineTool, self).__init__(
            process_id, inputs, outputs,
            requirements=requirements,
            hints=hints,
            label=label,
            description=description
        )
        self.base_command = base_command
        self.arguments = [a if isinstance(a, dict) else {'valueFrom': a}
                          for a in (arguments or [])]
        self.stdin = stdin
        self.stdout = stdout
        self.mappings = {}
        self.cli_job = None
        self._command_line = None
        self.container = next(
            (r for r in self.requirements if hasattr(r, 'run')),
            next((r for r in self.hints if hasattr(r, 'run')), None)
        )

    def run(self, job, job_dir=None):
        self.load_input_content(job)
        job_dir = os.path.abspath(job_dir or job.id)
        if not job_dir.endswith('/'):
            job_dir += '/'

        if not os.path.exists(job_dir):
            os.mkdir(job_dir)

        os.chmod(job_dir, os.stat(job_dir).st_mode | stat.S_IROTH |
                 stat.S_IWOTH)
        self.cli_job = CLIJob(job)

        eval = ValueResolver(job)

        cfr = self.get_requirement_or_hint(CreateFileRequirement)
        if cfr:
            cfr.create_files(job_dir, eval)

        env = None
        evr = self.get_requirement_or_hint(EnvVarRequirement)
        if evr:
            env = evr.var_map(eval)

        self.ensure_files(job, job_dir)
        self.install(job=job)

        abspath_job = Job(
            job.id, job.app, copy.deepcopy(job.inputs),
            job.allocated_resources, job.context
        )

        cmd_line = self.command_line(job, job_dir)
        log.info("Running: %s" % cmd_line)
        self.job_dump(job, job_dir)

        if self.container:
            self.container.run(cmd_line, job_dir, env)
        else:
            ret = subprocess.call(['bash', '-c', cmd_line], cwd=job_dir)
            if ret != 0:
                raise RabixError("Command failed with exit status %s" % ret)

        result_path = os.path.abspath(job_dir) + '/cwl.output.json'
        if os.path.exists(result_path):
            with open(result_path, 'r') as res:
                outputs = json.load(res)
                convert_to_files(outputs)
        else:
            with open(result_path, 'w') as res:
                outputs = self.cli_job.get_outputs(
                    os.path.abspath(job_dir), abspath_job)
                json.dump(job.context.to_primitive(outputs), res)

        self.unmap_paths(outputs)

        def write_rbx(f):
            if isinstance(f, File):
                with open(f.path + '.rbx.json', 'w') as rbx:
                    json.dump(f.to_dict(), rbx)

        map_rec_collection(write_rbx, outputs)

        return outputs

    def command_line(self, job, job_dir=None):
        self.remap_paths(job.inputs, job_dir)
        self._command_line = self.cli_job.cmd_line()
        return self._command_line

    def install(self, *args, **kwargs):
        if self.container:
            self.container.install(*args, **kwargs)

    def ensure_files(self, job, job_dir):
        if self.container:
            self.container.ensure_files(job, job_dir)

    def remap_paths(self, inputs, job_dir):
        if self.container:
            files = collect_files(inputs)
            flatened = flatten_files(files)
            paths = [os.path.dirname(f.path) for f in flatened] + [job_dir]
            prefixes = collect_prefixes(paths)
            self.mappings = self.container.get_mapping(prefixes)
            for file in files:
                file.remap(self.mappings)

    def unmap_paths(self, outputs):
        files = collect_files(outputs)
        for file in files:
            file.remap({v: k for k, v in six.iteritems(self.mappings)})

    def to_dict(self, context=None):
        d = super(CommandLineTool, self).to_dict(context)
        d.update({
            'class': 'CommandLineTool',
            'baseCommand': self.base_command if isinstance(
                self.base_command, list) else [self.base_command],
            'arguments': self.arguments,
            'stdin': self.stdin,
            'stdout': self.stdout
        })
        return d

    @classmethod
    def from_dict(cls, context, d):
        converted = {k: context.from_dict(v) for k, v in six.iteritems(d)}
        kwargs = Process.kwarg_dict(converted)
        kwargs.update({
            'base_command': converted['baseCommand'],
            'arguments': converted.get('arguments'),
            'stdin': converted.get('stdin', ""),
            'stdout': converted.get('stdout', ""),
            'inputs': [InputParameter.from_dict(context, inp)
                       for inp in converted.get('inputs', [])],
            'outputs': [OutputParameter.from_dict(context, inp)
                        for inp in converted.get('outputs', [])]
        })
        return cls(**kwargs)


class CreateFileRequirement(object):

    def __init__(self, file_defs):
        self.file_defs = file_defs

    def to_dict(self, context=None):
        return {
            "class": "CreateFileRequirement",
            "fileDef": self.file_defs
        }

    def resolve_file_defs(self, eval):
        return [(eval.resolve(f['filename']), eval.resolve(f['fileContent']))
                for f in self.file_defs]

    def create_files(self, dir, eval):
        for name, content in self.resolve_file_defs(eval):
            dst = os.path.join(dir, name)
            if isinstance(content, File):
                shutil.copyfile(content.path, dst)
            else:
                with open(dst, 'w') as out:
                    out.write(content)

    @classmethod
    def from_dict(cls, context, d):
        return cls(d['fileDef'])


class EnvVarRequirement(object):
    def __init__(self, env_defs):
        self.env_defs = env_defs

    def to_dict(self, context=None):
        return {
            "class": "EnvVarRequirement",
            "envDef": self.env_defs
        }

    def var_map(self, eval):
        return ["{}={}".format(e['envName'], eval.resolve(e['envValue']))
                for e in self.env_defs]

    @classmethod
    def from_dict(cls, context, d):
        return cls(d['envDef'])


class MemRequirement(object):

    def __init__(self, val):
        self.value = val

    @classmethod
    def from_dict(cls, context, d):
        return cls(d.get('value'))

    def to_dict(self, context):
        return {'class': 'MemRequirement', 'value': self.value}


class CpuRequirement(object):

    def __init__(self, val):
        self.value = val

    @classmethod
    def from_dict(cls, context, d):
        return cls(d.get('value'))

    def to_dict(self, context):
        return {'class': 'CPURequirement', 'value': self.value}


if __name__ == '__main__':
    import doctest
    doctest.testmod()
