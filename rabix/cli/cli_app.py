import os
import six
import json
import stat
import copy

from rabix.cli.adapter import CLIJob
from rabix.common.models import Process, File
from rabix.common.io import InputCollector
from rabix.common.util import map_or_apply, map_rec_collection


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

    >>> collect_prefixes(['/a/b', '/a/b/c'])
    set(['/a/b/'])
    >>> collect_prefixes(['/a/b/c', '/a/b/d'])
    set(['/a/b/d/', '/a/b/c/'])
    >>> collect_prefixes(['/a/b', '/c/d'])
    set(['/a/b/', '/c/d/'])
    >>> collect_prefixes(['/a/b/', '/a/b/c', '/c/d'])
    set(['/a/b/', '/c/d/'])

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
        inputs = job.app.inputs.io
        self.input_collector = InputCollector(job_dir)
        input_values = job.inputs
        self._resolve(inputs, input_values, job.inputs)

    def _resolve(self, inputs, input_values, job):

        if inputs:
            file_ins = [i for i in inputs if i.constructor.name == 'File']
            for f in file_ins:
                val = input_values.get(f.id)
                if val:
                    job[f.id] = map_or_apply(
                        lambda e: self.input_collector.download(
                            e.url, f.annotations.get('secondaryFiles')
                            if f.annotations is not None else None
                        ),
                        val)


class CommandLineTool(Process):
    WORKING_DIR = '/work'

    def __init__(
            self, process_id, inputs, outputs, requirements, hints,
            label, description, base_command, arguments=None,
            stdin=None, stdout=None):
        super(CommandLineTool, self).__init__(
            process_id, inputs, outputs,
            requirements=requirements,
            hints=hints,
            label=label,
            description=description
        )
        self.base_command = base_command
        self.arguments = arguments
        self.stdin = stdin
        self.stdout = stdout
        self.mappings = {}
        self.cli_job = None
        self._command_line = None

    def run(self, job, job_dir=None):
        job_dir = os.path.abspath(job_dir or job.id)
        if not job_dir.endswith('/'):
            job_dir += '/'

        os.mkdir(job_dir)
        os.chmod(job_dir, os.stat(job_dir).st_mode | stat.S_IROTH |
                 stat.S_IWOTH)
        self.cli_job = CLIJob(job)

        if self.requirements.container:
            self.ensure_files(job, job_dir)
            abspath_job = copy.deepcopy(job)
            self.install(job=job)

            cmd_line = self.command_line(job, job_dir)
            self.job_dump(job, job_dir)
            self.requirements.container.run(cmd_line, job_dir)
            result_path = os.path.abspath(job_dir) + '/result.cwl.json'
            if os.path.exists(result_path):
                with open(result_path, 'r') as res:
                    outputs = json.load(res)
            else:
                with open(result_path, 'w') as res:
                    outputs = self.cli_job.get_outputs(
                        os.path.abspath(job_dir), abspath_job)
                    json.dump(outputs, res)

            outputs = job.app.construct_outputs(outputs)
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
        if self.requirements and self.requirements.container:
            self.requirements.container.install(*args, **kwargs)

    def ensure_files(self, job, job_dir):
        if self.requirements and self.requirements.container:
            self.requirements.container.ensure_files(job, job_dir)

    def remap_paths(self, inputs, job_dir):
        if self.requirements and self.requirements.container:
            files = collect_files(inputs)
            flatened = flatten_files(files)
            paths = [os.path.dirname(f.path) for f in flatened] + [job_dir]
            prefixes = collect_prefixes(paths)
            self.mappings = self.requirements.container.get_mapping(prefixes)
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
            'baseCommand': self.base_command,
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
            'stdin': converted.get('stdin'),
            'stdout': converted.get('stdout')
        })
        return cls(**kwargs)

if __name__ == '__main__':
    import doctest
    doctest.testmod()
