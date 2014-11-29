import os
import json
import logging
import uuid
import stat
import copy

import docker
import six

from rabix.common.io import InputRunner
from rabix.docker.container import Container, ensure_image
from rabix.cli.adapter import CLIJob
from rabix.tests import infinite_loop, infinite_read


log = logging.getLogger(__name__)


def match_shape(schema, job):
    jobs = [job]
    for input, inp_scm in six.iteritems(schema['inputs']['properties']):
        if inp_scm['type'] != 'array' and isinstance(job[input], list):
            jobs_tmp = []
            for job in jobs:
                for val in job[input]:
                    new_job = job.copy()
                    new_job[input] = val
                    jobs_tmp.append(new_job)
            jobs = jobs_tmp
    return jobs


class BindDict(dict):
    def items(self):
        ret = []
        for k, v in six.iteritems(super(BindDict, self)):
            ret.append((v, k))
        return ret


class Runner(object):
    WORKING_DIR = '/work'

    def __init__(self, app, working_dir='./', stdout=None, stderr='out.err'):
        if not os.path.isabs(working_dir):
            working_dir = os.path.abspath(working_dir)
        self.app = app
        self.enviroment = (app.requirements or {}).get('environment')
        self.working_dir = working_dir
        self.stdout = stdout
        self.stderr = stderr

    def run_job(self, job):
        pass

    def rnd_name(self):
        return str(uuid.uuid4())

    def install(self):
        pass

    def write_result(self, result):
        pass

    def provide_files(self, job, dir=None):
        return InputRunner(job, self.app.inputs.schema['properties'], dir)()


class DockerRunner(Runner):
    def __init__(self, app, working_dir='./', dockr=None, stderr=None):
        stdout = (app.adapter or {}).get('stdout', None)
        super(DockerRunner, self).__init__(app, working_dir, stdout)
        self.docker_client = dockr or docker.Client(os.getenv(
            "DOCKER_HOST", None), version='1.12')

    def _volumes(self, job):
        remaped_job = copy.deepcopy(job)
        volumes = {}
        binds = {}

        inputs = self.app.inputs.schema['properties']
        input_values = remaped_job.get('inputs')
        self._remap(inputs, input_values, volumes, binds, remaped_job[
            'inputs'])

        return volumes, BindDict(binds), remaped_job

    def _remap(self, inputs, input_values, volumes, binds, remaped_job,
               parent=None):
        is_single = lambda i: any([inputs[i]['type'] == 'directory',
                                   inputs[i]['type'] == 'file'])
        is_array = lambda i: inputs[i]['type'] == 'array' and any([
            inputs[i]['items']['type'] == 'directory',
            inputs[i]['items']['type'] == 'file'])
        is_object = lambda i: (inputs[i]['type'] == 'array' and
                               inputs[i]['items']['type'] == 'object')

        if inputs:
            single = filter(is_single, [i for i in inputs])
            lists = filter(is_array, [i for i in inputs])
            objects = filter(is_object, [i for i in inputs])
            for inp in single:
                self._remap_single(inp, input_values, volumes, binds,
                                   remaped_job, parent)
            for inp in lists:
                self._remap_list(inp, input_values, volumes, binds,
                                 remaped_job, parent)
            for obj in objects:
                if input_values.get(obj):
                    for num, o in enumerate(input_values[obj]):
                        self._remap(inputs[obj]['items']['properties'],
                                    o, volumes, binds, remaped_job[obj][num],
                                    parent='/'.join([obj, str(num)]))

    def _remap_single(self, inp, input_values, volumes, binds, remaped_job,
                      parent):
        if input_values.get(inp):
            if parent:
                docker_dir = '/' + '/'.join([parent, inp])
            else:
                docker_dir = '/' + inp
            dir_name, file_name = os.path.split(
                os.path.abspath(input_values[inp]['path']))
            volumes[docker_dir] = {}
            binds[docker_dir] = dir_name
            remaped_job[inp]['path'] = '/'.join(
                [docker_dir, file_name])

    def _remap_list(self, inp, input_values, volumes, binds, remaped_job,
                    parent):
        if input_values[inp]:
            for num, inv in enumerate(input_values[inp]):
                if parent:
                    docker_dir = '/' + '/'.join([parent, inp, str(num)])
                else:
                    docker_dir = '/' + '/'.join([inp, str(num)])
                dir_name, file_name = os.path.split(
                    os.path.abspath(inv['path']))
                volumes[docker_dir] = {}
                binds[docker_dir] = dir_name
                remaped_job[inp][num]['path'] = '/'.join(
                    [docker_dir, file_name])

    @property
    def _envvars(self):
        envvars = (self.app.adapter or {}).get('environment', {})
        envlst = []
        for env, val in six.iteritems(envvars):
            envlst.append('='.join([env, val]))
        return envlst

    def _run(self, command, vol=None, bind=None,
             user=None, env=None, work_dir=None):
        volumes = vol or {self.WORKING_DIR: {}}
        working_dir = work_dir or self.WORKING_DIR
        user = user or ':'.join([str(os.getuid()), str(os.getgid())])
        container = Container(self.docker_client,
                              self.enviroment['container']['imageId'],
                              self.enviroment['container']['uri'],
                              command, user=user, volumes=volumes,
                              environment=env, working_dir=working_dir)
        binds = bind or {self.working_dir: self.WORKING_DIR}
        # TODO : Add mem_limit, ports, entrypoint, cpu_shares
        container.start(binds)
        return container

    def run_job(self, job, job_id=None):
        job_dir = job_id or self.rnd_name()
        if not os.path.exists(job_dir):
            os.mkdir(job_dir)
        os.chmod(job_dir, os.stat(job_dir).st_mode | stat.S_IROTH |
                 stat.S_IWOTH)
        job = self.provide_files(job, os.path.abspath(job_dir))

        volumes, binds, remaped_job = self._volumes(job)
        volumes['/' + job_dir] = {}
        binds['/' + job_dir] = os.path.abspath(job_dir)
        adapter = CLIJob(remaped_job, self.app)
        container = self._run(['bash', '-c', adapter.cmd_line()],
                              vol=volumes, bind=binds, env=self._envvars,
                              work_dir='/' + job_dir)
        container.get_stderr(file='/'.join([os.path.abspath(job_dir),
                                            self.stderr]))
        if not container.is_success():
            raise RuntimeError("err %s" % container.get_stderr())
        with open(os.path.abspath(job_dir) + '/result.json', 'w') as f:
            outputs = adapter.get_outputs(os.path.abspath(job_dir))
            for k, v in six.iteritems(outputs):
                if v:
                    meta = v.pop('meta', {})
                    with open(v['path'] + '.meta', 'w') as m:
                        json.dump(meta, m)
            json.dump(outputs, f)
            return outputs

    def install(self):
        ensure_image(self.docker_client,
                     self.enviroment['container']['imageId'],
                     self.enviroment['container']['uri'])


class NativeRunner(Runner):
    def __init__(self, app, working_dir='./', stdout=None, stderr=None):
        super(NativeRunner, self).__init__(app, working_dir, stdout, stderr)

    def run_job(self, job):
        pass


if __name__ == '__main__':
    from multiprocessing import Process

    '''
    Streaming between two containers test.
    '''
    working_dir = str(uuid.uuid4())
    os.mkdir(working_dir)
    os.chmod(working_dir, os.stat(working_dir).st_mode | stat.S_IROTH |
             stat.S_IWOTH)
    os.chdir(working_dir)

    runner_inf = DockerRunner(infinite_loop['tool'])
    runner_read = DockerRunner(infinite_read['tool'])

    command_inf = ['bash', '-c', '/home/infinite.sh > %s' %
                   ('/' + working_dir + '/pipe')]
    command_read = ['bash', '-c', '/home/infinite_read.sh < %s > %s' %
                    ('/' + working_dir + '/pipe',
                     '/' + working_dir + '/result')]
    volumes = {''.join(['/', working_dir]): {}}
    binds = {os.path.abspath('./'): ''.join(['/', working_dir])}
    container_inf = runner_inf._run(command_inf, vol=volumes, bind=binds,
                                    work_dir='/' + working_dir)
    container_rd = runner_read._run(command_read, vol=volumes, bind=binds,
                                    work_dir='/' + working_dir)
    p2 = Process(target=container_rd.get_stdout, kwargs={'file': 'result'})
    p2.start()
