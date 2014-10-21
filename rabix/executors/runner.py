import os
import docker
import six
import json
import logging
import uuid
import stat
import copy

from multiprocessing import Process
from rabix.executors.io import InputRunner
from rabix.executors.container import Container
from rabix.cliche.adapter import Adapter
from rabix.tests import infinite_loop, infinite_read


log = logging.getLogger(__name__)


class BindDict(dict):
    def items(self):
        ret = []
        for k, v in six.iteritems(super(BindDict, self)):
            ret.append((v, k))
        return ret


class Runner(object):
    WORKING_DIR = '/work'

    def __init__(self, tool, working_dir='./', stdout=None, stderr='out.err'):
        if not os.path.isabs(working_dir):
            working_dir = os.path.abspath(working_dir)
        self.tool = tool
        self.enviroment = tool['requirements']['environment']
        self.working_dir = working_dir
        self.stdout = stdout
        self.stderr = stderr

    def run_job(self, job):
        pass

    def rnd_name(self):
        return str(uuid.uuid4())

    def provide_files(self, job, dir=None):
        return InputRunner(job, self.tool.get('inputs', {}).get('properties'), dir)()


class DockerRunner(Runner):
    def __init__(self, tool, working_dir='./', dockr=None, stderr=None):
        stdout = tool.get('adapter', {}).get('stdout', None)
        super(DockerRunner, self).__init__(tool, working_dir, stdout)
        self.docker_client = dockr or docker.Client(os.getenv("DOCKER_HOST", None), version='1.12')

    def _volumes(self, job):
        remaped_job = copy.deepcopy(job)
        volumes = {}
        binds = {}
        is_single = lambda i: any([inputs[i]['type'] == 'directory',
                                  inputs[i]['type'] == 'file'])
        is_array = lambda i: inputs[i]['type'] == 'array' and any([
            inputs[i]['items']['type'] == 'directory',
            inputs[i]['items']['type'] == 'file'])

        inputs = self.tool.get('inputs', {}).get('properties')
        input_values = remaped_job.get('inputs')
        if inputs:
            single = filter(is_single, [i for i in inputs])
            lists = filter(is_array, [i for i in inputs])
            for inp in single:
                docker_dir = '/' + inp
                dir_name, file_name = os.path.split(
                    os.path.abspath(input_values[inp]['path']))
                volumes[docker_dir] = {}
                binds[docker_dir] = dir_name
                remaped_job['inputs'][inp]['path'] = '/'.join(
                    [docker_dir, file_name])
            for inp in lists:
                for num, inv in enumerate(input_values[inp]):
                    docker_dir = '/' + '/'.join([inp, str(num)])
                    dir_name, file_name = os.path.split(
                        os.path.abspath(inv['path']))
                    volumes[docker_dir] = {}
                    binds[docker_dir] = dir_name
                    remaped_job['inputs'][inp][num]['path'] = '/'.join(
                        [docker_dir, file_name])
            return volumes, BindDict(binds), remaped_job

    @property
    def _envvars(self):
        envvars = self.tool.get('adapter', {}).get('environment', {})
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
        os.mkdir(job_dir)
        os.chmod(job_dir, os.stat(job_dir).st_mode | stat.S_IROTH |
                 stat.S_IWOTH)
        job = self.provide_files(job, os.path.abspath(job_dir))
        adapter = Adapter(self.tool)
        volumes, binds, remaped_job = self._volumes(job)
        volumes['/' + job_dir] = {}
        binds['/' + job_dir] = os.path.abspath(job_dir)
        container = self._run(['bash', '-c', adapter.cmd_line(remaped_job)],
                              vol=volumes, bind=binds, env=self._envvars,
                              work_dir='/' + job_dir)
        container.get_stderr(file='/'.join([os.path.abspath(job_dir),
                                            self.stderr]))
        if not container.is_success():
            raise RuntimeError("err %s" % container.get_stderr())
        with open(os.path.abspath(job_dir) + '/result.json', 'w') as f:
            outputs = adapter.get_outputs(os.path.abspath(job_dir), job)
            for k, v in six.iteritems(outputs):
                if v:
                    meta = v.pop('meta', {})
                    with open(v['path'] + '.meta', 'w') as m:
                        json.dump(meta, m)
            json.dump(outputs, f)
            print(outputs)


class NativeRunner(Runner):
    def __init__(self, tool, working_dir='./', stdout=None, stderr=None):
        super(NativeRunner, self).__init__(tool, working_dir, stdout, stderr)

    def run(self, command):
        pass


if __name__ == '__main__':

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
