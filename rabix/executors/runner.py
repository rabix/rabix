import os
import docker
import logging
import uuid
import stat
import copy
from rabix.executors.container import Container
from rabix.cliche.adapter import Adapter


class BindDict(dict):
    def items(self):
        ret = []
        for k, v in self.iteritems():
            ret.append((v, k))
        return ret


class Runner(object):
    WORKING_DIR = '/work'

    def __init__(self, tool, working_dir='./', stdout=None, stderr=None):
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


class DockerRunner(Runner):
    def __init__(self, tool, working_dir='./', dockr=None, stdout=None,
                 stderr=None):
        super(DockerRunner, self).__init__(tool, working_dir, stdout, stderr)
        self.docker_client = dockr or docker.Client(version='1.12')

    def volumes(self, job):
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

    def _run(self, command, vol=None, bind=None, user=None, work_dir=None):
        volumes = vol or {self.WORKING_DIR: {}}
        working_dir = work_dir or self.WORKING_DIR
        user = user or ':'.join([str(os.getuid()), str(os.getgid())])
        container = Container(self.docker_client, self.enviroment, command,
                              user=user, volumes=volumes,
                              working_dir=working_dir)
        binds = bind or {self.working_dir: self.WORKING_DIR}
        # TODO : Add mem_limit, ports, environment, entrypoint, cpu_shares
        container.start(binds)
        return container

    def run_job(self, job, job_id=None):
        job_dir = job_id or self.rnd_name()
        os.mkdir(job_dir)
        os.chmod(job_dir, os.stat(job_dir).st_mode | stat.S_IROTH |
                 stat.S_IWOTH | stat.S_IXOTH)
        adapter = Adapter(self.tool)
        volumes, binds, remaped_job = self.volumes(job)
        volumes['/' + job_dir] = {}
        binds['/' + job_dir] = os.path.abspath(job_dir)
        container = self._run(['bash', '-c', adapter.cmd_line(remaped_job)],
                              vol=volumes, bind=binds, work_dir='/' + job_dir)
        if not container.is_success():
            raise RuntimeError("err %s" % container.get_stderr())
        return adapter.get_outputs(os.path.abspath(job_dir), job)


class NativeRunner(Runner):
    def __init__(self, tool, working_dir='./', stdout=None, stderr=None):
        super(NativeRunner, self).__init__(tool, working_dir, stdout, stderr)

    def run(self, command):
        pass
