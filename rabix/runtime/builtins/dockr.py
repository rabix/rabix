import pwd
import logging
import os
import signal
import stat

import docker

from rabix.common.errors import ResourceUnavailable
from rabix.common.util import handle_signal
from rabix.common.protocol import WrapperJob, Outputs, JobError
from rabix.runtime import from_json, to_json
from rabix.runtime.models import App, AppSchema
from rabix.runtime.tasks import Worker, PipelineStepTask

log = logging.getLogger(__name__)
MOUNT_POINT = '/rabix'


class DockerApp(App):
    TYPE = 'app/tool/docker'

    image_ref = property(lambda self: self['docker_image_ref'])
    wrapper_id = property(lambda self: self['wrapper_id'])

    def _validate(self):
        self._check_field('docker_image_ref', dict, null=False)
        self._check_field('wrapper_id', basestring, null=False)
        self._check_field('schema', AppSchema, null=False)
        self.schema.validate()


class DockerRunner(Worker):
    """
    Runs docker apps. Instantiates a container from specified image, mounts the current directory and runs entry point.
    A directory is created for each job.
    """
    def __init__(self, task):
        super(DockerRunner, self).__init__(task)
        if not isinstance(task, PipelineStepTask):
            raise TypeError('Can only run pipeline step tasks.')
        if not isinstance(task.app, DockerApp):
            raise TypeError('Can only run app/tool/docker.')
        docker_image_ref = task.app.image_ref
        self.image_repo = docker_image_ref.get('image_repo')
        self.image_tag = docker_image_ref.get('image_tag')
        if not self.image_repo or not self.image_tag:
            raise NotImplementedError('Currently, can only run images specified by repo+tag.')
        self.container = None
        self.image_id = None
        self._docker_client = None

    def run_and_wait(self):
        self.image_id = get_image(self.docker_client, self.image_repo, self.image_tag)['Id']
        self.container = Container(self.docker_client, self.image_id, mount_point=MOUNT_POINT)
        wrp_job = WrapperJob(self.task.app.wrapper_id, job_id=self.task.task_id,
                             args=self._fix_input_paths(self.task.arguments), resources=self.task.resources)
        task_dir = self.task.task_id
        os.mkdir(task_dir)
        os.chmod(task_dir, os.stat(task_dir).st_mode | stat.S_IROTH | stat.S_IWOTH | stat.S_IXOTH)  # TODO: remove?
        in_file, out_file = [os.path.join(task_dir, f) for f in '__in__.json', '__out__.json']
        with open(in_file, 'w') as fp:
            to_json(wrp_job, fp)
        self.container.run_job('__in__.json', '__out__.json', cwd=task_dir)
        if not os.path.isfile(out_file):
            raise JobError('Job failed.')
        with open(out_file) as fp:
            result = from_json(fp)
        if isinstance(result, Exception):
            raise result
        return self._fix_output_paths(result) if isinstance(result, Outputs) else result

    def _fix_output_paths(self, result):
        mount_point = MOUNT_POINT if MOUNT_POINT.endswith('/') else MOUNT_POINT + '/'
        for k, v in result.outputs.iteritems():
            v = [os.path.abspath(os.path.join(mount_point, self.task.task_id, out)) for out in v if out]
            for out in v:
                if not out.startswith(mount_point):
                    raise JobError('Output file outside mount point: %s' % out)
            v = [os.path.abspath(out[len(mount_point):]) for out in v]
            result.outputs[k] = v
        return result

    def _fix_input_paths(self, args):
        # TODO: Some other way to transform paths. This is really bad.
        log.debug('_fix_input_paths(%s)', args)
        if not isinstance(args, dict):
            return args
        if set(args.keys()) != {'$inputs', '$params'}:
            log.debug('_fix_input_paths: Keys do not match: %s', set(args.keys()))
            return args
        if not isinstance(args['$inputs'], dict):
            log.debug('_fix_input_paths: Type of $inputs is %s', type(args['$inputs']))
            return args
        args['$inputs'] = {k: self._transform_input(v) for k, v in args.get('$inputs', {}).iteritems()}
        log.debug('_fix_input_paths -> %s', args)
        return args

    def _transform_input(self, inp):
        if not isinstance(inp, list):
            return inp
        cwd = os.path.abspath('.') + '/'
        for i in inp:
            if not i.startswith(cwd):
                raise ValueError('Inputs and outputs must be passed as absolute paths. Got %s' % i)
        return [os.path.join(MOUNT_POINT, i[len(cwd):]) for i in inp]

    @property
    def docker_client(self):
        if self._docker_client is None:
            self._docker_client = docker.Client(os.environ.get('DOCKER_HOST'))
        return self._docker_client


class DockerAppInstaller(Worker):
    def run_and_wait(self):
        if not isinstance(self.task.app, DockerApp):
            raise TypeError('Can only install app/tool/docker')
        repo, tag = self.task.app.image_ref['image_repo'], self.task.app.image_ref['image_tag']
        get_image(docker.Client(os.environ.get('DOCKER_HOST')), repo, tag)


class Container(object):
    """
    Convenience wrapper around docker container.
    Instantiation of the class does not make the docker container. Call run methods to do that.
    """
    def __init__(self, docker_client, image_id, container_config=None, mount_point='/rabix'):
        self.docker = docker_client
        self.base_image_id = image_id
        self.base_cmd = docker_client.inspect_image(image_id)['config']['Cmd']
        self.mount_point = mount_point
        self.config = {
            'Image': self.base_image_id,
            'AttachStdin': False,
            'AttachStdout': False,
            'AttachStderr': False,
            'Tty': False,
            'Privileged': False,
            'Memory': 0,
            'Volumes': {self.mount_point: {}},
            'WorkingDir': self.mount_point,
            'Dns': None
        }
        self.config.update(container_config or {})
        self.binds = {os.path.abspath('.'): self.mount_point + ':rw'}
        self.container = None
        self.image = None

    def _check_container_ready(self):
        if not self.container:
            raise RuntimeError('Container not instantiated yet.')

    def inspect(self):
        self._check_container_ready()
        return self.docker.inspect_container(self.container)

    def is_running(self):
        self._check_container_ready()
        return self.inspect()['State']['Running']

    def wait(self, kill_on=(signal.SIGTERM, signal.SIGINT)):
        self._check_container_ready()

        def handler(signum, _):
            logging.info('Received signal %s, stopping.', signum)
            self.stop()

        with handle_signal(handler, *kill_on):
            self.docker.wait(self.container)

    def is_success(self):
        self._check_container_ready()
        self.wait()
        return self.inspect()['State']['ExitCode'] == 0

    def remove(self):
        self._check_container_ready()
        self.wait()
        self.docker.remove_container(self.container)

    def stop(self, nice=False):
        self._check_container_ready()
        return self.docker.stop(self.container) if nice else self.docker.kill(self.container)

    def print_log(self):
        self._check_container_ready()
        if self.is_running():
            for out in self.docker.attach(container=self.container, stream=True):
                print out.rstrip()
        else:
            print self.docker.logs(self.container)

    def commit(self, message=None, conf=None):
        self._check_container_ready()
        self.image = self.docker.commit(self.container['Id'], message=message, conf=conf)

    def run(self, command):
        log.info("Running command %s", command)
        self.config['User'] = '%d:%d' % (os.getuid(), pwd.getpwuid(os.getuid()).pw_gid)
        self.container = self.docker.create_container_from_config(dict(self.config, Cmd=command))
        self.docker.start(container=self.container, binds=self.binds)

    def run_and_print(self, command):
        self.run(command)
        self.wait()  # TODO: Remove this line when streaming works.
        self.print_log()

    def run_job(self, input_path, output_path, cwd=None):
        cmd = self.base_cmd + ['run', '-i', input_path, '-o', output_path]
        if cwd:
            cmd += ['--cwd', cwd]
        self.run(cmd)
        if self.is_success():
            self.remove()

    def schema(self, output=None):
        cmd = self.base_cmd + ['schema']
        cmd += ['--output', output] if output else []
        self.run_and_print(cmd)


def find_image(client, repo, tag='latest'):
    """
    Returns image dict if it exists locally, or None
    :param client: docker.Client
    :param repo: Docker repository name
    :param tag: Docker repository tag
    """
    images = client.images(repo)
    images = filter(lambda x: (repo + ':' + tag) in x['RepoTags'], images)
    return (images or [None])[0]


def get_image(client, repo, tag, pull_attempts=1):
    """
    Returns the image dict. If not found locally, will pull from the repository.
    :param client: docker.Client
    :param repo: Docker repository name
    :param tag: Docker repository tag
    :param pull_attempts: Number of attempts to pull the repo+tag.
    """
    img = find_image(client, repo, tag)
    if img:
        log.debug('Image found: %s:%s', repo, tag)
        return img
    if pull_attempts < 1:
        raise ResourceUnavailable('Image not found: %s tag: %s' % (repo, tag))
    log.info('No local image %s:%s. Downloading...', repo, tag)
    client.pull(repo, tag)
    return get_image(client, repo, tag, pull_attempts-1)
