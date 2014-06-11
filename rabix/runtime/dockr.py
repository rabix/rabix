import logging
import os
import signal
from rabix.common.errors import ResourceUnavailable
from rabix.common.util import handle_signal
import pwd

log = logging.getLogger(__name__)


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
        print command
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


def parse_repository_tag(repo):
    column_index = repo.rfind(':')
    if column_index < 0:
        return repo, ''
    tag = repo[column_index+1:]
    slash_index = tag.find('/')
    if slash_index < 0:
        return repo[:column_index], tag
    return repo, ''


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
