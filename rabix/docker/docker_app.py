from __future__ import absolute_import
import os
import shlex
import logging
import sys
import six
from docker.client import Client
from docker.utils import kwargs_from_env
from docker.errors import APIError

from rabix.cli import Container
from .container import get_image
from rabix.common.errors import RabixError


log = logging.getLogger(__name__)

DOCKER_DEFAULT_API_VERSION = "1.14"
DOCKER_DEFAULT_TIMEOUT = 60

DEFAULT_DOCKER_HOST = 'tcp://192.168.59.103:2376'
DEFAULT_DOCKER_CERT_PATH = os.path.join(os.path.expanduser("~"),
                                        '.boot2docker/certs/boot2docker-vm')
DEFAULT_DOCKER_TLS_VERIFY = '1'

DEFAULT_CONFIG = {
    "version": DOCKER_DEFAULT_API_VERSION,
    "timeout": DOCKER_DEFAULT_TIMEOUT,
}


def set_env():
    docker_host = os.environ.get('DOCKER_HOST', None)
    if not docker_host:
        os.environ['DOCKER_HOST'] = DEFAULT_DOCKER_HOST
    docker_cert_path = os.environ.get('DOCKER_CERT_PATH', None)
    if not docker_cert_path:
        os.environ['DOCKER_CERT_PATH'] = DEFAULT_DOCKER_CERT_PATH
    os.environ['DOCKER_TLS_VERIFY'] = DEFAULT_DOCKER_TLS_VERIFY


def docker_client_osx(**kwargs):
    set_env()
    env = kwargs_from_env()
    env['tls'].verify = False
    env.update(kwargs)
    return Client(**env)


def docker_client_linux(**kwargs):
    return Client(**kwargs)


def docker_client(cfg=None):
    if cfg:
        client_config = {
            "version": cfg.docker_client_version,
            "timeout": cfg.docker_client_timeout,
            }
    else:
        client_config = DEFAULT_CONFIG
    if sys.platform.startswith('darwin'):
        client = docker_client_osx(**client_config)
    elif sys.platform.startswith('linux'):
        client = docker_client_linux(**client_config)
    else:
        raise EnvironmentError('Unsupported OS')
    return client


def make_config(**kwargs):
    keys = ['Hostname', 'Domainname', 'User', 'Memory', 'MemorySwap',
            'CpuShares', 'Cpuset', 'AttachStdin', 'AttachStdout',
            'AttachStderr', 'PortSpecs', 'ExposedPorts', 'Tty', 'OpenStdin',
            'StdinOnce', 'Env', 'Cmd', 'Image', 'Volumes', 'WorkingDir',
            'Entrypoint', 'NetworkDisabled', 'OnBuild']
    cfg = {
        'AttachStdin': False,
        'AttachStdout': False,
        'AttachStderr': False,
        'Tty': False,
        'Privileged': False,
        }
    cfg.update({k[0].upper() + k[1:]: v for k, v in six.iteritems(kwargs)})
    cfg = {k: v for k, v in six.iteritems(cfg) if k in keys}
    entrypoint = cfg.get("Entrypoint")
    if isinstance(entrypoint, six.string_types):
        cfg['Entrypoint'] = shlex.split(entrypoint)
    return cfg


class DockerContainer(Container):

    def __init__(self, uri, image_id=None, user=None, dockr=None):
        super(DockerContainer, self).__init__()
        self.uri = uri.lstrip("docker://")\
            if uri and uri.startswith('docker:/') else uri

        self.image_id = image_id
        self.docker_client = dockr or docker_client()
        self.config = {}
        self.volumes = {}
        self.binds = {}
        self.user = user or ':'.join(
            [six.text_type(os.getuid()), six.text_type(os.getgid())]
        )

    def install(self, *args, **kwargs):

        image = get_image(
            self.docker_client,
            image_id=self.image_id,
            repo=self.uri
        )

        if not image:
            log.info('Image %s not found:' % self.image_id)
            raise RabixError('Image %s not found:' % self.image_id)

        # if not image['Id'].startswith(self.image_id):
        #
        #     raise RabixError(
        #         'Wrong id of pulled image: expected "%s", got "%s"'
        #         % (self.image_id, image['Id'])
        #     )

        self.image_id = image['Id']

    def get_mapping(self, paths):
        volumes = {}
        binds = {}

        for idx, p in enumerate(paths):
            mapping = '/mnt/%s/' % idx
            volumes[mapping] = {}
            binds[p] = mapping

        self.volumes = volumes
        self.binds = binds

        return dict(self.binds)

    def _start(self, cmd):
        self.config["Cmd"] = ['bash', '-c', cmd]
        try:
            self.container = self.docker_client.create_container_from_config(
                self.config)
        except APIError:
            raise RuntimeError('Failed to create Container')
        try:
            self.docker_client.start(container=self.container, binds=self.binds)
        except APIError:
            logging.error('Failed to run container %s' % self.container)
            raise RuntimeError('Unable to run container from image %s:'
                               % self.image_id)

    def run(self, cmd, job_dir, env=None):

        if not os.path.isabs(job_dir):
            raise RabixError('job_dir must be an abslute path.')

        for k, v in six.iteritems(self.binds):
            if job_dir.startswith(k):
                working_dir = '/'.join([v, job_dir[len(k):]])
                break
        else:
            raise RabixError("Invalid working dir: " + job_dir)

        cfg = {
            "Image": self.image_id,
            "User": self.user,
            "Volumes": self.volumes,
            "WorkingDir": working_dir,
            "Env": env
        }
        self.config = make_config(**cfg)
        self._start(cmd)
        self.get_stderr(file='/'.join([job_dir, 'out.err']))
        if not self.is_success():
            raise RabixError("Tool failed:\n%s" % self.get_stderr())

    def remove(self, success_only=False):
        self.wait()
        if not success_only or self.is_success():
            self.docker_client.remove_container(self.container)
        return self

    def inspect(self):
        return self.docker_client.inspect_container(self.container)

    def is_running(self):
        return self.inspect()['State']['Running']

    def wait(self):
        if self.is_running():
            self.docker_client.wait(self.container)
        return self

    def is_success(self):
        return self.wait().inspect()['State']['ExitCode'] == 0

    def get_stdout(self, file=None):
        if file:
            f = open(file, 'w', buffering=1)
        if self.is_running():
            for out in self.docker_client.attach(self.container, stdout=True,
                                                 stderr=False, stream=True,
                                                 logs=True):
                if file:
                    f.write(out.rstrip() + '\n')
                else:
                    print(out.rstrip())
            if file:
                f.close()
        else:
            print(self.docker_client.logs(self.container))
        return self

    def get_stderr(self, file=None):
        if file:
            f = open(file, 'w')
            if self.is_running():
                for out in self.docker_client.attach(self.container,
                                                     stdout=False, stderr=True,
                                                     stream=True, logs=True):
                    if file:
                        f.write(six.text_type(out).rstrip() + '\n')
                    else:
                        print(out.rstrip())
            else:
                f.write(self.docker_client.logs(self.container,
                                                stdout=False, stderr=True))
            f.close()
        else:
            print(self.docker_client.logs(self.container))
        return self

    def to_dict(self, context=None):
        return {
            "class": "DockerRequirement",
            "dockerPull": self.uri,
            "dockerImageId": self.image_id
        }

    @classmethod
    def from_dict(cls, context, d):
        return cls(d.get('dockerPull'))
