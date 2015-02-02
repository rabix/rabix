import os
import six
import shlex
import logging
import docker

from docker.errors import APIError

from rabix.cli.cli_app import Container
from rabix.docker.container import get_image
from rabix.common.errors import ResourceUnavailable, RabixError


log = logging.getLogger(__name__)

DOCKER_DEFAULT_API_VERSION = "1.12"
DOCKER_DEFAULT_TIMEOUT = 60


def docker_client(docker_host=None,
                  api_version=DOCKER_DEFAULT_API_VERSION,
                  timeout=DOCKER_DEFAULT_TIMEOUT,
                  tls=None):

    docker_host = docker_host or os.getenv("DOCKER_HOST", None)
    tls = False if tls is None else os.getenv("DOCKER_TLS_VERIFY", "") != ""
    docker_cert_path = os.getenv("DOCKER_CERT_PATH", "")  # ???

    return docker.Client(docker_host, api_version, timeout, tls)


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

    def __init__(self, uri, image_id, user=None, dockr=None):
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

        if self.image_id and not image['Id'].startswith(self.image_id):
            raise RabixError(
                'Wrong id of pulled image: expected "%s", got "%s"'
                % (self.image_id, image['Id'])
            )

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

    def run(self, cmd, job_dir):

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
            "WorkingDir": working_dir
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
            "@type": "Docker",
            "type": "docker",
            "uri": self.uri,
            "imageId": self.image_id
        }

    @classmethod
    def from_dict(cls, context, d):
        return cls(d.get('uri'), d.get('imageId'))
