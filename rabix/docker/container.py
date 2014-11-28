import logging
import six
import shlex

from docker.errors import APIError

from rabix.common.errors import ResourceUnavailable

log = logging.getLogger(__name__)


def ensure_image(docker_client, image_id, uri):
    if image_id and [x for x in docker_client.images() if x['Id'].startswith(
            image_id)]:
        log.debug("Provide image: found %s" % image_id)
        return
    else:
        if not uri:
            log.error('Image cannot be pulled: no URI given')
            raise Exception('Cannot pull image')
        repo, tag = parse_docker_uri(uri)
        log.info("Pulling image %s:%s" % (repo, tag))
        docker_client.pull(repo, tag)
        if filter(lambda x: (image_id in x['Id']),
                  docker_client.images()):
            return
        raise Exception('Image not found')


def parse_docker_uri(uri):
    repo, tag = uri.split('#')
    repo = repo.lstrip('docker://')
    return repo, tag


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


class Container(object):

    def __init__(self, docker_client, image_id, image_uri, cmd, user=None,
                 volumes=None, mem_limit=0, ports=None, environment=None,
                 entrypoint=None, cpu_shares=None, working_dir=None, **kwargs):
        self.docker_client = docker_client
        self.image_id = image_id
        self.uri = image_uri
        self.cmd = cmd
        self.user = user
        self.volumes = volumes
        self.mem_limit = mem_limit
        self.ports = ports
        self.environment = environment  # ["PASSWORD=xxx"]{"PASSWORD": "xxx"}
        self.entrypoint = entrypoint  #
        self.cpu_shares = cpu_shares
        self.working_dir = working_dir
        self.produced_image = None
        kwargs.update(
            {
                "Image": image_id,
                "Cmd": cmd,
                "User": user,
                "Volumes": volumes,
                "Memory": mem_limit,
                "ExposedPorts": ports,
                "Env": environment,
                "CpuShares": cpu_shares,
                "WorkingDir": working_dir
            })
        self.config = make_config(**kwargs)

        try:
            ensure_image(docker_client, self.image_id, self.uri)
            self.container = self.docker_client.create_container_from_config(
                self.config)
        except APIError as e:
            if e.response.status_code == 404:
                log.info('Image %s not found:' % self.image_id)
                raise RuntimeError('Image %s not found:' % self.image_id)
            raise RuntimeError('Failed to create Container')

    def start(self, binds=None, port_bindings=None):
        try:
            self.docker_client.start(container=self.container, binds=binds,
                                     port_bindings=port_bindings)
        except APIError:
            logging.error('Failed to run container %s' % self.container)
            raise RuntimeError('Unable to run container from image %s:'
                               % self.image_id)

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
            for out in self.docker_client.attach(self.container, stdout=False,
                                                 stderr=True, stream=True,
                                                 logs=True):
                if file:
                    f.write(str(out).rstrip() + '\n')
                else:
                    print(out.rstrip())
        else:
            print(self.docker_client.logs(self.container))
        return self

    def commit(self, message=None, conf=None, repository=None, tag=None):
        log.debug("repository: {}, ")
        self.produced_image = self.docker_client.commit(
            self.container['Id'], message=message, conf=make_config(**conf),
            repository=repository, tag=tag
        )
        return self


def find_image(client, image_id, repo=None, tag=None):
    """Returns image dict if it exists locally, or None"""
    images = client.images()
    tag = tag or 'latest'
    img = ([i for i in images if i['Id'].startswith(image_id)]
           if image_id else None)
    if not img:
        img = ([i for i in images if (repo + ':' + tag) in i['RepoTags']]
               if repo and tag else None)
    return (img or [None])[0]


def get_image(client, repo=None, tag=None, image_id=None):
    """Returns the image dict. Pulls from repo if not found locally."""

    if not image_id and not repo:
        raise ValueError('Need either repository or image ID.')

    img = None

    if image_id:
        img = find_image(client, image_id)

    if not img:
        log.info('Pulling %s', repo)
        client.pull(repo, tag)
        img = find_image(client, image_id, repo, tag)

    if not img:
        raise ResourceUnavailable(repo, 'Image not found.')

    return img
