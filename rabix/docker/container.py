from __future__ import print_function
import logging
import shlex

import six
from docker.errors import APIError
from docker.utils.utils import parse_repository_tag

from rabix.common.errors import ResourceUnavailable, RabixError


log = logging.getLogger(__name__)


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

        get_image(docker_client, image_id=self.image_id, repo=self.uri)
        self.container =\
            self.docker_client.create_container_from_config(self.config)

    def start(self, binds=None, port_bindings=None):
        try:
            self.docker_client.start(container=self.container, binds=binds,
                                     port_bindings=port_bindings)
        except APIError:
            logging.error('Failed to run container %s' % self.container)
            raise RabixError('Unable to run container from image %s:'
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

    def write_stdout(self, file=None):
        write = lambda out: print(out.rstrip())
        f = None
        if file:
            f = open(file, 'w', buffering=1)
            write = f.write

        if self.is_running():
            for out in self.docker_client.attach(self.container, stdout=True,
                                                 stderr=False, stream=True,
                                                 logs=True):
                write(out)
            if f:
                f.close()
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


def match_image(image, query):
    """Returns image dict if it exists locally, or None"""
    if isinstance(query, str):
        return (len(query) >= 12 and image['Id'].startswith(query)) or \
            query in image['RepoTags'] or \
            (query + ":latest") in image['RepoTags']

    if isinstance(query, list):
        return any([match_image(image, q) for q in query])

    if isinstance(query, tuple):
        repo, tag = query
        return (repo + ":" + tag) in image['RepoTags']

    if isinstance(query, dict):
        image_id = query.get('image_id')
        repo = query.get('repo')
        tag = query.get('tag', 'latest')
        return (image_id and image['Id'].startswith(image_id)) or \
               ((repo + ":" + tag) in image['RepoTags'])

    return False


def find_image(client, query):
    images = client.images()
    it = (image for image in images if match_image(image, query))
    return next(it, None)


def get_image(client, repo=None, tag=None, image_id=None):
    """Returns the image dict. Pulls from repo if not found locally."""

    if not image_id and not repo:
        raise ValueError('Need either repository or image ID.')

    if repo and not tag:
        repo, tag = parse_repository_tag(repo)

    queries = [image_id, repo]
    if tag:
        queries.append((repo, tag))

    img = find_image(client, queries)

    if not img:
        log.info('Pulling %s', repo)
        client.pull(repo, tag)
        img = find_image(client, queries)

    if not img:
        raise ResourceUnavailable(repo, 'Image not found.')

    return img
