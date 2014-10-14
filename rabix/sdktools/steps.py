from __future__ import print_function
import docker
import logging
import re
import shlex
from os import getenv
import six

from rabix.executors.container import Container
from rabix.common.errors import RabixError

log = logging.getLogger(__name__)

MOUNT_POINT = '/build'


def build(client, from_img, **kwargs):
    cmd = kwargs.pop('cmd', None)
    if not cmd:
        raise RabixError("Commands ('cmd') not specified!")
    cfg = make_config()
    mount_point = kwargs.pop('mount_point', MOUNT_POINT)
    container = Container(client, from_img, cfg, mount_point=mount_point)

    run_cmd = make_cmd(cmd, join=True)

    container.run(run_cmd, override_entrypoint=True)
    container.print_log()

    if container.is_success():
        message = kwargs.pop('message', None)
        register = kwargs.pop('register', {})
        cfg = {"Cmd": []}
        cfg.update(make_config(**kwargs))
        container.commit(
            message, cfg, repository=register.get('repo'),
            tag=register.get('tag')
        )
    else:
        raise RabixError("Build failed!")
    return container.produced_image['Id']


def run(client, from_img, **kwargs):
    cmd = kwargs.pop('cmd', None)
    if not cmd:
        raise RabixError("Commands ('cmd') not specified!")
    cfg = make_config(**kwargs)
    run_cmd = make_cmd(cmd, join=True)
    mount_point = kwargs.pop('mount_point', MOUNT_POINT)
    container = Container(client, from_img, cfg, mount_point=mount_point)
    container.run(run_cmd)
    container.print_log()
    if not container.is_success():
        raise RabixError(container.docker.logs(container.container))


def install_wrapper(client, from_img, **kwargs):
    cmd = [
        'pip install -e "git+https://github.com/rabix/rabix.git'
        '@devel#egg=rabix-core&subdirectory=rabix-core"',
        'cd ' + MOUNT_POINT,
        'pip install .'
    ]
    run(client, from_img, cmd=cmd)
    pass


def make_config(**kwargs):
    keys = ['Hostname', 'Domainname', 'User', 'Memory', 'MemorySwap',
            'CpuShares', 'Cpuset', 'AttachStdin', 'AttachStdout',
            'AttachStderr', 'PortSpecs', 'ExposedPorts', 'Tty', 'OpenStdin',
            'StdinOnce', 'Env', 'Cmd', 'Image', 'Volumes', 'WorkingDir',
            'Entrypoint', 'NetworkDisabled', 'OnBuild']

    cfg = {k.title(): v for k, v in six.iteritems(kwargs)}
    cfg = {k: v for k, v in six.iteritems(cfg) if k in keys}
    entrypoint = cfg.get("Entrypoint")
    if isinstance(entrypoint, six.string_types):
        cfg['Entrypoint'] = shlex.split(entrypoint)

    return cfg


def make_cmd(cmd, join=False):
    if isinstance(cmd, six.string_types):
        return shlex.split(cmd)
    elif isinstance(cmd, list) and len(cmd) > 1 and join:
        return ['/bin/sh', '-c', ' && '.join(cmd)]
    return cmd


class Runner(object):

    def __init__(self, docker, steps=None, context=None):
        self.types = {
            "run": run,
            "build": build
        }
        self.types.update(steps or {})

        self.context = context or {}

        self.docker = docker
        pass

    def run(self, config):
        steps = config['steps']
        for step in steps:
            step_name, step_conf = step.popitem()
            type_name = step_conf.pop('type', None)
            if not type_name:
                raise RabixError("Step type not specified!")

            step_type = self.types.get(type_name)
            if not step_type:
                raise RabixError("Unknown step type: %" % type_name)

            resolved = {k: self.resolve(v) for k, v in six.iteritems(step_conf)}

            img = resolved.pop('from', None)
            if not img:
                raise RabixError("Base image ('from') not specified!")

            log.info("Running step: %s" % step_name)
            self.context[step_name] = \
                step_type(self.docker, img, **resolved)
        pass

    def resolve(self, val):
        if isinstance(val, list):
            return [self.resolve(item) for item in val]
        elif isinstance(val, dict):
            return {k: self.resolve(v) for k, v in six.iteritems(val)}
        elif isinstance(val, six.string_types):
            resolved = re.sub("\$\{([a-zA-Z0-9_]+)\}",
                              lambda x: self.context[x.group(1)],
                              val)

            return resolved
        else:
            return val


def run_steps(config, docker_host=None, steps=None, context=None):
    docker_host = docker_host or getenv("DOCKER_HOST", None)
    r = Runner(docker.Client(docker_host, version="1.8"), steps, context)
    r.run(config)
