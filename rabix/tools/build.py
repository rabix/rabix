import logging
import re
import six
import shlex

from os.path import abspath
from docker.utils.utils import parse_repository_tag

from rabix.docker.container import Container, get_image
from rabix.docker import docker_client
from rabix.common.errors import RabixError

log = logging.getLogger(__name__)

MOUNT_POINT = '/build'


def build(client, from_img, **kwargs):
    container = run_container(client, from_img, kwargs, {})
    if container.is_success():
        message = kwargs.pop('message', None)
        register = kwargs.pop('register', {})
        cfg = {"Cmd": []}
        cfg.update(**kwargs)

        container.commit(
            message, cfg, repository=register.get('repo'),
            tag=register.get('tag')
        )
    else:
        raise RabixError("Build failed!")
    return container.produced_image['Id']


def run(client, from_img, **kwargs):
    container = run_container(client, from_img, kwargs, kwargs)
    if not container.is_success():
        raise RabixError(container.docker_client.logs(container.container))


def run_container(client, from_img, kwargs, container_kwargs):

    cmd = kwargs.pop('cmd', None)
    if not cmd:
        raise RabixError("Commands ('cmd') not specified!")

    repo, tag = parse_repository_tag(from_img)
    img = get_image(client, from_img)
    if not img:
        raise RabixError("Unable to find image: %s" % img)

    mount_point = kwargs.pop('mount_point', MOUNT_POINT)
    run_cmd = make_cmd(cmd, join=True)

    container = Container(client, img['Id'],
                          "{}:{}".format(repo, tag),
                          run_cmd, volumes={mount_point: {}},
                          working_dir=mount_point, **container_kwargs)

    container.start({abspath('.'): mount_point})
    container.write_stdout()
    return container


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
                raise RabixError("Unknown step type: %s" % type_name)

            resolved = {k: self.resolve(v) for k, v
                        in six.iteritems(step_conf)}

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
    r = Runner(docker_client(docker_host), steps, context)
    r.run(config)
