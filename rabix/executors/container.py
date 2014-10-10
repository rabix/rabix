import logging
from docker.errors import APIError


def provide_image(image_id, uri, docker_client):
    if filter(lambda x: (image_id in x['Id']), docker_client.images()):
        return
    else:
        if not uri:
            logging.error('Image cannot be pulled')
            raise Exception('Cannot pull image')
        repo, tag = uri.split('#')
        repo = repo.lstrip('docker://')
        docker_client.pull(repo, tag)
        if filter(lambda x: (image_id in x['Id']),
                  docker_client.images()):
            return
        raise Exception('Image not found')


def make_config(image, command, user, volumes, mem_limit, ports, environment,
                entrypoint, cpu_shares, working_dir):
    config = {'Image': image,
              'Cmd': command,
              'AttachStdin': False,
              'AttachStdout': False,
              'AttachStderr': False,
              'Tty': False,
              'Privileged': False,
              'Memory': mem_limit,
              'ExposedPorts': ports,
              'User': user,
              'Volumes': volumes,
              'Env': environment,
              'Entrypoint': entrypoint,
              'CpuShares': cpu_shares,
              'WorkingDir': working_dir
    }
    return config


class Container(object):

    def __init__(self, docker_client, env, cmd, user=None, volumes=None,
                 mem_limit=0, ports=None, environment=None, entrypoint=None,
                 cpu_shares=None, working_dir=None):
        self.docker_client = docker_client
        self.image_id = env['container']['imageId']
        self.uri = env['container']['uri']
        self.cmd = cmd
        self.user = user
        self.volumes = volumes
        self.mem_limit = mem_limit
        self.ports = ports #
        self.environment = environment # ["PASSWORD=xxx"] or {"PASSWORD": "xxx"}
        self.entrypoint = entrypoint #
        self.cpu_shares = cpu_shares
        self.working_dir = working_dir
        #detach, stdin_open,
        #dns,
        #domainname, memswap_limit
        self.config = make_config(self.image_id, self.cmd, self.user,
                                  self.volumes, self.mem_limit, self.ports,
                                  self.environment, self.entrypoint,
                                  self.cpu_shares, self.working_dir)
        provide_image(self.image_id, self.uri, self.docker_client)
        try:
            self.container = self.docker_client.create_container_from_config(
                self.config)
        except APIError as e:
            if e.response.status_code == 404:
                logging.info('Image %s not found:' % self.image_id)
                raise RuntimeError('Image %s not found:' % self.image_id)
            raise RuntimeError('Failed to create Container')

    def start(self, binds):
        try:
            self.docker_client.start(container=self.container, binds=binds)
        except APIError:
            logging.error('Failed to run container %s' % self.container)
            raise RuntimeError('Unable to run container from image %s:' % self.image_id)

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


    def get_stdout(self):
        self.wait()
        return self.docker_client.logs(self.container, stdout=True, stderr=False,
                                       stream=False, timestamps=False)

    # TODO : test and finish
    def pipe_stdout(self, container, pipe):
        stream = self.docker_client.logs(container, stdout=True, stream=True)
        while True:
            try:
                pipe.write(stream.next())
            except StopIteration:
                pipe.close()
                return
            else:
                raise RuntimeError

    def get_stderr(self):
        self.wait()
        return self.docker_client.logs(self.container, stdout=False, stderr=True,
                                       stream=False, timestamps=False)


