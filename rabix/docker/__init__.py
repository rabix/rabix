from rabix.docker.docker_app import DockerContainer

DOCKER_API_VERSION = '1.12'
DEFAULT_TIMEOUT_SECONDS = 60


def init(context):
    context.add_type('Docker', DockerContainer.from_dict)
