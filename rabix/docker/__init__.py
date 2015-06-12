from rabix.docker.docker_app import DockerContainer, docker_client
from rabix.docker.container import find_image, get_image


def init(context):
    context.add_type('DockerRequirement', DockerContainer.from_dict)
