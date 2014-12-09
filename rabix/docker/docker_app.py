from rabix.cli.cli_app import Container
from rabix.docker.runner import DockerRunner


class DockerContainer(Container):

    def __init__(self, uri, image_id):
        super(DockerContainer, self).__init__()
        self.uri = uri
        self.image_id = image_id

    def install(self):
        runner = DockerRunner(self)
        runner.install()

    def run(self, app, job):  # should be run(self, cmd_line, job)
        runner = DockerRunner(app)
        return runner.run_job(job.to_dict())

    def to_dict(self):
        return {
            "@type": "Docker",
            "type": "docker",
            "uri": self.uri,
            "imageId": self.image_id
        }

    @classmethod
    def from_dict(cls, context, d):
        return cls(d['uri'], d.get('imageId'))
