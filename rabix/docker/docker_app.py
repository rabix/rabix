from uuid import uuid4

from rabix.cli.cli_app import CliApp
from rabix.docker.runner import DockerRunner
from rabix.schema import IOSchema


class DockerApp(CliApp):

    def __init__(self, app_id, inputs, outputs,
                 app_description=None,
                 annotations=None,
                 platform_features=None,
                 adapter=None,
                 software_description=None,
                 requirements=None):
        super(DockerApp, self).__init__(
            app_id, inputs, outputs,
            app_description=app_description,
            annotations=annotations,
            platform_features=platform_features,
            adapter=adapter,
            software_description=software_description,
            requirements=requirements,
        )

    def install(self):
        runner = DockerRunner(self)
        runner.install()

    def run(self, job):
        runner = DockerRunner(self)
        return runner.run_job(job.to_dict())

    def to_dict(self):
        d = super(DockerApp, self).to_dict()
        d.update({
            "@type": "Docker",
            "inputs": self.inputs.to_dict(),
            "outputs": self.outputs.to_dict()
        })
        return d

    @classmethod
    def from_dict(cls, context, d):
        return cls(d.get('@id', str(uuid4())),
                   IOSchema(d['inputs']),
                   IOSchema(d['outputs']),
                   app_description=d.get('appDescription'),
                   annotations=d.get('annotations'),
                   platform_features=d.get('platform_features'),
                   adapter=context.from_dict(d.get('adapter')),
                   software_description=d.get('softwareDescription'),
                   requirements=d.get('requirements'))


def init(context):
    context.add_type('Docker', DockerApp.from_dict)
