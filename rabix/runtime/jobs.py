import logging
from rabix.runtime.tasks import TaskDAG

log = logging.getLogger(__name__)


class Job(object):
    QUEUED, RUNNING, FINISHED, CANCELED, FAILED = (
        'queued', 'running', 'finished', 'canceled', 'failed'
    )

    def __init__(self, job_id):
        self.status = Job.QUEUED
        self.job_id = job_id
        self.tasks = TaskDAG(task_prefix=str(job_id))
        self.error_message = None
        self.warnings = []

    __str__ = __unicode__ = __repr__ = lambda self: (
        '{0.__class__.__name__}[{0.job_id}]'.format(self)
    )


class AppJob(Job):
    def __init__(self, job_id, app):
        super(AppJob, self).__init__(job_id)
        self.app = app
        self.app.validate()


class RunJob(AppJob):
    def __init__(self, job_id, app, inputs=None, params=None):
        super(RunJob, self).__init__(job_id, app)
        self.inputs = inputs or {}
        self.params = params or {}
        self.tasks.add_from_app(app, inputs)

    def get_outputs(self):
        return self.tasks.get_outputs()


class InstallJob(AppJob):
    def __init__(self, job_id, app):
        super(InstallJob, self).__init__(job_id, app)
        self.tasks.add_install_tasks(app)
