import logging
from rabix.runtime.tasks import TaskDAG

log = logging.getLogger(__name__)


class Job(object):
    QUEUED, RUNNING, FINISHED, CANCELED, FAILED = 'queued', 'running', 'finished', 'canceled', 'failed'

    def __init__(self, job_id):
        self.status = Job.QUEUED
        self.job_id = job_id
        self.tasks = TaskDAG(task_prefix=str(job_id))
        self.error_message = None
        self.warnings = []

    __str__ = __unicode__ = __repr__ = lambda self: '%s[%s]' % (self.__class__.__name__, self.job_id)


class PipelineJob(Job):
    def __init__(self, job_id, pipeline):
        super(PipelineJob, self).__init__(job_id)
        self.pipeline = pipeline
        self.pipeline.validate()


class RunJob(PipelineJob):
    def __init__(self, job_id, pipeline, inputs=None, params=None):
        super(RunJob, self).__init__(job_id, pipeline)
        self.inputs = inputs or {}
        self.params = params or {}
        self.tasks.add_from_pipeline(pipeline, inputs)

    def get_outputs(self):
        return self.tasks.get_outputs()


class InstallJob(PipelineJob):
    def __init__(self, job_id, pipeline):
        super(InstallJob, self).__init__(job_id, pipeline)
        self.tasks.add_install_tasks(pipeline)


