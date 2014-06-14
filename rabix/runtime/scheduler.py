import logging

from rabix import CONFIG
from rabix.common.util import import_name
from rabix.common.protocol import JobError
from rabix.runtime.tasks import TaskDAG, Task

log = logging.getLogger(__name__)
scheduler = None


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


class SequentialScheduler(object):
    def __init__(self, before_task=None, after_task=None):
        self.jobs = {}
        self.before_task = before_task or (lambda t: None)
        self.after_task = after_task or (lambda t: None)

    def get_worker(self, task):
        worker_config = CONFIG['scheduler']['workers'][task.__class__.__name__]
        if isinstance(worker_config, basestring):
            return import_name(worker_config)(task)
        if isinstance(worker_config, dict):
            return import_name(worker_config[task.app.TYPE])(task)
        raise TypeError('Worker config must be string or dict. Got %s' % type(worker_config))

    def submit(self, job):
        self.jobs[job.job_id] = job
        return self

    def run(self):
        for job in self.jobs.itervalues():
            if job.status != Job.QUEUED:
                continue
            job.status = Job.RUNNING
            try:
                self.run_job(job)
                job.status = Job.FINISHED
            except JobError, e:
                job.status = Job.FAILED
                job.error_message = str(e)

    def run_job(self, job):
        log.info('Running job %s', job)
        ready = job.tasks.get_ready_tasks()
        while ready:
            for task in ready:
                self.before_task(task)
                self.get_worker(task).run(async=False)
                if task.status == Task.FAILED:
                    raise JobError('Task %s failed. Reason: %s' % (task.task_id, task.result))
                self.after_task(task)
                job.tasks.resolve_task(task, task.status)
            ready = job.tasks.get_ready_tasks()


def get_scheduler():
    global scheduler
    if scheduler:
        return scheduler
    scheduler = import_name(CONFIG['scheduler']['class'])(**CONFIG['scheduler'].get('options', {}))
    return scheduler
