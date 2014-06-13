import logging

from rabix import CONFIG
from rabix.common.util import import_name
from rabix.runtime.tasks import TaskDAG

log = logging.getLogger(__name__)
scheduler = None


class Job(object):
    QUEUED, RUNNING, FINISHED, CANCELED, FAILED = 'queued', 'running', 'finished', 'canceled', 'failed'

    def __init__(self, job_id):
        self.status = Job.QUEUED
        self.job_id = job_id
        self.tasks = TaskDAG(task_prefix=str(job_id))

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
        self.assignments = {}  # task_id: worker
        self.before_task = before_task or (lambda t: None)
        self.after_task = after_task or (lambda t: None)

    def get_worker(self, task):
        worker_config = CONFIG['scheduler']['workers'][task.__class__.__name__]
        if isinstance(worker_config, basestring):
            worker_cls = import_name(worker_config)
            log.debug('Worker for %s: %s', task, worker_cls)
            return worker_cls(task)
        if isinstance(worker_config, dict):
            worker_cls = import_name(worker_config[task.app.TYPE])
            log.debug('Worker for %s: %s', task, worker_cls)
            return worker_cls(task)
        raise TypeError('Worker config must be string or dict. Got %s' % type(worker_config))

    def submit(self, job):
        self.jobs[job.job_id] = job
        return self

    def run(self):
        for job in self.jobs.itervalues():
            if job.status != Job.QUEUED:
                continue
            job.status = Job.RUNNING
            self.run_job(job)

    def run_job(self, job):
        log.info('Running job %s', job)
        ready = job.tasks.get_ready_tasks()
        while ready:
            for task in ready:
                log.debug('Running task %s with %s', task, task.arguments)
                self.before_task(task)
                worker = self.assignments[task.task_id] = self.get_worker(task)
                result = worker.run(async=False)
                task.status = worker.report()
                self.after_task(task)
                if isinstance(result, Exception):
                    raise RuntimeError('Task %s failed. Reason: %s' % (task.task_id, result))
                log.debug('Result for %s: %s', task, result)
                job.tasks.resolve_task(task.task_id, result)
            ready = job.tasks.get_ready_tasks()


def get_scheduler():
    global scheduler
    if scheduler:
        return scheduler
    scheduler = import_name(CONFIG['scheduler']['class'])(**CONFIG['scheduler'].get('options', {}))
    return scheduler
