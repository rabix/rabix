import logging

from rabix.runtime.builtins.dockr import DockerApp, DockerRunner, DockerAppInstaller
from rabix.runtime.builtins.io import InputRunner
from rabix.runtime.builtins.mocks import MockRunner, MockApp
from rabix.runtime.models import Pipeline
from rabix.runtime.tasks import TaskDAG, Worker, InputTask, OutputTask, PipelineStepTask, AppInstallTask

log = logging.getLogger(__name__)


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
        if not isinstance(pipeline, Pipeline):
            raise TypeError('Not a pipeline: %s' % pipeline)
        self.pipeline = pipeline
        self.pipeline.validate()


class RunJob(PipelineJob):
    def __init__(self, job_id, pipeline, inputs=None, params=None):
        super(RunJob, self).__init__(job_id, pipeline)
        self.inputs = inputs or {}
        self.params = params or {}
        self.tasks.add_from_pipeline(pipeline, inputs)

    def get_outputs(self):
        result = {}
        for task in [x['task'] for x in self.tasks.dag.node.itervalues()]:
            if isinstance(task, OutputTask):
                result[task.task_id] = task.arguments
        return result


class InstallJob(PipelineJob):
    def __init__(self, job_id, pipeline):
        super(InstallJob, self).__init__(job_id, pipeline)
        for app_id, app in self.pipeline.apps.iteritems():
            self.tasks.add_task(AppInstallTask(app, task_id=app_id))


class SequentialScheduler(object):
    def __init__(self):
        super(SequentialScheduler, self).__init__()
        self.jobs = {}
        self.assignments = {}  # task_id: worker

    def get_worker(self, task):
        if isinstance(task, InputTask):
            return InputRunner(task)
        elif isinstance(task, OutputTask):
            return Worker(task)
        elif isinstance(task, PipelineStepTask):
            return {
                DockerApp: DockerRunner,
                MockApp: MockRunner,
            }[task.app.__class__](task)
        elif isinstance(task, AppInstallTask):
            return {
                DockerApp: DockerAppInstaller,
                MockApp: Worker,
            }[task.app.__class__](task)
        raise ValueError('No runner for task %s' % task)

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
                worker = self.assignments[task.task_id] = self.get_worker(task)
                result = worker.run(async=False)
                task.status = worker.report()
                if isinstance(result, Exception):
                    raise RuntimeError('Task %s failed. Reason: %s' % (task.task_id, result))
                log.debug('Result for %s: %s', task, result)
                job.tasks.resolve_task(task.task_id, result)
            ready = job.tasks.get_ready_tasks()
