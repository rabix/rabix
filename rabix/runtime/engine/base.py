import six
import logging

from rabix import CONFIG
from rabix.common.util import import_name
from rabix.common.protocol import JobError
from rabix.runtime.tasks import Task
from rabix.runtime.jobs import Job

log = logging.getLogger(__name__)


class Engine(object):
    def __init__(self, before_task=None, after_task=None):
        self.jobs = {}
        self.before_task = before_task or (lambda t: None)
        self.after_task = after_task or (lambda t: None)

    __str__ = __unicode__ = __repr__ = lambda self: (
        '%s[%s jobs]' % (self.__class__.__name__, len(self.jobs))
    )

    def get_runner(self, task):
        runner_cfg = CONFIG['runners'][task.__class__.__name__]
        if isinstance(runner_cfg, six.string_types):
            return import_name(runner_cfg)(task)
        return import_name(runner_cfg[task.app.TYPE])(task)

    def run(self, *jobs):
        for job in jobs:
            self.jobs[job.job_id] = job
        self.run_all()

    def run_all(self):
        pass

    def _iter_ready(self):
        for job in six.itervalues(self.jobs):
            for task in job.tasks.get_ready_tasks():
                yield job, task

    def _update_jobs_check_ready(self):
        has_ready = False
        for job in six.itervalues(self.jobs):
            if job.tasks.get_ready_tasks():
                has_ready = True
                continue
            else:
                failed = any([
                    t.status != Task.FINISHED for t in job.tasks.iter_tasks()
                ])
                job.status = Job.FAILED if failed else Job.FINISHED
        return has_ready


class SequentialEngine(Engine):
    def run_task(self, task):
        task.status = Task.RUNNING
        try:
            task.result = self.get_runner(task).run()
            task.status = Task.FINISHED
        except Exception as e:
            log.exception('Task error (%s)', task.task_id)
            task.status = Task.FAILED
            task.result = e

    def run_all(self):
        for job in six.itervalues(self.jobs):
            if job.status != Job.QUEUED:
                continue
            job.status = Job.RUNNING
            try:
                self._run_job(job)
                job.status = Job.FINISHED
            except JobError as e:
                job.status = Job.FAILED
                job.error_message = str(e)

    def _run_job(self, job):
        log.info('Running job %s', job)
        ready = job.tasks.get_ready_tasks()
        while ready:
            for task in ready:
                self.before_task(task)
                self.run_task(task)
                if task.status == Task.FAILED:
                    raise JobError('Task %s failed. Reason: %s' %
                                   (task.task_id, task.result))
                self.after_task(task)
                job.tasks.resolve_task(task)
            ready = job.tasks.get_ready_tasks()
