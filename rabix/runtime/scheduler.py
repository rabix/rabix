import logging
import multiprocessing
import time

import rq
import redis

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


class Scheduler(object):
    def __init__(self, before_task=None, after_task=None):
        self.jobs = {}
        self.before_task = before_task or (lambda t: None)
        self.after_task = after_task or (lambda t: None)

    __str__ = __unicode__ = __repr__ = lambda self: '%s[%s jobs]' % (self.__class__.__name__, len(self.jobs))

    def get_worker(self, task):
        worker_config = CONFIG['workers'][task.__class__.__name__]
        if isinstance(worker_config, basestring):
            return import_name(worker_config)(task)
        if isinstance(worker_config, dict):
            return import_name(worker_config[task.app.TYPE])(task)
        raise TypeError('Worker config must be string or dict. Got %s' % type(worker_config))

    def run(self, *jobs):
        for job in jobs:
            self.jobs[job.job_id] = job
        self.run_all()

    def run_all(self):
        pass


class SequentialScheduler(Scheduler):
    def run_task(self, task):
        task.status = Task.RUNNING
        try:
            task.result = self.get_worker(task).run()
            task.status = Task.FINISHED
        except Exception, e:
            log.exception('Task error (%s)', task.task_id)
            task.status = Task.FAILED
            task.result = e

    def run_all(self):
        for job in self.jobs.itervalues():
            if job.status != Job.QUEUED:
                continue
            job.status = Job.RUNNING
            try:
                self._run_job(job)
                job.status = Job.FINISHED
            except JobError, e:
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
                    raise JobError('Task %s failed. Reason: %s' % (task.task_id, task.result))
                self.after_task(task)
                job.tasks.resolve_task(task)
            ready = job.tasks.get_ready_tasks()


class AsyncScheduler(Scheduler):
    def __init__(self, before_task=None, after_task=None):
        super(AsyncScheduler, self).__init__(before_task, after_task)
        self.running = []
        self.total_ram = CONFIG['scheduler']['ram_mb']
        self.total_cpu = multiprocessing.cpu_count()
        self.available_ram = self.total_ram
        self.available_cpu = self.total_cpu
        self.multi_cpu_lock = False

    def _res(self):
        return '%s/%s%s;%s/%s' % (self.available_cpu, self.total_cpu, 'L' if self.multi_cpu_lock else '',
                                  self.available_ram, self.total_ram)

    def acquire_resources(self, task):
        res = task.resources
        log.debug('[resources: %s] Acquiring %s for %s', self._res(), res, task)
        if res.mem_mb > self.available_ram:
            return False
        if res.cpu == res.CPU_ALL and (self.multi_cpu_lock or self.available_cpu != self.total_cpu):
            return False
        if res.cpu > self.available_cpu:
            return False
        self.available_ram -= res.mem_mb
        if res.cpu == res.CPU_ALL:
            self.multi_cpu_lock = True
        else:
            self.available_cpu -= res.cpu
        return True

    def release_resources(self, task):
        res = task.resources
        log.debug('[resources: %s] Releasing %s from %s', self._res(), res, task)
        self.available_ram += res.mem_mb
        if res.cpu == res.CPU_ALL:
            self.multi_cpu_lock = False
        else:
            self.available_cpu += res.cpu

    def get_result_or_raise_error(self, async_result):
        raise NotImplementedError()

    def run_task_async(self, worker):
        raise NotImplementedError()

    def check_async_result_ready(self, async_result):
        raise NotImplementedError()

    def process_result(self, job, task, async_result):
        try:
            task.result = self.get_result_or_raise_error(async_result)
            task.status = Task.FINISHED
            log.info('Finished: %s', task)
            log.debug('Result: %s', task.result)
        except Exception, e:
            log.error('Failed: %s', task.task_id)
            task.status = Task.FAILED
            task.result = e
        self.after_task(task)
        self.release_resources(task)
        job.tasks.resolve_task(task)

    def _iter_ready(self):
        for job in self.jobs.itervalues():
            for task in job.tasks.get_ready_tasks():
                yield job, task

    def _run_ready_tasks(self):
        for job, task in self._iter_ready():
            worker = self.get_worker(task)
            if not task.resources:
                task.resources = worker.get_requirements()
            if not self.acquire_resources(task):
                continue
            self.before_task(task)
            task.status = Task.RUNNING
            log.info('Running %s', task)
            log.debug('Arguments: %s', task.arguments)
            result = self.run_task_async(worker)
            self.running.append([job, task, result])

    def _update_jobs_check_ready(self):
        has_ready = False
        for job in self.jobs.itervalues():
            if job.tasks.get_ready_tasks():
                has_ready = True
                continue
            else:
                if filter(lambda t: t.status != Task.FINISHED, job.tasks.iter_tasks()):
                    job.status = Job.FAILED
                else:
                    job.status = Job.FINISHED
        return has_ready

    def run_all(self):
        while True:
            self._run_ready_tasks()
            to_remove = []
            for ndx, item in enumerate(self.running):
                if self.check_async_result_ready(item[2]):
                    self.process_result(*item)
                    to_remove.append(ndx)
            if to_remove:
                self.running = [x for n, x in enumerate(self.running) if n not in to_remove]
                has_ready = self._update_jobs_check_ready()
                if not self.running and not has_ready:
                    return
            else:
                time.sleep(1)


class MultiprocessingScheduler(AsyncScheduler):
    def __init__(self, before_task=None, after_task=None):
        super(MultiprocessingScheduler, self).__init__(before_task, after_task)
        self.pool = multiprocessing.Pool()

    def get_result_or_raise_error(self, async_result):
        return async_result.get()

    def run_task_async(self, worker):
        return self.pool.apply_async(worker)

    def check_async_result_ready(self, async_result):
        return async_result.ready()


class RQScheduler(AsyncScheduler):
    def __init__(self, before_task=None, after_task=None):
        super(RQScheduler, self).__init__(before_task, after_task)
        self.queue = rq.Queue(connection=redis.Redis())
        self.failed = rq.Queue('failed', connection=redis.Redis())

    def check_async_result_ready(self, async_result):
        async_result.refresh()
        return async_result.get_status() in ['finished', 'failed']

    def get_result_or_raise_error(self, async_result):
        if async_result.get_id() in self.failed.job_ids:
            raise RuntimeError('Task failed.')
        return self.queue.fetch_job(async_result.get_id()).result

    def run_task_async(self, worker):
        cls = worker.__class__
        return self.queue.enqueue(rq_work, '.'.join([cls.__module__, cls.__name__]), worker.task)


def rq_work(importable, task):
    worker = import_name(importable)
    return worker(task).run()


def get_scheduler():
    global scheduler
    if scheduler:
        return scheduler
    scheduler = import_name(CONFIG['scheduler']['class'])(**CONFIG['scheduler'].get('options', {}))
    return scheduler
