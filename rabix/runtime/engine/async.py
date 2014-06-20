import time
import logging
import multiprocessing

import rq
import redis

from rabix import CONFIG
from rabix.common.util import import_name, get_import_name
from rabix.runtime.tasks import Task
from rabix.runtime.engine.base import Engine

log = logging.getLogger(__name__)


class AsyncEngine(Engine):
    def __init__(self, before_task=None, after_task=None):
        super(AsyncEngine, self).__init__(before_task, after_task)
        self.running = []
        self.total_ram = CONFIG['engine']['ram_mb']
        self.total_cpu = multiprocessing.cpu_count()
        self.available_ram = self.total_ram
        self.available_cpu = self.total_cpu
        self.multi_cpu_lock = False

    def _res(self):
        return '%s/%s%s;%s/%s' % (
            self.available_cpu,
            self.total_cpu, 'L' if self.multi_cpu_lock else '',
            self.available_ram,
            self.total_ram
        )

    def acquire_resources(self, task):
        res = task.resources
        log.debug('[resources: %s] Acquiring %s for %s',
                  self._res(), res, task)
        if res.mem_mb > self.available_ram:
            return False
        if (res.cpu == res.CPU_ALL and
                (self.multi_cpu_lock or self.available_cpu != self.total_cpu)):
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
        log.debug('[resources: %s] Releasing %s from %s',
                  self._res(), res, task)
        self.available_ram += res.mem_mb
        if res.cpu == res.CPU_ALL:
            self.multi_cpu_lock = False
        else:
            self.available_cpu += res.cpu

    def get_result_or_raise_error(self, async_result):
        raise NotImplementedError()

    def run_task_async(self, runner):
        raise NotImplementedError()

    def check_async_result_ready(self, async_result):
        raise NotImplementedError()

    def process_result(self, job, task, async_result):
        try:
            task.result = self.get_result_or_raise_error(async_result)
            task.status = Task.FINISHED
            log.info('Finished: %s', task)
            log.debug('Result: %s', task.result)
        except Exception as e:
            log.error('Failed: %s', task.task_id)
            task.status = Task.FAILED
            task.result = e
        self.after_task(task)
        self.release_resources(task)
        job.tasks.resolve_task(task)

    def _run_ready_tasks(self):
        for job, task in self._iter_ready():
            runner = self.get_runner(task)
            if not task.resources:
                task.resources = runner.get_requirements()
            if not self.acquire_resources(task):
                continue
            self.before_task(task)
            task.status = Task.RUNNING
            log.info('Running %s', task)
            log.debug('Arguments: %s', task.arguments)
            result = self.run_task_async(runner)
            self.running.append([job, task, result])

    def run_all(self):
        while True:
            self._run_ready_tasks()
            to_remove = []
            for ndx, item in enumerate(self.running):
                if self.check_async_result_ready(item[2]):
                    self.process_result(*item)
                    to_remove.append(ndx)
            if to_remove:
                self.running = [
                    x for n, x in enumerate(self.running) if n not in to_remove
                ]
                has_ready = self._update_jobs_check_ready()
                if not self.running and not has_ready:
                    return
            else:
                time.sleep(1)


class MultiprocessingEngine(AsyncEngine):
    def __init__(self, before_task=None, after_task=None):
        super(MultiprocessingEngine, self).__init__(before_task, after_task)
        self.pool = multiprocessing.Pool()

    def get_result_or_raise_error(self, async_result):
        return async_result.get()

    def run_task_async(self, runner):
        return self.pool.apply_async(runner)

    def check_async_result_ready(self, async_result):
        return async_result.ready()


class RQEngine(AsyncEngine):
    def __init__(self, before_task=None, after_task=None):
        super(RQEngine, self).__init__(before_task, after_task)
        self.queue = rq.Queue(connection=redis.Redis(), default_timeout=-1)
        self.failed = rq.Queue('failed', connection=redis.Redis())

    def check_async_result_ready(self, async_result):
        async_result.refresh()
        return async_result.get_status() in ['finished', 'failed']

    def get_result_or_raise_error(self, async_result):
        if async_result.get_id() in self.failed.job_ids:
            raise RuntimeError('Task failed.')
        return self.queue.fetch_job(async_result.get_id()).result

    def run_task_async(self, runner):
        return self.queue.enqueue(
            rq_work, get_import_name(runner.__class__), runner.task
        )


def rq_work(importable, task):
    runner = import_name(importable)
    return runner(task).run()
