import time
import logging

import rq
import redis

import rabix.common.six as six
from rabix import CONFIG
from rabix.common.util import import_name, get_import_name
from rabix.runtime.tasks import Task
from rabix.runtime.engine.base import Engine

log = logging.getLogger(__name__)


class Node(object):
    def __init__(self, node_id, ram_mb, cpu=1, config=None, connection=None):
        self.node_id = node_id
        self.running = []
        self.total_ram = ram_mb
        self.total_cpu = cpu
        self.available_ram = self.total_ram
        self.available_cpu = self.total_cpu
        config = config or CONFIG
        redis_conf = config.get('redis', {})
        self.cn = connection or redis.Redis(**redis_conf)
        self.queue = rq.Queue(
            'rabix-%s' % node_id, connection=self.cn, default_timeout=-1
        )
        log.debug('Node initialized: %s', self)

    def __repr__(self):
        return 'Node[%s %s/%sMB %s/%scpu %s jobs]' % (
            self.node_id, self.available_ram, self.total_ram,
            self.available_cpu, self.total_cpu, len(self.running))
    __str__ = __unicode__ = __repr__

    def acquire_resources(self, task):
        log.debug('%s: Acquiring %s for %s', self, task.resources, task)
        cpu, ram = task.resources.cpu, task.resources.mem_mb
        if cpu == -1:
            cpu = self.total_cpu
        if ram > self.available_ram or cpu > self.available_cpu:
            return False
        self.available_ram -= ram
        self.available_cpu -= cpu
        return True

    def release_resources(self, task):
        log.debug('%s: Releasing %s from %s', self, task.resources, task)
        cpu, ram = task.resources.cpu, task.resources.mem_mb
        if cpu == -1:
            cpu = self.total_cpu
        self.available_ram += ram
        self.available_cpu += cpu

    def try_submit(self, task_gid, task, runner):
        if not self.acquire_resources(task):
            return False
        if not isinstance(runner, six.string_types):
            runner = get_import_name(runner.__class__)
        task.status = Task.RUNNING
        log.info('Running %s on %s', task, self)
        log.debug('Arguments: %s', task.arguments)
        rq_job = self.queue.enqueue(rq_work, runner, task)
        self.running.append([task_gid, task, rq_job])
        return True

    def try_process_result(self, task, rq_job):
        rq_job.refresh()
        if rq_job.get_status() not in ['finished', 'failed']:
            return
        task.result = rq_job.result
        task.status = Task.FINISHED \
            if rq_job.get_status() == 'finished' else Task.FAILED
        log.info('Done: %s', task)
        log.debug('Result: %s', task.result)
        self.release_resources(task)
        return task

    def pop_finished(self):
        to_remove, finished = [], []
        for ndx, (task_gid, task, rq_job) in enumerate(self.running):
            task = self.try_process_result(task, rq_job)
            if task:
                to_remove.append(ndx)
                finished.append([task, task_gid])
        self.running = [
            x for n, x in enumerate(self.running) if n not in to_remove
        ]
        return finished


def rq_work(importable, task):
    """Wrap so RQ can pickle the function."""
    return import_name(importable)(task).run()


class RQEngine(Engine):
    def __init__(self, config=None, before_task=None, after_task=None):
        super(RQEngine, self).__init__(before_task, after_task)
        self.nodes = {}
        config = config or CONFIG
        redis_conf = config.get('redis', {})
        self.cn = redis.Redis(**redis_conf)
        for node in config.get('nodes', []):
            self.nodes[node['node_id']] = Node(
                node['node_id'], node['ram_mb'], node['cpu'],
                connection=self.cn, config=config)
        log.debug('Engine initialized with %s nodes.', len(self.nodes))

    def run_all(self):
        log.info('%s: Running in burst mode.', self)
        if not self.nodes:
            raise RuntimeError('No nodes registered.')
        while True:
            self._run_ready_tasks()
            modified = False
            for node in six.itervalues(self.nodes):
                for task, job_id in node.pop_finished():
                    modified = True
                    self.jobs[job_id].tasks.resolve_task(task)
                    self.after_task(task)
            if not modified:
                time.sleep(1)
            elif not self._update_jobs_check_ready() and not self.busy:
                return

    @property
    def busy(self):
        return any(node.running for node in six.itervalues(self.nodes))

    def _run_ready_tasks(self):
        for job, task in self._iter_ready():
            runner = self.get_runner(task)
            if not task.resources:
                task.resources = runner.get_requirements()
            log.debug('Task ready to run: %s', task)
            for node in six.itervalues(self.nodes):
                if node.try_submit(job.job_id, task, runner):
                    self.before_task(task)
                    break
