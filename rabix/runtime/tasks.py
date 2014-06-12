import logging
import copy

import networkx as nx

from rabix.common.protocol import WrapperJob, Outputs
from rabix.common.util import rnd_name
from rabix.runtime.apps import App

log = logging.getLogger(__name__)


class Task(object):
    QUEUED, READY, RUNNING, FINISHED, CANCELED, FAILED = 'queued', 'ready', 'running', 'finished', 'canceled', 'failed'

    def __init__(self, task_id='', resources=None, arguments=None):
        self.status = Task.QUEUED
        self.task_id = task_id
        self.resources = resources
        self.arguments = arguments
        self.result = None

    def replacement(self, resources, arguments):
        result = copy.deepcopy(self)
        result.status = Task.QUEUED
        result.resources = resources
        result.arguments = self._replace_wrapper_job_with_task(arguments)
        return result

    def _replace_wrapper_job_with_task(self, obj):
        """ Traverses arguments, creates replacement tasks in place of wrapper jobs """
        if isinstance(obj, list):
            for ndx, item in enumerate(list):
                if isinstance(item, WrapperJob):
                    obj[ndx] = self.replacement(resources=item.resources, arguments=item.args)
                elif isinstance(obj, (dict, list)):
                    return self._replace_wrapper_job_with_task(obj)
        if isinstance(obj, dict):
            for key, val in obj.iteritems():
                if isinstance(val, WrapperJob):
                    obj[key] = self.replacement(resources=val.resources, arguments=val.args)
                elif isinstance(obj, (dict, list)):
                    return self._replace_wrapper_job_with_task(obj)
        return obj

    def iter_deps(self):
        """ Yields (path, task) for all tasks somewhere in the arguments tree. """
        if isinstance(self.arguments, Task):
            yield self.arguments
        for path, job in filter(None, Task._recursive_traverse([], self.arguments)):
            yield path, job

    @staticmethod
    def _recursive_traverse(path, obj):
        if not isinstance(obj, (dict, list)):
            return
        gen = obj.iteritems() if isinstance(obj, dict) else enumerate(obj)
        for k, v in gen:
            child_path = path + [k]
            if isinstance(v, Task):
                yield child_path, v
            elif isinstance(v, (dict, list)):
                tasks = filter(None, Task._recursive_traverse(child_path, v))
                for task in tasks:
                    yield task
            else:
                yield None


class AppTask(Task):
    def __init__(self, app, task_id='', resources=None, arguments=None):
        super(AppTask, self).__init__(task_id, resources, arguments)
        if not isinstance(app, App):
            raise TypeError('Wrong type for app: %s' % app.__class__.__name__)
        self.app = app


class PipelineStepTask(AppTask):
    def __init__(self, app, step, task_id='', resources=None, arguments=None):
        super(PipelineStepTask, self).__init__(task_id, resources)
        self.step = step
        self.arguments = arguments
        if arguments is None:
            self.arguments = {'$inputs': {inp: [] for inp in app.schema.inputs}, '$params': {}}


class AppInstallTask(AppTask):
    pass


class InputTask(Task):
    pass


class OutputTask(Task):
    pass


class TaskDAG(object):
    def __init__(self, task_prefix=''):
        self.task_prefix = task_prefix or rnd_name()
        self.dag = nx.DiGraph()

    def get_id_for_task(self, task):
        task_id = '%s.%s' % (self.task_prefix, task.task_id)
        counter = 0
        while task_id in self.dag:
            counter += 1
            task_id = '%s.%s' % (task_id, counter)
        return task_id

    def add_task(self, task):
        """ Modifies task id. Returns modified task. """
        task.task_id = self.get_id_for_task(task)
        self.dag.add_node(task.task_id, task=task)
        for path, dep in task.iter_deps():
            dep = self.add_task(dep)
            self.connect(dep.task_id, task.task_id, [], path)
        return task

    def connect(self, src_id, dst_id, src_path, dst_path):
        if src_id not in self.dag or dst_id not in self.dag:
            raise ValueError('Node does not exist.')
        conns = self.dag.get_edge_data(src_id, dst_id, default={'conns': []})['conns']
        conns.append((src_path, dst_path))
        self.dag.add_edge(src_id, dst_id, conns=conns)

    def get_task(self, task_id):
        return self.dag.node[task_id]['task']

    def resolve_task(self, task_id, result, resolution=Task.FINISHED):
        """
        If successful, result is stored and propagated downstream. Downstream tasks may get READY status.
        If result is a task, it is used as a replacement and dependency tasks are added to the graph.
        If failed, result should be an exception.
        If cancelled, result should be None.
        """
        task = self.get_task(task_id)
        valid_resolutions = Task.FINISHED, Task.CANCELED, Task.FAILED
        if resolution not in valid_resolutions:
            raise ValueError('Resolution must be one of: %s' % ', '.join(valid_resolutions))
        if resolution == Task.FAILED and not isinstance(result, Exception):
            raise TypeError('Results of failed tasks should be exceptions.')
        if isinstance(result, Outputs):
            result = result.outputs
        task.result = result
        if isinstance(result, WrapperJob):
            return self.add_task(task.replacement(resources=task.result.resources, arguments=task.result.args))
        for dst_id in self.dag.neighbors_iter(task_id):
            dst = self.dag.node[dst_id]
            for src_path, dst_path in self.dag.get_edge_data(task_id, dst_id)['conns']:
                self.propagate(task, dst, src_path, dst_path)

    def propagate(self, src, dst, src_path, dst_path):
        """" Propagates results. Possibly override to add shuttling tasks when multi node and no shared storage. """
        val = get_val_from_path(src.result, src_path)
        dst.arguments = update_on_path(dst.arguments, dst_path, val)


def get_val_from_path(obj, path, default=None):
    if path is None:
        return None
    if not isinstance(path, list):
        raise TypeError('Path must be a list.')
    if not path:
        return obj
    curr = obj
    for chunk in path:
        try:
            curr = curr[chunk]
        except (KeyError, IndexError):
            return default
    return curr


def update_on_path(obj, path, val):
    if path is None:
        return obj
    if not isinstance(path, list):
        raise TypeError('Path must be a list.')
    if not path:
        return val
    curr = obj
    for chunk in path[:-1]:
        curr = curr[chunk]
    parent, dest = curr, path[-1]
    if isinstance(parent[dest], list):
        val = val if isinstance(val, list) else [val]
        parent[dest].extend(val)
    else:
        parent[dest] = val
    return obj