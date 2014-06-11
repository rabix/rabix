import logging
from rabix.common.protocol import BaseJob, Outputs
from rabix.common.util import rnd_name


log = logging.getLogger(__name__)


class RunFailed(RuntimeError):
    pass


class Arguments(object):
    """
    Helper class for path-based access to JSON object/list tree structures.
    Used for both arguments and results of nodes/jobs.

    >>> a = Arguments({"x": ["a", "b"]})
    >>> a.update(["x", 0], "c")
    >>> assert a.get(["x", 0]) == "c"
    >>> assert a.data == {"x": ["c", "b"]}
    >>> a.update([], {"new": "val"})
    >>> assert a.get(["new"]) == "val"
    >>> assert a.get(["not", "valid"]) is None
    >>> assert a.get(["not", "valid"], default=42) == 42
    """
    def __init__(self, data=None):
        self.data = data or {}

    def update(self, path, val):
        """
        Replace object on path with val. If path is falsy, replace data with val.

        :param path: list of str or int that specifies node in the tree, or None to replace the tree.
        :param val: Value to put at the position of path.
        """
        if not path:
            self.data = val
            return
        if not isinstance(path, (list, tuple)):
            raise TypeError('Path must be a list.')
        curr = self.data
        for chunk in path[:-1]:
            curr = curr[chunk]
        parent, dest = curr, path[-1]
        if isinstance(parent[dest], list):
            val = val if isinstance(val, list) else [val]
            parent[dest].extend(val)
        else:
            parent[dest] = val

    def get(self, path, default=None):
        """
        Get the object at the position of path.
        :param path: list of str or int that specifies node in the tree, or None for the whole tree.
        :param default: If nothing found on or along the path, return this.
        """
        if not path:
            return self.data
        assert isinstance(path, list)
        curr = self.data
        for chunk in path:
            try:
                curr = curr[chunk]
            except (KeyError, IndexError):
                return default
        return curr

    def assure_input_in_arguments(self, input_name):
        if '$inputs' not in self.data:
            self.data['$inputs'] = {}
        if input_name not in self.data['$inputs']:
            self.data['$inputs'][input_name] = []

    def iter_jobs(self):
        """ Yields (path, job) for each job anywhere in self.data. """
        for path, job in filter(None, Arguments._recursive_traverse([], self.data)):
            yield path, job

    __str__ = __unicode__ = __repr__ = lambda self: str(self.data)

    @staticmethod
    def _recursive_traverse(path, obj):
        if not isinstance(obj, (dict, list)):
            return
        gen = obj.iteritems() if isinstance(obj, dict) else enumerate(obj)
        for k, v in gen:
            child_path = path + [k]
            if isinstance(v, BaseJob):
                yield child_path, v
            elif isinstance(v, (dict, list)):
                jobs = filter(None, Arguments._recursive_traverse(child_path, v))
                for job in jobs:
                    yield job
            else:
                yield None


class JobRelation(object):
    """
    Represents a relation between jobs in the graph. Created by JobGraph.connect().
    JobRelation.resolve() is called from JobNode.resolve() and transfers results downstream.
    JobRelation.new_src(node) is called from JobNode.resolve() when a job result is a Job.
    """

    def __init__(self, src_node, dst_node, src_path, dst_path):
        self.src_node = src_node
        self.dst_node = dst_node
        self.src_path = src_path
        self.dst_path = dst_path
        self.resolved = False

    __str__ = __unicode__ = __repr__ = lambda self: 'JobRelation[%s.%s->%s.%s]' % (
        self.src_node.node_id, self.src_path, self.dst_node.node_id, self.dst_path)

    def resolve(self, runner_map):
        default = [] if self.src_node.role == JobNode.FINAL else None  # Default to empty list for finished steps.
        val = self.src_node.result.get(self.src_path, default=default)
        if self.src_node.role in [JobNode.FINAL, JobNode.INPUT]:
            val = self.src_node.get_runner_class(runner_map).transform_output(val)
            val = self.dst_node.get_runner_class(runner_map).transform_input(val)
        self.dst_node.update_arg(self.dst_path, val)
        self.resolved = True

    def new_src(self, node):
        assert not self.resolved
        self.src_node = node


class JobNode(object):
    """
    Represents a job in the graph. Created by JobGraph.add_node().
    Call JobNode.resolve(result) to mark as resolved and propagate results downstream or replace with new node.
    Roles:
        INITIAL: Initial job for each step in pipeline. If jobs start returning jobs, the new jobs are MIDDLE.
        MIDDLE: See above. A step can have any number of MIDDLE jobs. Cannot tell in advance.
        FINAL: A dummy node. It's just there to make graph maintenance easy. Resolved automatically (result=args).
        INPUT: Handles the input files to pipeline. Not an actual step, but can take a while in case of download/copy.
        OUTPUT: Output jobs fix the path of files, and don't do much more. But it's the thing we care about.
    """
    INITIAL, MIDDLE, FINAL, INPUT, OUTPUT = 'initial', 'job', 'final', 'input', 'output'
    PENDING, RUNNING, DONE, FAILED = 'pending', 'running', 'done', 'failed'

    def __init__(self, node_id, step, app, arguments=None, resources=None, role=None):
        self.node_id = node_id
        self.step = step
        self.app = app
        self.arguments = arguments or Arguments()
        self.result = Arguments()
        self.resources = resources
        self.role = role or JobNode.MIDDLE
        self.status = JobNode.PENDING
        self.incoming = []
        self.outgoing = []
        self.runner = None

    __str__ = __unicode__ = __repr__ = lambda self: 'JobNode[%s - %s]' % (self.node_id, self.status)

    def add_incoming(self, rel):
        self.incoming.append(rel)

    def add_outgoing(self, rel):
        self.outgoing.append(rel)

    def update_arg(self, path, val):
        self.arguments.update(path, val)

    def get_unresolved_incoming(self):
        return filter(lambda rel: not rel.resolved, self.incoming)

    def get_unresolved_outgoing(self):
        return filter(lambda rel: not rel.resolved, self.outgoing)

    def run(self, runner_map):
        runner_class = self.get_runner_class(runner_map)
        self.runner = runner_class(self.app, self.node_id, self.arguments.data, self.resources, {})
        return self.runner()

    def get_runner_class(self, runner_map):
        runner_class = runner_map.get(self.app.__class__ if self.app else self.role)
        if not runner_class:
            raise TypeError('No runner for app type %s' % self.app.__class__.__name__)
        return runner_class

    def install_tool(self, runner_map):
        runner_class = runner_map.get(self.app.__class__ if self.app else self.role)
        if not runner_class:
            raise TypeError('No runner for app type %s' % self.app.__class__.__name__)
        self.runner = runner_class(self.app, self.node_id, self.arguments.data, self.resources, {})
        self.runner.install_tool()

    @property
    def is_ready(self):
        return not self.get_unresolved_incoming() and self.status == JobNode.PENDING

    @property
    def is_initial(self):
        return self.role == JobNode.INITIAL

    @property
    def is_final(self):
        return self.role == JobNode.FINAL

    def _replace_with(self, node):
        assert len(self.get_unresolved_outgoing()) == 1
        rel = self.get_unresolved_outgoing()[0]
        rel.new_src(node)
        node.add_outgoing(rel)
        self.status = JobNode.DONE

    def add_or_create_input(self, name):
        self.arguments.assure_input_in_arguments(name)

    def resolve(self, result, runner_map):
        assert self.status != JobNode.DONE
        if isinstance(result, JobNode):
            return self._replace_with(result)

        if isinstance(result, Outputs):
            result = result.outputs
        if self.is_final:
            self.result = self.arguments
        else:
            self.result.update(None, result)
        for rel in self.outgoing:
            rel.resolve(runner_map)
        self.status = JobNode.DONE


class JobGraph(object):
    """
    Holds the state of the run. Mutates during the run (and not just node states!).
    See JobNode and JobRelation first.

    Lots of code here that needs to go somewhere else (e.g. a Scheduler class).
    If you're just using this class, see the from_pipeline and simple_run methods.
    """
    def __init__(self, job_prefix=None, runner_map=None):
        self.nodes = {}
        self.job_prefix = job_prefix or rnd_name(5)
        self.runner_map = runner_map or {}

    __str__ = __unicode__ = __repr__ = lambda self: 'JobGraph[%s nodes]' % len(self.nodes)

    def add_node(self, step, app, arguments=None, resources=None, role=JobNode.MIDDLE):
        if not isinstance(arguments, Arguments):
            arguments = Arguments(arguments)
        node_id = self.create_node_id(step, role)
        node = JobNode(node_id, step, app, arguments, resources, role)
        self.nodes[node.node_id] = node
        return node

    def create_node_id(self, step, role):
        base = '.'.join([self.job_prefix, step['id'], role])
        node_id, n = base, 0
        while True:
            if node_id not in self.nodes:
                return node_id
            n += 1
            node_id = '%s.%s' % (base, n)

    def get_node(self, node_id):
        return self.nodes[node_id]

    def get_initial(self, step_id):
        nodes = filter(lambda n: n.step['id'] == step_id, self.nodes.itervalues())
        result = filter(lambda n: n.role in (JobNode.INITIAL, JobNode.OUTPUT, JobNode.INPUT), nodes)
        if not result:
            raise ValueError('No initial JobNode found for step %s' % step_id)
        return result[0]

    def get_final(self, step_id):
        nodes = filter(lambda n: n.step['id'] == step_id, self.nodes.itervalues())
        result = filter(lambda n: n.role in (JobNode.FINAL, JobNode.OUTPUT, JobNode.INPUT), nodes)
        if not result:
            raise ValueError('No final JobNode found for step %s' % step_id)
        return result[0]

    def get_ready_nodes(self):
        return filter(lambda n: n.is_ready, self.nodes.itervalues())

    def resolve_node(self, node, result):
        if isinstance(result, BaseJob):
            replacement = self.add_node(node.step, node.app, result.args, result.resources)
            self.add_prereq_jobs(replacement)
            node.resolve(replacement, self.runner_map)
        else:
            node.resolve(result, self.runner_map)
            ready_finals = filter(lambda n: n.is_final, self.get_ready_nodes())
            if ready_finals:
                self.resolve_node(ready_finals[0], None)

    def add_prereq_jobs(self, node):
        for path, job in node.arguments.iter_jobs():
            src = self.add_node(node.step, node.app, job.args)
            self.connect(src, node, None, path)
            self.add_prereq_jobs(src)

    def connect(self, src_node, dst_node, src_path, dst_path):
        rel = JobRelation(src_node, dst_node, src_path, dst_path)
        src_node.add_outgoing(rel)
        dst_node.add_incoming(rel)
        return rel

    def connect_io(self, src_id, dst_id, inp_id, out_id):
        src_node = self.get_final(src_id)
        dst_node = self.get_initial(dst_id)
        dst_node.add_or_create_input(inp_id)
        return self.connect(src_node, dst_node, [out_id], ['$inputs', inp_id])

    def create_output_node(self, step_id):
        node_id = '.'.join([self.job_prefix, step_id, JobNode.INPUT])
        node = self.nodes.get(node_id)
        if not node:
            return self.add_node({'id': step_id}, None, role=JobNode.OUTPUT)
        if node.role != JobNode.OUTPUT:
            raise ValueError('Node %s already exists and is of type %s' % (node.node_id, node.role))
        return node

    def create_input_node(self, step_id):
        node_id = '.'.join([self.job_prefix, step_id, JobNode.INPUT])
        node = self.nodes.get(node_id)
        if not node:
            return self.add_node({'id': step_id}, None, {'url': None}, role=JobNode.INPUT)
        if node.role != JobNode.INPUT:
            raise ValueError('Node %s already exists and is of type %s' % (node.node_id, node.role))
        return node

    def set_input(self, step_id, url):
        node_id = '.'.join([self.job_prefix, step_id, JobNode.INPUT])
        self.nodes[node_id].arguments.update(['url'], url)

    def get_outputs(self):
        nodes = filter(lambda n: n.role == JobNode.OUTPUT and n.status == JobNode.DONE, self.nodes.itervalues())
        return {n.step['id']: n.result.get(['io']) for n in nodes}

    @classmethod
    def from_pipeline(cls, pipeline, *args, **kwargs):
        """
        Builds an instance of JobGraph from a Pipeline instance.
        *args and **kwargs are forwarded to the constructor.
        """
        to_list = lambda o: o if isinstance(o, list) else [] if o is None else [o]
        args_for_step = lambda s: {'$inputs': {}, '$params': s.get('params', {})}
        graph = cls(*args, **kwargs)
        in_connections, out_connections = {}, {}

        pipeline.validate()

        for step in pipeline.get('steps', []):
            app = pipeline['apps'][step['app']]
            initial = graph.add_node(step, app, args_for_step(step), role=JobNode.INITIAL)
            final = graph.add_node(step, app, role=JobNode.FINAL)
            graph.connect(initial, final, [], [])
            for inp_id, v in step.get('inputs', {}).iteritems():
                in_connections[(step['id'], inp_id)] = to_list(v)
            for out_id, v in step.get('outputs', {}).iteritems():
                out_connections[(step['id'], out_id)] = to_list(v)

        for dst, src_list in in_connections.iteritems():
            dst_id, inp_id = dst
            for src in src_list:
                if '.' in src:
                    src_id, out_id = src.split('.')
                else:
                    src_id = src
                    out_id = 'io'
                    graph.create_input_node(src_id)
                graph.connect_io(src_id, dst_id, inp_id, out_id)

        for src, dst_list in out_connections.iteritems():
            src_id, out_id = src
            for dst in dst_list:
                if '.' in dst:
                    dst_id, inp_id = dst.split('.')
                else:
                    dst_id = dst
                    inp_id = 'io'
                    graph.create_output_node(dst_id)
                graph.connect_io(src_id, dst_id, inp_id, out_id)

        return graph

    def simple_run(self, inputs=None, before_job=None, after_job=None):
        """
        Runs each job one at a time. May take a while. Months, even.

        :param inputs: dict that maps input_id to (list of) files.
        :param before_job: callable that gets a node argument. Called before running the job.
        :param after_job: callable that gets a node argument. Called after job is done.
        """
        for node in self.nodes.itervalues():
            if node.app.__class__ not in self.runner_map and node.role not in self.runner_map:
                raise RuntimeError('Cannot run app: %s' % node.app)
        inputs = inputs or {}
        ready = self.get_ready_nodes()
        for step_id, url in inputs.iteritems():
            self.set_input(step_id, url)
        while ready:
            for node in ready:
                log.debug('Running node %s with %s', node, node.arguments)
                if callable(before_job):
                    before_job(node)
                try:
                    result = node.run(self.runner_map)
                except:
                    node.status = JobNode.FAILED
                    logging.exception('Job failed.')
                    raise RunFailed('Job %s failed (%s).' % (node.step['id'], node))
                log.debug('Result for node %s: %s', node, result)
                self.resolve_node(node, result)
                if callable(after_job):
                    after_job(node)
            ready = self.get_ready_nodes()

    def install_tools(self):
        """
        Goes through a pipeline and installs all referenced tools.
        """
        for node in self.nodes.itervalues():
            if node.app.__class__ not in self.runner_map and node.role not in self.runner_map:
                continue
            node.install_tool(self.runner_map)
