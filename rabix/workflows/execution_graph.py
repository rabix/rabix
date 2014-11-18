import importlib
import six

from collections import defaultdict

from rabix.workflows import CONFIG
from rabix.workflows.workflow import InputNode, OutputNode, InputRelation
from rabix.common.errors import RabixError


def get_runner(runner, config):
    clspath = config['runners'].get(runner, None)
    if not clspath:
        raise Exception('Runner not specified')
    mod_name, cls_name = clspath.rsplit('.', 1)
    try:
        mod = importlib.import_module(mod_name)
    except ImportError:
        raise Exception('Unknown module %s' % mod_name)
    try:
        cls = getattr(mod, cls_name)
    except AttributeError:
        raise Exception('Unknown executor %s' % cls_name)
    return cls


# FIXME: should two scalars be merged into a list? Scalar be added to a list?
# Does that depend on the schema? Should type mismatches be an error?
def recursive_merge(dst, src):
    for k, v in six.iteritems(src):
        dst_val = dst.get(k)
        if isinstance(v, dict):
            if isinstance(dst_val, dict):
                dst[k] = recursive_merge(dst_val, v)
            else:
                dst[k] = v
        elif isinstance(v, list):
            if isinstance(dst_val, list):
                dst[k] = dst_val + v
            else:
                dst[k] = v
    return dst


class Executable(object):

    def __init__(self, node_id, tool, job, input_counts, outputs,
                 config=CONFIG, working_dir='./'):
        self.status = 'WAITING'
        self.node_id = node_id
        self.tool = tool
        self.job = job
        self.input_counts = input_counts
        self.outputs = outputs

        self.runner_path = (
            tool.get("@type") or
            tool["requirements"]["environment"]["container"]["type"]
        )

        self.executor = get_runner(self.runner_path, config)(
            tool, working_dir=working_dir
        )
        self.running = []
        self.resources = None

    @property
    def resolved(self):
        for cnt in self.input_counts:
            if cnt > 0:
                return False
        return True

    def resolve_input(self, input_port, results):
        input_count = self.input_counts[input_port]
        if input_count <= 0:
            raise RabixError("Input already satisfied")
        self.input_counts[input_port] = input_count - 1
        recursive_merge(self.job["inputs"][input_port], results)
        return self.resolved

    def propagate_result(self, result):
        for k, v in six.iteritems(result):
            self.outputs[k].resolve_input(v)


class Relation(object):

    def __init__(self, node, input_port):
        self.node = node
        self.input_port = input_port

    def resolve_input(self, result):
        self.node.resolve_input(self.input_port, result)


class ExecutionGraph(object):

    def __init__(self, workflow, job):
        self.workflow = workflow
        self.executables = {}
        self.ready = {}
        self.job = job

        graph = workflow.graph

        self.hide_outputs()

        for node_id in graph.back_topo_sort()[1]:
            executable = self.make_executable(node_id)
            if executable:
                self.executables[node_id] = executable

    def make_executable(self, node_id):
        node = self.graph.node_data(node_id)
        if isinstance(node, InputNode):
            return None

        out_edges = self.graph.out_edges(node_id)
        in_edges = self.graph.inc_edges(node_id)

        outputs = {}
        input_counts = ExecutionGraph.count_inputs(self.graph, in_edges)
        for out_edge in out_edges:
            tail = self.executables[self.graph.tail(out_edge)]
            ports = self.graph.edge_data(out_edge)
            outputs[ports.src_port] = Relation(tail, ports.dst_port)

        executable = Executable(
            node_id, node.app, node.inputs, input_counts, out_edges
        )

        for in_edge in in_edges:
            rel = self.graph.edge_data(in_edge)
            head = self.graph.head(in_edge)
            if (isinstance(in_edge, InputRelation) and
                        head in self.job['inputs']):

                executable.resolve_input(
                    rel.dst_port, self.job['inputs']['head']
                )

        return executable

    def hide_outputs(self):
        for node_id in self.graph.node_list():
            node = self.graph.node_data(node_id)
            if isinstance(node, OutputNode):
                self.graph.hide_node(node_id)

    @staticmethod
    def count_inputs(graph, in_edges):
        input_counts = defaultdict(lambda: 0)
        for edge in in_edges:
            relation = graph.edge_data(edge)
            print(relation)
            input_count = input_counts[relation.dst_port]
            input_counts[relation.dst_port] = input_count + 1
        return input_counts

    def job_done(self, node_id, results):
        pass

    @property
    def graph(self):
        return self.workflow.graph


# Smoke test
if __name__ == '__main__':
    from os.path import abspath, join
    from rabix.common.ref_resolver import from_url
    from rabix.workflows.workflow import Workflow

    def root_relative(path):
        return abspath(join(__file__, '../../../', path))

    doc = from_url(root_relative('examples/workflow.yml'))

    wf = Workflow(doc['workflows']['add_one_mul_two']['steps'])
    job = doc['jobs']['batch_add_one_mul_two']

    eg = ExecutionGraph(wf, job)
    print(eg.executables)
