import six

from collections import defaultdict

from rabix.workflows.workflow import InputNode, OutputNode, InputRelation
from rabix.common.errors import RabixError


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

    def __init__(self, node_id, tool, inputs, input_counts, outputs):
        self.result = None
        self.status = 'WAITING'
        self.node_id = node_id
        self.tool = tool
        self.job = {'inputs': inputs}
        self.input_counts = input_counts
        self.outputs = outputs

        self.running = []
        self.resources = None

    @property
    def resolved(self):
        for name, cnt in six.iteritems(self.input_counts):
            if cnt > 0:
                return False
        return True

    def resolve_input(self, input_port, results):
        input_count = self.input_counts[input_port]
        if input_count <= 0:
            raise RabixError("Input already satisfied")
        self.input_counts[input_port] = input_count - 1
        # recursive_merge(self.job['inputs'].get(input_port), results)
        self.job['inputs'][input_port] = results
        return self.resolved

    def propagate_result(self, result):
        self.result = result
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

        workflow.hide_nodes(OutputNode)

        for node_id in graph.back_topo_sort()[1]:
            executable = self.make_executable(node_id)
            if executable:
                self.executables[node_id] = executable

        workflow.hide_nodes(InputNode)

        self.order = graph.back_topo_sort()[1]

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
            if (isinstance(rel, InputRelation) and
                    head in self.job['inputs']):

                executable.resolve_input(
                    rel.dst_port, self.job['inputs'][head]
                )

        return executable

    @staticmethod
    def count_inputs(graph, in_edges):
        input_counts = defaultdict(lambda: 0)
        for edge in in_edges:
            relation = graph.edge_data(edge)
            input_count = input_counts[relation.dst_port]
            input_counts[relation.dst_port] = input_count + 1
        return input_counts

    def job_done(self, node_id, results):
        ex = self.executables[node_id]
        ex.propagate_result(results)

    def next_job(self):
        if not self.order:
            return None
        return self.executables[self.order.pop()]

    def has_next(self):
        return len(self.order) > 0

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
