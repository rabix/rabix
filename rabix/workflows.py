import six
import logging
import copy

from uuid import uuid4
from collections import namedtuple, defaultdict
from altgraph.Graph import Graph

from rabix.common.errors import ValidationError, RabixError
from rabix.common.util import wrap_in_list, map_rec_list
from rabix.common.models import App, IO, Job
from rabix.schema import JsonSchema

log = logging.getLogger(__name__)

AppNode = namedtuple('AppNode', ['app', 'inputs'])

Relation = namedtuple('Relation', ['src_port', 'dst_port'])
InputRelation = namedtuple('InputRelation', ['dst_port'])
OutputRelation = namedtuple('OutputRelation', ['src_port'])


class Step(object):

    def __init__(self, step_id, app, inputs, outputs=None):
        self.id = step_id
        self.app = app
        self.inputs = inputs
        self.outputs = outputs

    def to_dict(self, context):
        return {
            "id": self.id,
            "@type": "Step",
            "app": self.app.to_dict(context),
            "inputs": context.to_dict(self.inputs),
            "outputs": context.to_dict(self.outputs)
        }

    @classmethod
    def from_dict(cls, context, d):
        return cls(
            d["id"],
            context.from_dict(d['app']),
            context.from_dict(d.get('inputs')),
            context.from_dict(d.get('outputs')))


class WorkflowApp(App):

    def __init__(self, app_id, steps, context,
                 inputs=None, outputs=None, to=None,
                 app_description=None,
                 annotations=None,
                 platform_features=None):
        self.graph = Graph()
        self.inputs = inputs or []
        self.outputs = outputs or []
        self.executor = context.executor
        self.steps = steps
        self.to = to or {}
        self.context = context

        for step in steps:
            self.add_node(step.id,  AppNode(step.app, {}))

        for step in steps:
            # inputs
            for input_port, input_val in six.iteritems(step.inputs):
                inp = wrap_in_list(input_val)
                for item in inp:
                    self.add_edge_or_input(step, input_port, item)

            # outputs
            if step.outputs:
                for output_port, output_val in six.iteritems(step.outputs):
                    self.to[output_val['$to']] = output_port
                    if isinstance(step.app, WorkflowApp):
                        output_node = step.app.get_output(step.app.to.get(output_port))
                    else:
                        output_node = step.app.get_output(output_port)
                    output_id = output_val['$to']
                    self.add_node(output_id, output_node)
                    self.graph.add_edge(
                        step.id, output_id, OutputRelation(output_port)
                    )
                    # output_node.id = output_val['$to']
                    self.outputs.append(output_node)

        if not self.graph.connected():
            pass
            # raise ValidationError('Graph is not connected')

        schema = {
            "@type": "JsonSchema",
            "type": "object",
            "properties": {},
            "required": []
        }

        for inp in self.inputs:
            schema['properties'][inp.id] = inp.validator.schema
            if inp.required:
                schema['required'].append(inp.id)

        super(WorkflowApp, self).__init__(
            app_id, JsonSchema(context, schema), self.outputs,
            app_description=app_description,
            annotations=annotations,
            platform_features=platform_features
        )

    def add_edge_or_input(self, step, input_name, input_val):
        node_id = step.id
        if isinstance(input_val, dict) and '$from' in input_val:
            frm = wrap_in_list(input_val['$from'])
            for inp in frm:
                if '.' in inp:
                    node, outp = inp.split('.')
                    self.graph.add_edge(node, node_id, Relation(outp, input_name))
                else:
                    # TODO: merge input schemas if one input goes to different apps

                    input = step.app.get_input(input_name)
                    if inp not in self.graph.nodes:
                        self.add_node(inp, input)
                    self.graph.add_edge(
                        inp, node_id, InputRelation(input_name)
                    )
                    wf_input = copy.deepcopy(input)
                    wf_input.id = inp
                    self.inputs.append(wf_input)

        else:
            self.graph.node_data(node_id).inputs[input_name] = input_val

    # Graph.add_node silently fails if node already exists
    def add_node(self, node_id, node):
        if node_id in self.graph.nodes:
            raise ValidationError('Duplicate node ID: %s' % node_id)
        self.graph.add_node(node_id, node)

    def hide_nodes(self, type):
        for node_id in self.graph.node_list():
            node = self.graph.node_data(node_id)
            if isinstance(node, type):
                self.graph.hide_node(node_id)

    def run(self, job):
        eg = ExecutionGraph(self, job)
        while eg.has_next():
            next_id, next = eg.next_job()
            self.executor.execute(next, eg.job_done, next_id)
        return eg.outputs

    def to_dict(self, context):
        d = super(WorkflowApp, self).to_dict(context)
        d.update({
            "@type": "Workflow",
            'steps': [step.to_dict(context) for step in self.steps]
        })
        return d

    @classmethod
    def from_dict(cls, context, d):
        steps = [Step(
            step['id'], context.from_dict(step['app']),
            step['inputs'], step.get('outputs')
        )
            for step in d['steps']]

        return cls(
            d.get('@id', six.text_type(uuid4())),
            steps,
            context
        )


def init(context):
    context.add_type('Workflow', WorkflowApp.from_dict)


class PartialJob(object):

    def __init__(self, node_id, app, inputs, input_counts, outputs, context):
        self.result = None
        self.status = 'WAITING'
        self.node_id = node_id
        self.app = app
        self.inputs = inputs
        self.input_counts = input_counts
        self.outputs = outputs
        self.context = context

        self.running = []
        self.resources = None

    @property
    def resolved(self):
        for name, cnt in six.iteritems(self.input_counts):
            if cnt > 0:
                return False
        return True

    def resolve_input(self, input_port, results):
        print("Resolving input '%s' with value %s" % (input_port, results))
        log.debug("Resolving input '%s' with value %s" % (input_port, results))
        input_count = self.input_counts[input_port]
        if input_count <= 0:
            raise RabixError("Input already satisfied")
        self.input_counts[input_port] = input_count - 1

        prev_result = self.inputs.get(input_port)
        if prev_result is None:
            self.inputs[input_port] = results
        elif isinstance(prev_result, list):
            prev_result.append(results)
        else:
            self.inputs[input_port] = [prev_result, results]
        return self.resolved

    def propagate_result(self, result):
        log.debug("Propagating result: %s" % result)
        self.result = result
        for k, v in six.iteritems(result):
            log.debug("Propagating result: %s, %s" % (k, v))
            for out in self.outputs[k]:
                out.resolve_input(v)

    def job(self):
        return Job(None, self.app, self.inputs, {}, self.context)


class ExecRelation(object):

    def __init__(self, node, input_port):
        self.node = node
        self.input_port = input_port

    def resolve_input(self, result):
        io = self.node.app.get_input(self.input_port)
        res = map_rec_list(io.constructor, result)
        self.node.resolve_input(self.input_port, res)


class OutRelation(object):

    def __init__(self, graph, name):
        self.name = name
        self.graph = graph

    def resolve_input(self, result):
        self.graph.outputs[self.name] = result


class ExecutionGraph(object):

    def __init__(self, workflow, job):
        self.workflow = workflow
        self.executables = {}
        self.ready = {}
        self.job = job
        self.outputs = {}

        graph = workflow.graph

        for node_id in graph.back_topo_sort()[1]:
            executable = self.make_executable(node_id)
            if executable:
                self.executables[node_id] = executable

        workflow.hide_nodes(IO)

        self.order = graph.back_topo_sort()[1]

    def add_output(self, outputs, port, relation):
        if not outputs.get(port):
            outputs[port] = [relation]
        else:
            outputs[port].append(relation)

    def make_executable(self, node_id):
        node = self.graph.node_data(node_id)
        if isinstance(node, IO):
            return None

        out_edges = self.graph.out_edges(node_id)
        in_edges = self.graph.inc_edges(node_id)

        outputs = {}
        input_counts = ExecutionGraph.count_inputs(self.graph, in_edges)
        for out_edge in out_edges:
            rel = self.graph.edge_data(out_edge)
            if isinstance(rel, Relation):
                tail = self.executables[self.graph.tail(out_edge)]
                self.add_output(outputs, rel.src_port, ExecRelation(
                    tail, rel.dst_port))
            elif isinstance(rel, OutputRelation):
                tail = self.graph.tail(out_edge)
                self.add_output(outputs, rel.src_port, OutRelation(
                    self, tail))

        executable = PartialJob(
            node_id, node.app, node.inputs,
            input_counts, outputs, self.workflow.context
        )

        for in_edge in in_edges:
            rel = self.graph.edge_data(in_edge)
            head = self.graph.head(in_edge)
            if (isinstance(rel, InputRelation) and
                    head in self.job.inputs):
                executable.resolve_input(
                    rel.dst_port, self.job.inputs[head]
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
        next = self.order.pop()
        return next, self.executables[next].job()

    def has_next(self):
        return len(self.order) > 0

    @property
    def graph(self):
        return self.workflow.graph


# Smoke test
if __name__ == '__main__':
    from os.path import abspath, join
    from rabix.common.ref_resolver import from_url

    def root_relative(path):
        p = abspath(join(__file__, '../', path))
        return abspath(join(__file__, '../../../', path))

    doc = from_url(root_relative('tests/workflow.yml'))

    wf = WorkflowApp(doc['workflows']['add_one_mul_two']['steps'])
    print(wf.graph.forw_topo_sort())

    for edge in wf.graph.edges:
        print(wf.graph.describe_edge(edge))

    print(wf.inputs)
    print(wf.outputs)
