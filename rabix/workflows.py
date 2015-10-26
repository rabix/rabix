import six
import logging

from copy import deepcopy
from collections import namedtuple, defaultdict
from altgraph.Graph import Graph

from rabix.common.errors import ValidationError, RabixError
from rabix.common.util import wrap_in_list
from rabix.common.models import (
    Process, Parameter, Job, InputParameter, OutputParameter,
    process_builder, parameter_name
)

log = logging.getLogger(__name__)

AppNode = namedtuple('AppNode', ['app', 'inputs'])

Relation = namedtuple('Relation', ['source', 'destination', 'position'])
InputRelation = namedtuple('InputRelation', ['destination', 'position'])
OutputRelation = namedtuple('OutputRelation', ['source', 'position'])

Relation.to_dict = Relation._asdict


class WorkflowStepInput(InputParameter):

    def __init__(self, id, validator=None, required=False, label=None,
                 description=None, depth=0, input_binding=None, source=None,
                 value=None):
        super(WorkflowStepInput, self).__init__(
            id, validator, required, label, description, depth, input_binding
        )

        self.source = wrap_in_list(source) if source is not None else []
        self.value = value

    def to_dict(self, ctx=None):
        d = super(WorkflowStepInput, self).to_dict(ctx)
        d['source'] = ctx.to_primitive(self.source)
        d['value'] = self.value
        return d

    @classmethod
    def from_dict(cls, context, d):
        instance = super(WorkflowStepInput, cls).from_dict(context, d)
        source = d.get('source')
        instance.source = wrap_in_list(source) if source is not None else []
        instance.value = d.get('default')
        return instance


class Step(Process):

    def __init__(
            self, process_id, inputs, outputs, requirements, hints,
            label, description, app, scatter
    ):
        super(Step, self).__init__(
            process_id, inputs, outputs,
            requirements=requirements,
            hints=hints,
            label=label,
            description=description
        )
        self.app = app
        self.scatter = scatter

    def to_dict(self, context):
        d = super(Step, self).to_dict(context)
        d['run'] = context.to_primitive(self.app)
        return d

    def run(self, job):
        self.app.run(job)

    @staticmethod
    def infer_step_id(d):
        step_id = d.get('id')
        app_id = d['run'].get('id')

        if app_id and step_id:
            return

        if not step_id:
            inp = d['inputs'][0]
            step_id = inp['id'].split('.')[0]
            d['id'] = step_id

        if not app_id:
            d['run']['id'] = step_id + '_process'

    @classmethod
    def from_dict(cls, context, d):
        cls.infer_step_id(d)
        converted = {
            k: process_builder(context, v) if k == 'run' else context.from_dict(v)
            for k, v in six.iteritems(d)
        }
        kwargs = Process.kwarg_dict(converted)
        kwargs.update({
            'app': converted['run'],
            'inputs': [WorkflowStepInput.from_dict(context, inp)
                       for inp in converted.get('inputs', [])],
            'outputs': [OutputParameter.from_dict(context, inp)
                        for inp in converted.get('outputs', [])],
            'scatter': converted.get('scatter')
        })
        return cls(**kwargs)


class WorkflowOutput(OutputParameter):

    def __init__(self, id, validator=None, required=False, label=None,
                 description=None, depth=0, output_binding=None, source=None):
        super(WorkflowOutput, self).__init__(
            id, validator, required, label, description, depth, output_binding
        )
        self.source = wrap_in_list(source) if source is not None else []

    def to_dict(self, ctx=None):
        d = super(WorkflowOutput, self).to_dict(ctx)
        d['source'] = ctx.to_primitive(self.source)
        return d

    @classmethod
    def from_dict(cls, context, d):
        instance = super(OutputParameter, cls).from_dict(context, d)
        source = d.get('source')
        instance.source = wrap_in_list(source) if source is not None else []
        return instance


class Workflow(Process):

    def __init__(self, process_id, inputs, outputs, requirements, hints, label,
                 description, steps, context, data_links=None):
        super(Workflow, self).__init__(
            process_id, inputs, outputs, requirements,
            hints, label, description
        )
        self.graph = Graph()
        self.executor = context.executor
        self.steps = steps
        self.data_links = data_links or []
        self.context = context
        self.port_step_index = {}

        for step in steps:
            node = AppNode(step.app, {})
            self.add_node(step.id, node)
            for inp in step.inputs:
                self.port_step_index[inp.id] = step.id
                self.move_connect_to_datalink(inp)
                if inp.value:
                    node.inputs[inp.id] = inp.value

            for out in step.outputs:
                self.port_step_index[out.id] = step.id

        for inp in self.inputs:
            self.add_node(inp.id, inp)

        for out in self.outputs:
            self.move_connect_to_datalink(out)
            self.add_node(out.id, out)

        # dedupe links
        s = {tuple(dl.items()) for dl in self.data_links}
        self.data_links = [dict(dl) for dl in s]

        for dl in self.data_links:
            dst = dl['destination'].lstrip('#')
            src = dl['source'].lstrip('#')

            if src in self.port_step_index and dst in self.port_step_index:
                rel = Relation(src, dst, dl.get('position', 0))
                src = self.port_step_index[src]
                dst = self.port_step_index[dst]
            elif src in self._inputs:
                rel = InputRelation(dst, dl.get('position', 0))
                dst = self.port_step_index[dst]
            elif dst in self._outputs:
                rel = OutputRelation(src, dl.get('position', 0))
                src = self.port_step_index[src]
            else:
                raise RabixError("invalid data link %s" % dl)

            self.graph.add_edge(src, dst, rel)

        if not self.graph.connected():
            pass
            # raise ValidationError('Graph is not connected')

    def move_connect_to_datalink(self, port):
        for src in port.source:
            self.data_links.append({'source': src, 'destination': '#'+port.id})
        del port.source[:]

    # Graph.add_node silently fails if node already exists
    def add_node(self, node_id, node):
        if node_id in self.graph.nodes:
            raise ValidationError('Duplicate node ID: %s' % node_id)
        self.graph.add_node(node_id, node)

    def run(self, job):
        eg = ExecutionGraph(self, job)
        while eg.has_next():
            next_id, next = eg.next_job()
            self.executor.execute(next, eg.job_done, next_id)
        return eg.outputs

    def to_dict(self, context):
        d = super(Workflow, self).to_dict(context)
        d.update({
            "class": "Workflow",
            'steps': [step.to_dict(context) for step in self.steps]
        })
        return d

    @classmethod
    def from_dict(cls, context, d):
        converted = {}
        for k, v in six.iteritems(d):
            if k == 'steps':
                converted[k] = [Step.from_dict(context, s) for s in v]
            else:
                converted[k] = context.from_dict(v)

        kwargs = Process.kwarg_dict(converted)
        kwargs.update({
            'steps': converted['steps'],
            'data_links': converted.get('dataLinks'),
            'context': context,
            'inputs': [InputParameter.from_dict(context, i)
                       for i in converted['inputs']],
            'outputs': [WorkflowOutput.from_dict(context, o)
                        for o in converted['outputs']]
        })
        return cls(**kwargs)


def init(context):
    context.add_type('Workflow', Workflow.from_dict)


class PartialJob(object):

    def __init__(self, node_id, app, inputs, input_counts, outputs, context):
        self.result = None
        self.status = 'WAITING'
        self.node_id = node_id
        self.app = app
        self.inputs = inputs
        self.input_counts = input_counts
        self.outputs = {parameter_name(k): v for k, v in six.iteritems(outputs)}
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
        self.result = result
        for k, v in six.iteritems(result):
            if self.outputs.get(k):
                for out in self.outputs[k]:
                    out.resolve_input(v)

    def job(self):
        return Job(None, self.app, self.inputs, {}, self.context)


class ExecRelation(object):

    def __init__(self, node, input_port):
        self.node = node
        self.input_port = input_port

    def resolve_input(self, result):
        self.node.resolve_input(self.input_port, result)


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

        for node_id in workflow.graph.back_topo_sort()[1]:
            executable = self.make_executable(node_id)
            if executable:
                self.executables[node_id] = executable

        self.order = self.calc_order()

    def calc_order(self):
        params = []
        for node_id in self.graph.node_list():
            node = self.graph.node_data(node_id)
            if isinstance(node, Parameter):
                params.append(node_id)

        for p in params:
            self.graph.hide_node(p)

        order = self.graph.back_topo_sort()[1]

        for p in params:
            self.graph.restore_node(p)

        return order

    def add_output(self, outputs, port, relation):
        if not outputs.get(port):
            outputs[port] = [relation]
        else:
            outputs[port].append(relation)

    def make_executable(self, node_id):
        node = self.graph.node_data(node_id)
        if isinstance(node, Parameter):
            return None

        out_edges = self.graph.out_edges(node_id)
        in_edges = self.graph.inc_edges(node_id)

        outputs = {}
        input_counts = ExecutionGraph.count_inputs(self.graph, in_edges)
        for out_edge in out_edges:
            rel = self.graph.edge_data(out_edge)
            if isinstance(rel, Relation):
                tail = self.executables[self.graph.tail(out_edge)]
                self.add_output(outputs, rel.source, ExecRelation(
                    tail, rel.destination))
            elif isinstance(rel, OutputRelation):
                tail = self.graph.tail(out_edge)
                self.add_output(outputs, rel.source, OutRelation(
                    self, tail))

        executable = PartialJob(
            node_id, node.app, deepcopy(node.inputs),
            input_counts, outputs, self.workflow.context
        )

        for in_edge in in_edges:
            rel = self.graph.edge_data(in_edge)
            head = self.graph.head(in_edge)
            if (isinstance(rel, InputRelation) and
                    head in self.job.inputs):
                executable.resolve_input(
                    rel.destination, self.job.inputs[head]
                )

        return executable

    @staticmethod
    def count_inputs(graph, in_edges):
        input_counts = defaultdict(lambda: 0)
        for edge in in_edges:
            relation = graph.edge_data(edge)
            input_count = input_counts[relation.destination]
            input_counts[relation.destination] = input_count + 1
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

    wf = Workflow(doc['workflows']['add_one_mul_two']['steps'])
    print(wf.graph.forw_topo_sort())

    for edge in wf.graph.edges:
        print(wf.graph.describe_edge(edge))

    print(wf.inputs)
    print(wf.outputs)
