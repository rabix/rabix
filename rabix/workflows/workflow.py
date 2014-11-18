import six

from collections import namedtuple
from altgraph.Graph import Graph

from rabix.common.errors import ValidationError
from rabix.common.util import wrap_in_list


AppNode = namedtuple('AppNode', ['app', 'inputs'])
InputNode = namedtuple('InputNode', ['schema'])
OutputNode = namedtuple('OutputNode', ['schema'])

Relation = namedtuple('Relation', ['src_port', 'dst_port'])
InputRelation = namedtuple('InputRelation', ['dst_port'])
OutputRelation = namedtuple('OutputRelation', ['src_port'])


class Workflow(object):

    def __init__(self, steps):
        self.graph = Graph()
        self.inputs = {
            "type": "object",
            "properties": {},
            "required": []
        }
        self.outputs = {
            "type": "object",
            "properties": {}
        }

        for step in steps:
            node_id = step['id']
            print('Adding node %s' % node_id)
            self.add_node(node_id,  AppNode(step['app'], {}))

        for step in steps:
            # inputs
            for input_port, input_val in six.iteritems(step['inputs']):
                inputs = wrap_in_list(input_val)
                for item in inputs:
                    self.add_edge_or_input(step, input_port, item)

            # outputs
            if 'outputs' in step:
                for output_port, input_val in six.iteritems(step['outputs']):
                    output_schema = step['app']['outputs']['properties'][output_port]
                    output_node = OutputNode(output_schema)
                    output_id = input_val['$to']
                    self.add_node(output_id, output_node)
                    self.graph.add_edge(
                        step['id'], output_id, OutputRelation(output_port)
                    )
                    self.outputs['properties'][output_id] = output_schema

        if not self.graph.connected():
            pass
            #raise ValidationError('Graph is not connected')

    def add_edge_or_input(self, step, input_name, input_val):
        node_id = step['id']
        if isinstance(input_val, dict) and '$from' in input_val:
            if '.' in input_val['$from']:
                node, outp = input_val['$from'].split('.')
                print('Adding edge: %s(%s) -> %s(%s)' %
                      (node, outp, node_id, input_name))
                self.graph.add_edge(node, node_id, Relation(outp, input_name))
            else:
                wf_input = input_val['$from']

                # TODO: merge input schemas if one input goes to different apps
                input_schema = step['app']['inputs']['properties'][input_name]
                io_node = InputNode(input_schema)

                self.inputs['properties'][wf_input] = input_schema
                required = step['app']['inputs'].get('required', [])
                if wf_input in required:
                    self.inputs['required'].appennd(wf_input)

                self.add_node(wf_input, io_node)
                self.graph.add_edge(
                    wf_input, node_id, InputRelation(input_name)
                )

        else:
            self.graph.node_data(node_id).inputs[input_name] = input_val

    # Graph.add_node silently fails if node already exists
    def add_node(self, node_id, node):
        if node_id in self.graph.nodes:
            raise ValidationError('Duplicate node ID: %s' % node_id)
        self.graph.add_node(node_id, node)


# Smoke test
if __name__ == '__main__':
    from os.path import abspath, join
    from rabix.common.ref_resolver import from_url

    def root_relative(path):
        return abspath(join(__file__, '../../../', path))

    doc = from_url(root_relative('examples/workflow.yml'))

    wf = Workflow(doc['workflows']['add_one_mul_two']['steps'])
    wf.graph.hide_node('a')
    wf.graph.hide_node('result')
    print(wf.graph.forw_topo_sort())

    for edge in wf.graph.edges:
        print(wf.graph.describe_edge(edge))

    print(wf.inputs)
    print(wf.outputs)