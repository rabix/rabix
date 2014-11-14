import six

from altgraph.Graph import Graph

from rabix.common.errors import ValidationError


class Workflow(object):

    def __init__(self, steps):
        self.step_graph = Graph()
        self.job_graph = Graph()
        self.inputs = {}
        self.outputs = {}

        for step in steps:
            print("Add node %s" % step["id"])
            self.step_graph.add_node(step["id"],  step["app"])

        for step in steps:
            for inp, val in six.iteritems(step["inputs"]):
                if isinstance(val, list):
                    for item in val:
                        self.add_edge_or_input(step, item)
                else:
                    self.add_edge_or_input(step, val)
            if "outputs" in step:
                for outp, val in six.iteritems(step["outputs"]):
                    self.outputs[val["$to"]] = \
                        step["app"]["outputs"]["properties"][outp]

        for node in self.step_graph.node_list():
            print("Node: %s, %s, %s, %s" % self.step_graph.describe_node(node))

        if not self.step_graph.connected():
            pass
            #raise ValidationError("Graph is not connected")

    def add_edge_or_input(self, step, inp):
        if isinstance(inp, dict) and "$from" in inp:
            if "." in inp["$from"]:
                node, outp = inp["$from"].split(".")
                print("Adding edge: %s(%s) -> %s(%s)" %
                      (node, outp, step["id"], inp))
                self.step_graph.add_edge(node, step["id"], (outp, inp))
            else:
                self.inputs[inp["$from"]] = \
                    step["app"]["inputs"]["properties"][inp]

    def ready_nodes(self):
        pass

    def run_job(self, job):
        pass


if __name__ == '__main__':
    from os.path import abspath, join
    from rabix.common.ref_resolver import from_url

    def root_relative(path):
        return abspath(join(__file__, '../../../', path))

    doc = from_url(root_relative('examples/workflow.yml'))

    wf = Workflow(doc['workflows']['add_one_mul_two']['steps'])
    print(wf.step_graph.forw_topo_sort())
    print(wf.inputs)
    print(wf.outputs)