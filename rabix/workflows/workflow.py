import six

from altgraph.Graph import Graph

from rabix.common.errors import ValidationError


class Workflow(object):

    def __init__(self, steps):
        self.graph = Graph()
        self.inputs = {}
        self.outputs = {}

        for step in steps:
            print("Add node %s" % step["id"])
            self.graph.add_node(step["id"],  step["app"])

        for step in steps:
            for inp, val in six.iteritems(step["inputs"]):
                if isinstance(val, dict) and "$from" in val:
                    if "." in val["$from"]:
                        node, outp = val["$from"].split(".")
                        print("Adding edge: %s(%s) -> %s(%s)" %
                              (node, outp, step["id"], inp))
                        self.graph.add_edge(node, step["id"], (outp, inp))
                    else:
                        self.inputs[val["$from"]] = \
                            step["app"]["inputs"]["properties"][inp]
            if "outputs" in step:
                for outp, val in six.iteritems(step["outputs"]):
                    self.outputs[val["$to"]] = \
                        step["app"]["outputs"]["properties"][outp]

        for node in self.graph.node_list():
            print("Node: %s, %s, %s, %s" % self.graph.describe_node(node))

        if not self.graph.connected():
            pass
            #raise ValidationError("Graph is not connected")

    def outputs(self, job_id, outputs):
        pass

    def ready_nodes(self):
        pass


if __name__ == '__main__':
    from os.path import abspath, join
    from rabix.common.ref_resolver import from_url

    def root_relative(path):
        return abspath(join(__file__, '../../../', path))

    doc = from_url(root_relative('examples/workflow.yml'))

    wf = Workflow(doc['workflows']['add_one_mul_two']['steps'])
    print(wf.graph.forw_topo_sort())
    print(wf.inputs)
    print(wf.outputs)