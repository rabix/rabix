import importlib
import six

from rabix.workflows import CONFIG
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


class Node(object):

    def __init__(self, node_id, tool, job, input_counts, outputs,
                 config=CONFIG, working_dir='./'):
        self.status = 'WAITING'
        self.node_id = node_id
        self.tool = tool
        self.job = job
        self.input_counts = input_counts
        self.outputs = outputs
        self.runner_path = tool.get("@type") or \
            tool["requirements"]["environment"]["container"]["type"]
        self.executor = get_runner(self.runner_path, config)(
            tool, working_dir=working_dir)
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
        pass

