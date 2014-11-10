import importlib
from rabix.cliche.adapter import from_url
from rabix.workflows import CONFIG


def getRunner(runner, config):
    clspath = config['runners'].get(runner, None)
    if not clspath:
        raise Exception('Runner not specified')
    mod_name, cls_name = clspath.rsplit('.',1)
    try:
        mod = importlib.import_module(mod_name)
    except ImportError:
        raise Exception('Unknown module %s' % mod_name)
    try:
        cls = getattr(mod, cls_name)
    except AttributeError:
        raise Exception('Unknown executor %s' % cls_name)
    return cls


class Node(object):

    def __init__(self, node_id, tool, inputs, config=CONFIG, working_dir='./'):
        self.status = 'WAITING'
        self.node_id = node_id
        self.tool = tool
        self.inputs = inputs
        self.runner_path = tool["requirements"]["environment"][
            "container"]["type"]
        self.executor = getRunner(self.runner_path, config)(
            tool, working_dir=working_dir)
        self.running = []
        self.resources = None

    @property
    def resolved(self):
        return None

    def get_resources(self):
        pass


if __name__=='__main__':
    tool = from_url('../tests/test-cmdline/bwa-mem.json#tool')
    n = Node('test_node', tool, {})
