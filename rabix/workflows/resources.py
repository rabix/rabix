import multiprocessing


class ResourceManager(object):

    def __init__(self, cpu, memory, network, ports={}):
        self.total_cpu = cpu
        self.total_memory = memory
        self.network = network
