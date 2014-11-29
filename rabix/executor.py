from rabix.common.util import wrap_in_list


class Executor(object):

    def __init__(self):
        pass

    def execute(self, jobs, callback=None):
        # TODO: resources, instances, parallelization, scheduling, yada yada...
        for job in wrap_in_list(jobs):
            result = job.run()
            if callback:
                callback(job.id, result)
