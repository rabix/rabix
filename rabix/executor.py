from rabix.common.util import wrap_in_list


class Executor(object):

    def __init__(self):
        pass

    def execute(self, jobs, callback=None):
        # TODO: resources, instances, scheduling, yada yada...
        result = None
        if isinstance(jobs, list):
            results = []
            for job in wrap_in_list(jobs):
                results.append(job.run())

            combined = {}
            for result in results:
                for k, v in result:
                    acc = combined.get(k, [])
                    acc.append(v)
                    combined[k] = v
            result = combined
        else:
            result = jobs.run()

        if callback:
            callback(jobs.id, result)
