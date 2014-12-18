import six
from rabix.common.util import wrap_in_list


class Executor(object):

    def __init__(self):
        pass

    def execute(self, jobs, callback=None, callback_id=None):
        # TODO: resources, instances, scheduling, yada yada...
        result = None
        if isinstance(jobs, list):
            results = []
            for job in wrap_in_list(jobs):
                results.append(job.run())

            combined = {}
            for result in results:
                for k, v in six.iteritems(result):
                    acc = combined.get(k, [])
                    acc.append(v)
                    combined[k] = acc
            result = combined
        else:
            result = jobs.run()

        if callback:
            callback(callback_id, result)
