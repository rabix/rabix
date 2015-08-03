import copy
import six
import logging

from rabix.common.errors import RabixError
from rabix.common.models import Job

log = logging.getLogger(__name__)


class Executor(object):

    def __init__(self):
        pass

    @staticmethod
    def depth(val):
        d = 0
        cur = val
        while isinstance(cur, list):
            d += 1
            if not cur:
                break
            cur = cur[0]

        return d

    @staticmethod
    def split_job(job):
        parallel_input = None
        for input_name, input_val in six.iteritems(job.inputs):
            io = job.app.get_input(input_name)
            val_d = Executor.depth(input_val)
            if val_d == io.depth:
                continue
            if val_d > io.depth + 1:
                raise RabixError("Depth difference to large")
            if val_d < io.depth:
                raise RabixError("Insufficient dimensionality")
            if parallel_input:
                raise RabixError("Already parallelized by input '%s'" % parallel_input)

            parallel_input = input_name

        if parallel_input:
            jobs = []
            for i, val in enumerate(job.inputs[parallel_input]):
                inputs = copy.deepcopy(job.inputs)
                inputs[parallel_input] = val
                jobs.append(Job(job.id+"_"+six.text_type(i), job.app, inputs, {}, job.context))
            return jobs
        else:
            return job

    def execute(self, job, callback=None, callback_id=None):
        # TODO: resources, instances, scheduling, yada yada...
        log.debug('executing job(%s), callback(%s)', job.id, callback_id)
        jobs = self.split_job(job)
        result = None
        if isinstance(jobs, list):
            results = [job.run() for job in jobs]

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
