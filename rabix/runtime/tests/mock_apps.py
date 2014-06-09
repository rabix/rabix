import os
import tempfile

from rabix.common.protocol import from_json, Outputs, BaseJob
from rabix.common.util import rnd_name
from rabix.runtime.graph import JobGraph
from rabix.runtime.runners import RUNNER_MAP


def load(path):
    with open(path) as fp:
        return fp.read()


def save(val, job_id):
    result = tempfile.mktemp(dir='.')
    with open(result, 'w') as fp:
        fp.write(unicode(val))
    return os.path.join('..', job_id, result) if job_id else result


def get_inp(d, inp, unpack=False, cast=None):
    val = map(load, d.get('$inputs', {}).get(inp, []))
    if cast:
        val = map(cast, val)
    if unpack:
        val = val[0]
    return val


def generator(job):
    return Outputs({'generated': save(1, job.job_id)})


def incrementor(job):
    args = job.args
    num = get_inp(args, 'to_increment', True, int) or 0
    return Outputs({'incremented': save(num + 1, job.job_id)})


def two_step_increment(job):
    args = job.args
    if args.get('$step', 0) == 0:
        num = get_inp(args, 'to_increment', True, int) or 0
        return BaseJob(args={'the_number': num+1, '$step': 1})
    else:
        num = args.get('the_number')
        return Outputs({'incremented': save(num + 1, job.job_id)})


def test_mock_run():
    prefix = 'x-test-%s' % rnd_name(5)  # Be warned, all dirs with this prefix will be rm -rf on success
    path = os.path.join(os.path.dirname(__file__), 'mock.pipeline.json')
    with open(path) as fp:
        pipeline = from_json(fp, parent_url=path)
    graph = JobGraph.from_pipeline(pipeline, job_prefix=prefix)
    graph.simple_run(RUNNER_MAP, {'initial': 'data:,1'})
    out = graph.get_outputs()['incremented'][0]
    with open(os.path.abspath('not_a_dir/'+out)) as fp:
        result = int(fp.read())
    assert result == 4
    os.system('rm -rf %s.*' % prefix)
