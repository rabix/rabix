import logging
import six

from nose.tools import nottest, assert_equal

from os.path import abspath, join
from rabix.common.models import Job
from rabix.common.ref_resolver import from_url
from rabix.main import init_context, construct_files

logging.basicConfig(level=logging.DEBUG)


@nottest
def assert_execution(job, outputs):
    result = job.run()
    assert_equal(result, outputs)


def test_workflow():
    path = abspath(join(__file__, '../../test_runtime/wf_tests.yaml'))

    doc = from_url(path)
    tests = doc['tests']
    for test_name, test in six.iteritems(tests):
        context = init_context(test['job'])
        job = Job.from_dict(context, test['job'])

        for inp in job.app.inputs:
            construct_files(job.inputs.get(inp.id), inp.validator)
        yield assert_execution, job, test['outputs']
