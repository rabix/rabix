import logging
import six

from nose.tools import nottest, raises, assert_equal

from os.path import abspath, join
from rabix.common.ref_resolver import from_url

from rabix.executors.runner import *

logging.basicConfig(level=logging.DEBUG)

@nottest
def assert_execution(job, outputs):
    workflow = job['app']

    result = run(workflow, job)
    assert_equal(result, outputs)


def test_workflow():
    path = abspath(join(__file__, '../../test_runtime/wf_tests.yaml'))

    doc = from_url(path)
    tests = doc['tests']
    print(tests)
    for test_name, test in six.iteritems(tests):
        if not test.get('requiresFeatures'):
            print("Running test: " + test_name)
            assert_execution(test['job'], test['outputs'])
