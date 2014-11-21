
from nose.tools import nottest, raises

from os.path import abspath, join
from rabix.common.ref_resolver import from_url

from rabix.executors.runner import *


def test_workflow():

    path = abspath(join(__file__, '../../test_runtime/workflow.yml'))

    doc = from_url(path)
    workflow = doc['workflows']['add_one_mul_two']
    job = doc['jobs']['batch_add_one_mul_two']
    print(workflow)
    print(job)
    run(workflow, job)
