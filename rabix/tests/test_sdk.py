import os
import glob
import logging
import shutil

from nose.tools import nottest, with_setup

from rabix.sdk import define, require

log = logging.getLogger(__name__)


@nottest
def remove_test_dir():
    for test_dir in glob.glob('test_*Wrapper_*'):
        shutil.rmtree(test_dir)


@require(100, require.CPU_SINGLE)
class UselessWrapper(define.Wrapper):
    class Inputs(define.Inputs):
        inp = define.input(required=True)

    class Outputs(define.Outputs):
        out = define.output()

    class Params(define.Params):
        p_int = define.integer(default=-1)

    def execute(self):
        self.outputs.out = 'result.txt'
        with open(self.inputs.inp) as fp:
            lines = fp.readlines()
        with open(self.outputs.out, 'w') as fp:
            if self.params.p_int < 0:
                fp.writelines(lines)
            else:
                fp.writelines(lines[:self.params.p_int])


@with_setup(teardown=remove_test_dir)
def test_useless_wrapper():
    inp = os.path.join(os.path.dirname(__file__),
                       'test-files/example_human_reference.fasta')
    w = UselessWrapper({'inp': inp}, {'p_int': 10})
    assert os.path.getsize(w.test().out)
