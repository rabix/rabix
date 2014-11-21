import os
from nose.tools import nottest, raises
from rabix.cliche.adapter import from_url
from rabix.executors.cli import TEMPLATE_JOB
from rabix.executors.cli import update_dict
from rabix.executors.cli import get_inputs_from_file

def test_remap_job():
    job = TEMPLATE_JOB
    tool = from_url('bwa-mem.json#tool')
    input_file = from_url('inputs.json')
    startdir = './'
    update_dict(job['inputs'], get_inputs_from_file(tool, input_file, startdir)[
        'inputs'])
    print job

test_remap_job()



