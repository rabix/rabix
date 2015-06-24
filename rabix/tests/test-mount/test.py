from os.path import dirname, join

from rabix.common.ref_resolver import from_url
from rabix.main import TEMPLATE_JOB, init_context


def test_remap_job():
    job = TEMPLATE_JOB
    tool = from_url(join(dirname(__file__), 'bwa-mem.json#tool'))
    context = init_context(tool)
    app = context.from_dict(tool)
    input_file = from_url(join(dirname(__file__), 'inputs.json'))
    startdir = './'
    # dot_update_dict(job['inputs'], get_inputs_from_file(app, input_file, startdir)[
    #    'inputs'])
    # print(job)

test_remap_job()
