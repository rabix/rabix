from os.path import dirname, join

from rabix.common.ref_resolver import from_url
from rabix.common.models import Job, IO
from rabix.common.context import Context
from rabix.main import TEMPLATE_JOB, dot_update_dict, get_inputs_from_file,\
    fix_types
from rabix.executor import Executor

import rabix.cli
import rabix.docker
import rabix.expressions
import rabix.workflows
import rabix.schema

TYPE_MAP = {
    'Job': Job.from_dict,
    'IO': IO.from_dict
}


def init_context():
    executor = Executor()
    context = Context(TYPE_MAP, executor)

    for module in (
            rabix.cli, rabix.expressions, rabix.workflows,
            rabix.schema, rabix.docker
    ):
        module.init(context)

    return context


def test_remap_job():
    job = TEMPLATE_JOB
    tool = from_url(join(dirname(__file__), 'bwa-mem.json#tool'))
    context = init_context()
    fix_types(tool)
    app = context.from_dict(tool)
    input_file = from_url(join(dirname(__file__), 'inputs.json'))
    startdir = './'
    dot_update_dict(job['inputs'], get_inputs_from_file(app, input_file, startdir)[
        'inputs'])
    print(job)

test_remap_job()
