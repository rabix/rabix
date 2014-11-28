from __future__ import print_function
import sys

from rabix.common.models import Job, IO
from rabix.common.context import Context
from rabix.common.ref_resolver import from_url
import rabix.cliche.cli_app as cli_app
import rabix.docker.docker_app as docker_app
import rabix.expressions.script_app as script_app
import rabix.workflows as workflow_app
import rabix.schema as schema
from rabix.executor import Executor

TYPE_MAP = {
    'Job': Job.from_dict,
    'IO': IO.from_dict
}


def init_context():
    executor = Executor()
    context = Context(TYPE_MAP, executor)

    for module in cli_app, script_app, workflow_app, schema, docker_app:
        module.init(context)

    return context


if __name__ == '__main__':
    data = from_url(sys.argv[1])
    print(data)

    context = init_context()
    job = context.from_dict(data)
    context.executor.execute(job, print)
