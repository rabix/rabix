#!/usr/bin/env python

import os
import json
import argparse


from rabix.cli.adapter import CLIJob
from rabix.common.ref_resolver import from_url
from rabix.common.models import Job, IO
from rabix.common.context import Context

import rabix.cli
import rabix.docker
import rabix.expressions
import rabix.workflows
import rabix.schema

TYPE_MAP = {
    'Job': Job.from_dict,
    'IO': IO.from_dict
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("tool", type=str)
    parser.add_argument("job_order", type=str)
    parser.add_argument("--conformance-test", action="store_true")
    parser.add_argument("--basedir", type=str)
    parser.add_argument("--no-container", action="store_true")
    args = parser.parse_args()

    app_dict = from_url(args.tool)
    app_dict['@type'] = 'CommandLine'

    inputs = app_dict.get('inputs')
    if isinstance(inputs, dict) and '@type' not in inputs:
        inputs['@type'] = 'JsonSchema'

    outputs = app_dict.get('outputs')

    if not outputs:
        outputs = {'type': 'object', 'properties': {}, '@type': 'JsonSchema'}
        app_dict['outputs'] = outputs

    if isinstance(outputs, dict) and '@type' not in outputs:
        outputs['@type'] = 'JsonSchema'

    requirements = app_dict.get('requirements', {})
    environment = requirements.get('environment')

    # container type
    if (environment and
            isinstance(environment.get('container'), dict) and
            environment['container'].get('type') == 'docker'):
        environment['container']['@type'] = 'Docker'


    job_dict = from_url(args.job_order)

    job_dict['app'] = app_dict

    def path_mapper(path):
        return os.path.normpath(os.path.join(args.basedir or '.', path))

    context = Context(TYPE_MAP, None)
    for module in (
            rabix.cli, rabix.expressions, rabix.schema, rabix.docker
    ):
        module.init(context)

    job_dict['@type'] = 'Job'
    job_dict['@id'] = args.basedir
    job = context.from_dict(job_dict)

    cli_job = CLIJob(job)
    print(json.dumps({
        'args': cli_job.make_arg_list(),
        'stdin': cli_job.stdin,
        'stdout': cli_job.stdout,
    }))


if __name__ == "__main__":
    main()
