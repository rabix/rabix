from __future__ import print_function
import os
import docopt
import sys
import logging
import six
import json
import copy

from avro.schema import NamedSchema
from functools import partial

# prevent naming collision with docker package when running directly as script
from rabix.conformance import conformance_test

script_dir = os.path.dirname(os.path.realpath(__file__))
if script_dir in sys.path:
    sys.path.remove(script_dir)

from rabix import __version__ as version
from rabix.common.util import log_level, result_str
from rabix.common.models import Job, process_builder, get_inputs
from rabix.common.context import Context
from rabix.common.ref_resolver import from_url
from rabix.common.errors import RabixError
from rabix.executor import Executor
from rabix.cli import CommandLineTool, CLIJob

import rabix.cli
import rabix.docker
import rabix.expressions
import rabix.workflows


TEMPLATE_RESOURCES = {
    "cpu": 4,
    "mem": 5000
}


TEMPLATE_JOB = {
    'class': 'Job',
    'inputs': {},
    'platform': 'http://example.org/my_platform/v1',
    'allocatedResources': TEMPLATE_RESOURCES
}

USAGE = """
Usage:
    rabix [-v...] [-hcpI] [-t <type>] [-d <dir>] [-i <inp>] [{resources}] <tool> [-- {inputs}...]
    rabix [--outdir=<outdir>] [--quiet] <tool> <inp>
    rabix --conformance-test [--basedir=<basedir>] [--no-container] [--quiet] <tool> <job>
    rabix --version

Options:
  -d --dir=<dir>        Working directory for the task. If not provided one will
                        be auto generated in the current dir.
  -h --help             Show this help message. In conjunction with tool,
                        it will print inputs you can provide for the job.

  -I --install          Only install referenced tools. Do not run anything.
  -i --inp-file=<inp>   Inputs
  -c --print-cli        Only print calculated command line. Do not run anything.
  -p --pretty-print     Print human readable result instead of JSON.
  -t --type=<type>      Interpret given tool json as <type>.
  -v --verbose          Verbosity. More Vs more output.
     --version          Print version and exit.
"""

TOOL_TEMPLATE = """
Usage:
  tool {inputs}
"""


def disable_warnings():
    import requests
    requests.packages.urllib3.disable_warnings()


def init_context(d):
    executor = Executor()
    context = Context(executor)

    for module in (
            rabix.common.models, rabix.cli, rabix.expressions, rabix.workflows, rabix.docker
    ):
        module.init(context)

    if d.get('class') == 'Job':
        context.build_from_document(d['app'])
    else:
        context.build_from_document(d)
    return context


###
# usage strings
###

def make_resources_usage_string(template=TEMPLATE_RESOURCES):
    param_str = []
    for k, v in six.iteritems(template):
        arg = ('--resources.%s' % k) if type(v) is bool \
            else ('--resources.%s=<%s>' % (k, type(v).__name__))
        param_str.append(arg)

    return ' '.join(param_str)


def make_app_usage_string(app, template=TOOL_TEMPLATE, inp=None):

    inp = inp or {}

    def resolve(k, v, usage_str, param_str, inp):
        if (v.validator.type == 'record' and
                v.validator.name != 'File'):
            return

        to_append = usage_str if (isinstance(v.validator, NamedSchema) and
                                  v.validator.name == 'File')\
            else param_str

        cname = v.validator.name if isinstance(v.validator, NamedSchema)\
            else v.validator.type

        prefix = '--%s' % k
        suffix = '' if v.validator.type == 'boolean' else '=<%s>' % cname

        arg = prefix + suffix

        if v.depth > 0:
            arg += '... '

        if not v.required or v.id in inp:
            arg = '['+arg+']'

        to_append.append(arg)

    def resolve_object(obj, usage_str, param_str, inp, root=False):
        properties = obj.inputs if root else obj.objects
        for input in properties:
            key = input.id if root else '.'.join([obj.id, input.id])
            resolve(key, input, usage_str, param_str, inp.keys())

    usage_str = []
    param_str = []

    resolve_object(app, usage_str, param_str, inp, root=True)
    usage_str.extend(param_str)
    return template.format(resources=make_resources_usage_string(),
                           inputs=' '.join(usage_str))


def get_tool(args):
    if args['<tool>']:
        return from_url(args['<tool>'])


def dry_run_parse(args=None):
    args = args or sys.argv[1:]
    if '--' in args:
        args = args[:args.index('--')]

    usage = USAGE.format(resources=make_resources_usage_string(),
                         inputs='<inputs>')
    try:
        return docopt.docopt(usage, args, version=version, help=False)
    except docopt.DocoptExit:
        return


def fail(message):
    print(message)
    sys.exit(1)


def main():
    disable_warnings()
    logging.basicConfig(level=logging.WARN)
    if len(sys.argv) == 1:
        print(USAGE)
        return

    usage = USAGE.format(resources=make_resources_usage_string(),
                         inputs='<inputs>')
    app_usage = usage

    if len(sys.argv) == 2 and \
            (sys.argv[1] == '--help' or sys.argv[1] == '-h'):
        print(USAGE)
        return

    dry_run_args = dry_run_parse()
    if not dry_run_args:
        print(USAGE)
        return

    if not (dry_run_args['<tool>']):
        print('You have to specify a tool, with --tool option')
        print(usage)
        return

    tool = get_tool(dry_run_args)
    if not tool:
        fail("Couldn't find tool.")

    if 'class' not in tool:
        fail("Document must have a 'class' field")

    if 'id' not in tool:
        tool['id'] = dry_run_args['<tool>']

    context = init_context(tool)

    app = process_builder(context, tool)
    job = None

    if isinstance(app, Job):
        job = app
        app = job.app

    rabix.expressions.update_engines(app)

    if dry_run_args['--install']:
        app.install()
        print("Install successful.")
        return

    if dry_run_args['--conformance-test']:
        job_dict = from_url(dry_run_args['<job>'])
        conformance_test(context, app, job_dict, dry_run_args.get('--basedir'))
        return

    try:
        args = docopt.docopt(usage, version=version, help=False)
        job_dict = copy.deepcopy(TEMPLATE_JOB)
        logging.root.setLevel(log_level(dry_run_args['--verbose']))

        input_file_path = args.get('<inp>') or args.get('--inp-file')
        if input_file_path:
            basedir = os.path.dirname(os.path.abspath(input_file_path))
            input_file = from_url(input_file_path)
            inputs = get_inputs(input_file, app.inputs, basedir)
            job_dict['inputs'].update(inputs)

        input_usage = job_dict['inputs']

        if job:
            basedir = os.path.dirname(args.get('<tool>'))
            job.inputs = get_inputs(job.inputs, app.inputs, basedir)
            input_usage.update(job.inputs)

        app_inputs_usage = make_app_usage_string(
            app, template=TOOL_TEMPLATE, inp=input_usage)

        app_usage = make_app_usage_string(app, USAGE, job_dict['inputs'])

        try:
            app_inputs = docopt.docopt(app_inputs_usage, args['<inputs>'])
        except docopt.DocoptExit:
            if not job:
                raise
            for inp in job.app.inputs:
                if inp.required and inp.id not in job.inputs:
                    raise
            app_inputs = {}

        if args['--help']:
            print(app_usage)
            return
        # trim leading --, and ignore empty arays
        app_inputs = {
            k[2:]: v
            for k, v in six.iteritems(app_inputs)
            if v != []
        }

        inp = get_inputs(app_inputs, app.inputs)
        if not job:
            job_dict['id'] = args.get('--outdir') or args.get('--dir')
            job_dict['app'] = app
            job = Job.from_dict(context, job_dict)

        job.inputs.update(inp)

        if args['--print-cli']:
            if not isinstance(app, CommandLineTool):
                fail(dry_run_args['<tool>'] + " is not a command line app")

            print(CLIJob(job).cmd_line())
            return

        if args['--pretty-print']:
            fmt = partial(result_str, job.id)
        else:
            fmt = lambda result: json.dumps(context.to_primitive(result))

        if not job.inputs and not args['--']:
            print(app_usage)
            return

        try:
            context.executor.execute(job, lambda _, result: print(fmt(result)))
        except RabixError as err:
            fail(err.message)

    except docopt.DocoptExit:
        fail(app_usage)


if __name__ == '__main__':
    main()
