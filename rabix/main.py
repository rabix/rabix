from __future__ import print_function
import os
import docopt
import sys
import logging
import six
import json

from rabix import __version__ as version
from rabix.common.util import log_level, dot_update_dict, map_or_apply,\
    map_rec_list, to_abspath
from rabix.common.models import Job, IO, ObjectConstructor, ArrayConstructor, FileConstructor
from rabix.common.context import Context
from rabix.common.ref_resolver import from_url
from rabix.common.errors import RabixError
from rabix.executor import Executor
from rabix.cli import CliApp, CLIJob

import rabix.cli
import rabix.docker
import rabix.expressions
import rabix.workflows
import rabix.schema


TEMPLATE_RESOURCES = {
    "cpu": 4,
    "mem": 5000
}


TEMPLATE_JOB = {
    'inputs': {},
    'platform': 'http://example.org/my_platform/v1',
    'allocatedResources': TEMPLATE_RESOURCES
}

USAGE = '''
Usage:
    rabix <tool> [-v...] [-hcI] [-t <type>] [-d <dir>] [-i <inp>] [{resources}] [-- {inputs}...]
    rabix --conformance-test [--basedir=<basedir>] [--no-container] <tool> <job> [-- <input>...]
    rabix --version

    Options:
  -d --dir=<dir>       Working directory for the task. If not provided one will
                       be auto generated in the current dir.
  -h --help            Show this help message. In conjunction with tool,
                       it will print inputs you can provide for the job.

  -I --install         Only install referenced tools. Do not run anything.
  -i --inp-file=<inp>  Inputs
  -c --print-cli       Only print calculated command line. Do not run anything.
  -t --type=<type>     Interpret given tool json as <type>.
  -v --verbose         Verbosity. More Vs more output.
     --version         Print version and exit.
'''

TOOL_TEMPLATE = '''
Usage:
  tool {inputs}
'''

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


###
# input massage
###

def fix_types(tool, toplevelType=None):

    toplevelType = toplevelType or 'CommandLine'

    # tool type
    if '@type' not in tool:
        tool['@type'] = toplevelType

    if tool.get('@type') == 'Job':
        fix_types(tool['app'])
        return

    requirements = tool.get('requirements', {})
    environment = requirements.get('environment')

    # container type
    if (environment and
            isinstance(environment.get('container'), dict) and
            environment['container'].get('type') == 'docker'):
        environment['container']['@type'] = 'Docker'

    if tool['@type'] == 'Workflow':
        for step in tool['steps']:
            fix_types(step['app'])

    # schema type
    inputs = tool.get('inputs')
    if isinstance(inputs, dict) and '@type' not in inputs:
        inputs['@type'] = 'JsonSchema'

    outputs = tool.get('outputs')
    if isinstance(outputs, dict) and '@type' not in outputs:
        outputs['@type'] = 'JsonSchema'


def rebase_input_path(input, value, base):
    if isinstance(input.constructor, ObjectConstructor):

        def do_rebase_obj(val):
            ret = {}
            for k, v in six.iteritems(val):
                rebased = rebase_input_path(input.properties[k], v, base)
                if rebased:
                    ret[k] = rebased
            return ret

        return map_or_apply(dot_update_dict, value)

    if isinstance(input.constructor, ArrayConstructor):
        ret = []
        for item in value:
            rebased = rebase_input_path(input.item_constructor, item, base)
            if rebased:
                ret.append(rebased)
        return ret

    def do_rebase(v):
        if isinstance(v, dict):
            v['path'] = to_abspath(v['path'], base)
            return v
        return to_abspath(v, base)

    if input.constructor == FileConstructor:
        return map_or_apply(do_rebase, value)

    return None


def rebase_paths(app, input_values, base):
    file_inputs = {}
    for input_name, val in six.iteritems(input_values):
        input = app.get_input(input_name)
        rebased = rebase_input_path(input, val, base)
        if rebased:
            file_inputs[input_name] = rebased

    return dot_update_dict(input_values, file_inputs)


###
# usage strings
###

def make_resources_usage_string(template=TEMPLATE_RESOURCES):
    param_str = []
    for k, v in six.iteritems(template):
        if type(v) is bool:
            arg = '--resources.%s' % k
        else:
            arg = '--resources.%s=<%s>' % (k, type(v).__name__)
        param_str.append(arg)
    return ' '.join(param_str)


def make_app_usage_string(app, template=TOOL_TEMPLATE, inp=None):

    inp = inp or {}

    def resolve(k, v, usage_str, param_str, inp):
        if isinstance(v.constructor, ObjectConstructor):
            return

        to_append = usage_str if v.constructor == FileConstructor\
            else param_str

        cname = getattr(v.constructor, 'name', None) or \
            getattr(v.constructor, '__name__', 'val')

        if cname in ('bool'):
            arg = '--%s' % k
        else:
            arg = '--%s=<%s>' % (k, cname)

        if v.depth > 0:
            arg += '... '

        if not v.required or v.id in inp:
            arg = '['+arg+']'

        to_append.append(arg)

    def resolve_object(obj, usage_str, param_str, inp, root=False):
        properties = obj.inputs.io if root else obj.objects
        for input in properties:
            key = input.id if root else '.'.join([obj.id, input.id])
            resolve(key, input, usage_str, param_str, inp.keys())

    usage_str = []
    param_str = []

    resolve_object(app, usage_str, param_str, inp, root=True)
    usage_str.extend(param_str)
    return template.format(resources=make_resources_usage_string(),
                           inputs=' '.join(usage_str))


def get_inputs(app, args):

    def get_arg(name):
        return args.get('--' + name) or args.get(name)

    return {
        input.id: map_rec_list(input.constructor, get_arg(input.id))
        for input in app.inputs.io
        if get_arg(input.id)
    }


def get_tool(args):
    if args['<tool>']:
        return from_url(args['<tool>'])


def dry_run_parse(args=None):
    args = args or sys.argv[1:]
    args += ['an_input']
    usage = USAGE.format(resources=make_resources_usage_string(),
                         inputs='<inputs>')
    try:
        return docopt.docopt(usage, args, version=version, help=False)
    except docopt.DocoptExit:
        return


def conformance_test(context, app, job_dict, basedir):
    job_dict['@type'] = 'Job'
    job_dict['@id'] = basedir
    job_dict['app'] = app

    if not app.outputs:
        app.outputs = rabix.schema.JsonSchema(context, {
            'type': 'object',
            'properties': {}
        })

    inputs = rebase_paths(app, job_dict['inputs'], basedir)
    inputs = get_inputs(app, inputs)

    dot_update_dict(job_dict['inputs'], inputs)

    job = context.from_dict(job_dict)

    adapter = CLIJob(job)

    print(json.dumps({
        'args': adapter.make_arg_list(),
        'stdin': adapter.stdin,
        'stdout': adapter.stdout,
    }))


def main():
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
        print("Couldn't find tool.")
        return

    fix_types(tool, dry_run_args.get('--type', 'CommandLine'))

    context = init_context()
    app = context.from_dict(tool)
    job = None

    if isinstance(app, Job):
        job = app
        app = job.app

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
        job_dict = TEMPLATE_JOB
        logging.root.setLevel(log_level(dry_run_args['--verbose']))

        if args['--inp-file']:
            basedir = os.path.dirname(args.get('--inp-file'))
            input_file = from_url(args.get('--inp-file'))
            rebased = rebase_paths(app, input_file, basedir)
            dot_update_dict(
                job_dict['inputs'],
                get_inputs(app, rebased)
            )

        input_usage = job_dict['inputs']

        if job:
            basedir = os.path.dirname(args.get('<tool>'))
            rebased = rebase_paths(app, job.inputs, basedir)
            dot_update_dict(job.inputs, rebased)
            job.inputs.update(get_inputs(app, job.inputs))
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

        inp = get_inputs(app, app_inputs)
        if not job:
            job_dict['inputs'].update(inp)
            job_dict['@id'] = args.get('--dir')
            job_dict['app'] = app
            job = Job.from_dict(context, job_dict)
        else:
            job.inputs.update(inp)

        if args['--print-cli']:
            if not isinstance(app, CliApp):
                print(dry_run_args['<tool>'] + " is not a command line app")
                return

            print(CLIJob(job).cmd_line())
            return

        if not job.inputs and not args['--']:
            print(app_usage)
            return

        try:
            context.executor.execute(job, lambda _, result: print(result))
        except RabixError as err:
            print(err.message)

    except docopt.DocoptExit:
        print(app_usage)
        return


if __name__ == '__main__':
    main()
