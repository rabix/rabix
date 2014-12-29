from __future__ import print_function
import os
import docopt
import sys
import logging
import six

from six.moves.urllib.parse import urlparse

from rabix import __version__ as version
from rabix.common.util import log_level, dot_update_dict
from rabix.common.models import Job, IO, File
from rabix.common.context import Context
from rabix.common.ref_resolver import from_url
from rabix.common.errors import RabixError
from rabix.cli.adapter import CLIJob
from rabix.executor import Executor
from rabix.cli import CliApp

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
    'app': 'http://example.com/app.json',
    'inputs': {},
    'platform': 'http://example.org/my_platform/v1',
    'allocatedResources': TEMPLATE_RESOURCES
}

USAGE = '''
Usage:
    rabix <tool> [-v...] [-hcI] [-d <dir>] [-i <inp>] [{resources}] [-- {inputs}...]
    rabix --version

    Options:
  -d --dir=<dir>       Working directory for the task. If not provided one will
                       be auto generated in the current dir.
  -h --help            Show this help message. In conjunction with tool,
                       it will print inputs you can provide for the job.

  -I --install         Only install referenced tools. Do not run anything.
  -i --inp-file=<inp>  Inputs
  -c --print-cli       Only print calculated command line. Do not run anything.
  -v --verbose         Verbosity. More Vs more output.
     --version         Print version and exit.
'''

TOOL_TEMPLATE = '''
Usage:
  tool {inputs}
'''


def make_resources_usage_string(template=TEMPLATE_RESOURCES):
    param_str = []
    for k, v in six.iteritems(template):
        if type(v) is bool:
            arg = '--resources.%s' % k
        else:
            arg = '--resources.%s=<%s>' % (k, type(v).__name__)
        param_str.append(arg)
    return ' '.join(param_str)


TYPE_MAP = {
    'Job': Job.from_dict,
    'IO': IO.from_dict,
    'File': File.from_dict
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


def fix_types(tool):
    requirements = tool.get('requirements', {})
    environment = requirements.get('environment')

    # container type
    if (environment and
            isinstance(environment.get('container'), dict) and
            environment['container'].get('type') == 'docker'):
        environment['container']['@type'] = 'Docker'

    # tool type
    if '@type' not in tool:
        tool['@type'] = 'CommandLine'

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


def make_app_usage_string(app, template=TOOL_TEMPLATE, inp=None):

    inp = inp or {}

    def resolve(k, v, usage_str, param_str, inp):

        if v.constructor == TYPE_MAP['File']:
            arg = '--%s=<file>' % k
            to_append = usage_str
        else:
            arg = '--%s=<%s>' % (k, v.constructor.__name__)
            to_append = param_str

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


def resolve_path(basedir, url):
    if basedir and url.scheme == 'file' and not os.path.isabs(url.path):
        l = list(url)  # transform from tuple
        l[2] = os.path.join(basedir, url.path)  # update path
        url = url.__class__(*l)  # return back to original type

    return url


def construct_value(constructor, val, basedir):
    if constructor == TYPE_MAP['File']:
        url = urlparse(val, 'file')
        url = resolve_path(basedir, url)
        val = {'path': url}

    return constructor(val)


def resolve_values(inp, nval, inputs, basedir=None):
    if isinstance(nval, list):
        inputs[inp.id] = [construct_value(inp.constructor, nv, basedir)
                          for nv in nval]
    else:
        inputs[inp.id] = construct_value(inp.constructor, nval, basedir)


def get_inputs_from_file(app, args, startdir):
    inp = {}
    inputs = app.inputs.io
    resolve_nested_paths(inp, inputs, args, startdir)
    return {'inputs': inp}


def resolve_nested_paths(inp, inputs, args, startdir):
    for input in inputs:
        nval = args.get(input.id)
        if nval:
            if input.depth > 0 and input.constructor == dict:
                inp[input.id] = []
                for sk, sv in enumerate(nval):
                    inp[input.id].append({})
                    resolve_nested_paths(
                        inp[input.id][sk],
                        input.objects,
                        args[input.id],
                        startdir
                    )
            else:
                resolve_values(input, nval, inp, startdir)


def get_inputs(app, args):
    inputs = {}
    properties = app.inputs.io
    for input in properties:
        nval = args.get('--' + input.id) or args.get(input.id)
        if nval:
            resolve_values(input, nval, inputs)
    return {'inputs': inputs}


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

    fix_types(tool)

    context = init_context()
    app = context.from_dict(tool)

    if dry_run_args['--install']:
        app.install()
        print("Install successful.")
        return

    try:
        args = docopt.docopt(usage, version=version, help=False)
        job = TEMPLATE_JOB
        logging.root.setLevel(log_level(dry_run_args['--verbose']))

        if args['--inp-file']:
            startdir = os.path.dirname(args.get('--inp-file'))
            input_file = from_url(args.get('--inp-file'))
            dot_update_dict(
                job['inputs'],
                get_inputs_from_file(app, input_file, startdir)['inputs']
            )

        app_inputs_usage = make_app_usage_string(
            app, template=TOOL_TEMPLATE, inp=job['inputs'])

        app_usage = make_app_usage_string(app, USAGE, job['inputs'])

        app_inputs = docopt.docopt(app_inputs_usage, args['<inputs>'])

        if args['--help']:
            print(app_usage)
            return

        inp = get_inputs(app, app_inputs)
        job['inputs'].update(inp['inputs'])

        if args['--print-cli']:
            if not isinstance(app, CliApp):
                print(dry_run_args['<tool>'] + " is not a command line app")
                return
            job['@id'] = args.get('--dir')
            job['app'] = app.to_dict(context)
            j = Job.from_dict(context, job)
            adapter = CLIJob(j.to_dict(context), j.app)
            print(adapter.cmd_line())
            return

        job['@id'] = args.get('--dir')
        job['app'] = app.to_dict(context)

        try:
            context.executor.execute(Job.from_dict(context, job),
                                     lambda _, result: print(result))
        except RabixError as err:
            print(err.message)

    except docopt.DocoptExit:
        print(app_usage)
        return


if __name__ == '__main__':
    main()
