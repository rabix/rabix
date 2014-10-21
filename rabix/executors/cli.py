import docopt
import sys
import logging

from rabix import __version__ as version
from rabix.executors.runner import DockerRunner, NativeRunner
from rabix.cliche.adapter import Adapter, from_url
from rabix.common.util import set_log_level


TEMPLATE_JOB = {
    'app': 'http://example.com/app.json',
    'inputs': {},
    'platform': 'http://example.org/my_platform/v1',
    'allocatedResources': {}
}

USAGE = '''
Usage:
    rabix [-v...] [-hci] [-d <dir>] -j <job> [-t <tool>] [-- {inputs}...]
    rabix [-v...] [-hci] [-d <dir>] -t <tool> [--] {inputs}...
    rabix --version

Options:
  -d --dir=<dir>       Working directory for the task. If not provided one will
                       be auto generated in the current dir.
  -h --help            Show this help message. In conjunction with job or tool,
                       it will print inputs you can provide for the job.
  -i --install         Only install referenced tools. Do not run anything.
  -j --job=<job>       URI to job order document to run.
  -c --print-cli       Only print calculated command line. Do not run anything.
  -t --tool=<tool>     URI to tool description document to run.
  -v --verbose         Verbosity. More Vs more output.
     --version         Print version and exit.
'''

TOOL_TEMPLATE = '''
Usage:
  tool {inputs}
'''


def make_tool_usage_string(tool, template=TOOL_TEMPLATE):
    inputs = tool.get('inputs', {}).get('properties')
    usage_str = []
    for k, v in inputs.items():
        if v.get('type') == 'file':
            arg = '--%s=<%s_file>' % (k, k)
            usage_str.append(arg if v.get('required') else '[%s]' % arg)
        elif v.get('type') == 'array' and \
                (v.get('items').get('type') == 'file' or
                    v.get('items').get('type') == 'directory'):
            arg = '--%s=<%s_file>...' % (k, k)
            usage_str.append(arg if v.get('required') else '[%s]' % arg)
    return template.format(inputs=' '.join(usage_str))


def get_inputs(tool, args):
    inp = {}
    inputs = tool.get('inputs', {}).get('properties')
    for k in inputs.keys():
        val = args.get('--' + k)
        if val:
            if isinstance(val, list):
                inp[k] = []
                for v in val:
                    inp[k].append({'path': v})
            else:
                inp[k] = {'path': val}
    return {'inputs': inp}


def update_paths(job, inputs):
    for inp in inputs['inputs'].keys():
        job['inputs'][inp] = inputs['inputs'][inp]
    return job


def get_tool(args):
    if args['--tool']:
        return from_url(args['--tool'])
    if args['--job']:
        return from_url(args['--job']).get('tool')


def dry_run_parse(args=None):
    args = args or sys.argv[1:]
    args = args + ['an_input']
    usage = USAGE.format(inputs='<inputs>')
    return docopt.docopt(usage, args, version=version, help=False)


def main():
    logging.basicConfig(level=logging.WARN)
    if len(sys.argv) == 1:
        print(USAGE)
        return

    usage = USAGE.format(inputs='<inputs>')

    if len(sys.argv) == 2 and \
            (sys.argv[1] == '--help' or
             sys.argv[1] == '-h'):
        print(USAGE)
        return

    dry_run_args = dry_run_parse()

    if not (dry_run_args['--tool'] or dry_run_args['--job']):
        print('You have to specify a tool, either directly with '
              '--tool option or using a job that references a tool')
        print(usage)
        return

    tool = get_tool(dry_run_args)
    if not tool:
        print("Couldn't find tool.")
        return

    tool_usage = make_tool_usage_string(tool, USAGE)
    try:
        args = docopt.docopt(usage, version=version, help=False)
        job = TEMPLATE_JOB
        set_log_level(args['--verbose'])
        if args['--job']:
            job_from_arg = from_url(args.get('--job'))
            job_from_arg.pop('tool')
            job = job_from_arg

        if args['--help']:
            print(tool_usage)
            return

        tool_inputs = {}
        tool_inputs_usage = make_tool_usage_string(tool)
        if args['<inputs>']:
            tool_inputs = docopt.docopt(tool_inputs_usage, args['<inputs>'])

        inp = get_inputs(tool, tool_inputs)
        job = update_paths(job, inp)

        if args['--print-cli']:
            adapter = Adapter(tool)
            print(adapter.cmd_line(job))
            return

        if args['--install']:
            return

        runner = DockerRunner(tool)
        runner.run_job(job, job_id=args.get('--dir'))

    except docopt.DocoptExit:
        print(tool_usage)
        return


if __name__ == '__main__':
    main()
