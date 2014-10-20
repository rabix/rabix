import docopt
import sys
import logging

from rabix import __version__ as version
from rabix.executors.validations import validate_inputs
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
    rabix [-v...] [-hci] -j <job> [-t <tool>] [{inputs}] [-d <dir>]
    rabix [-v...] [-hci] -t <tool> {inputs} [-d <dir>]
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


def make_tool_usage_string(tool):
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
    return USAGE.format(inputs=' '.join(usage_str))


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


def main():
    logging.basicConfig(level=logging.WARN)
    DOCOPT = USAGE
    if len(sys.argv) == 1:
        print(DOCOPT)
        return

    # A bit of a hack: inputs in the original USAGE string doubles as both
    # something user should see and as python pattern that will be expanded
    # once we figure out what tool are we talking about. In order to parse CLI
    # even without provided tool-dependent required inputs, we are adding
    # literal '{inputs}' here to help us analyze args before 'real' CLI parsing
    args = sys.argv[1:] + ['{inputs}']
    dry_run_args = docopt.docopt(DOCOPT, args, version=version)

    if not (dry_run_args['--tool'] or dry_run_args['--job']):
        print('You have to specify a tool, either directly with '
              '--tool option or using a job that references a tool')

    tool = get_tool(dry_run_args)
    if not tool:
        print("Couldn't find tool.")
        return

    DOCOPT = make_tool_usage_string(tool)
    try:
        args = docopt.docopt(DOCOPT, version=version)
        job = TEMPLATE_JOB
        set_log_level(args['--verbose'])
        if args['--job']:
            job_from_arg = from_url(args.get('--job'))
            job_from_arg.pop('tool')
            job = job_from_arg

        if args['--help']:
            print(DOCOPT)
            return

        inp = get_inputs(tool, args)
        job = update_paths(job, inp)

        if args['--print-cli']:
            adapter = Adapter(tool)
            print(adapter.cmd_line(job))
            return

        if args['--install']:
            return

        validate_inputs(tool, job)

        runner = DockerRunner(tool)
        runner.run_job(job, job_id=args.get('--dir'))

    except docopt.DocoptExit:
        print(DOCOPT)
        return


if __name__ == '__main__':
    main()
