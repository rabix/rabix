import docopt
import sys

from rabix import __version__ as version
from rabix.executors.validations import validate_inputs
from rabix.executors.runner import DockerRunner
from rabix.cliche.adapter import from_url


TEMPLATE_JOB = {
    'app': 'http://example.com/app.json',
    'inputs': {},
    'platform': 'http://example.org/my_platform/v1',
    'allocatedResources': {}
}

USAGE = '''
Usage:
    rabix run [-v] (--job=<job> [--tool=<tool> {inputs} --dir=<dir>] | --tool=<tool> {inputs} [--dir=<dir>])
    rabix -h

Options:
  -h --help                               Show help
  -v --verbose                            Verbosity. More Vs more output.
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
    for inx, arg in enumerate(args):
        if '--tool' in arg:
            tool_url = arg.split('=')
            if len(tool_url) == 2:
                return from_url(tool_url[1]).get('tool')
            else:
                return from_url(args[inx+1]).get('tool')
    for inx, arg in enumerate(args):
        if '--job' in arg:
            job_url = arg.split('=')
            if len(job_url) == 2:
                return from_url(job_url[1]).get('job', {}).get('tool')
            else:
                return from_url(args[inx+1]).get('job', {}).get('tool')


def main():
    DOCOPT = USAGE
    if len(sys.argv) == 1:
        print(DOCOPT)
        return
    if sys.argv[1] == 'run' and len(sys.argv) > 2:
        tool = get_tool(sys.argv)
        if not tool:
            raise Exception('Need to specify tool')
        DOCOPT = make_tool_usage_string(tool)
    try:
        args = docopt.docopt(DOCOPT, version=version)
        if args['run']:
            job = TEMPLATE_JOB
            if args['--job']:
                job_from_arg = from_url(args.get('--job', {})).get('job')
                job_from_arg.pop('tool')
                job = job_from_arg
            inp = get_inputs(tool, args)
            job = update_paths(job, inp)
            validate_inputs(tool, job)
            runner = DockerRunner(tool)
            runner.run_job(job, job_id=args.get('--dir'))
    except docopt.DocoptExit:
        print(DOCOPT)
        return


if __name__ == '__main__':
    main()
