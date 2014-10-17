import docopt
import logging
import sys
import yaml
import six

from os.path import isfile

from rabix import __version__ as version
from rabix.common.errors import RabixError
from rabix.cliche.ref_resolver import Loader, from_url
from rabix.common.util import rnd_name
from rabix.tools.steps import run_steps


log = logging.getLogger(__name__)


USAGE = """
Usage:
  rabix build [-v...] [--config=<cfg_path>]
  rabix checksum [--method=(md5|sha1)] <jsonptr>
  rabix install [-v...] <file>
  rabix test [-v...] <tool>
  rabix -h | --help
  rabix --version

Commands:
  build                     Execute steps for app building, wrapping and
                            deployment.
  checksum                  Calculate and print the checksum of json document
                            (or fragment) pointed by <jsonptr>
  install                   Install all the apps needed for running pipeline,
                            job or tool described in <file>.
  test                      Test if tool description file produces expected
                            command lines.

Options:
  -c --config=<cfg_path>    Specify path to config file [default: .rabix.yml]
  -h --help                 Display this message.
  -m --method (md5|sha1)    Checksum type [default: sha1]
  --version                 Print version to standard output and quit.
  -v --verbose              Verbosity. More Vs more output.
"""

RUN_TPL = """
Usage: (--job=<job> [--tool=<tool> {inputs}] | --tool=<tool> {inputs})

Options:
  -v --verbose                            Verbosity. More Vs more output.
  {options}
"""


def make_pipeline_usage_string(pipeline, path):
    usage_str, options = [], []
    for inp_id, inp_details in six.iteritems(pipeline.get_inputs()):
        arg = '--%s=<%s_file>%s' % (
            inp_id, inp_id, '...' if inp_details['list'] else ''
        )
        usage_str.append(
            arg if inp_details.get('required', False) else '[%s]' % arg
        )
        options.append('{0: <40}{1}'.format(
            arg, inp_details.get('description', ''))
        )
    return RUN_TPL.format(pipeline=path, arguments=' '.join(usage_str),
                          options='\n  '.join(options))


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


def before_task(task):
    print('Running %s' % task.task_id)
    sys.stdout.flush()


def present_outputs(outputs):
    header = False
    row_fmt = '{:<20}{:<80}'
    for out_id, file_list in six.iteritems(outputs):
        for path in file_list:
            if not header:
                print(row_fmt.format('Output ID', 'File path'))
                header = True
            print(row_fmt.format(out_id, path))


def set_log_level(v_count):
    if v_count == 0:
        level = logging.WARN
    elif v_count == 1:
        level = logging.INFO
    else:
        level = logging.DEBUG
    logging.root.setLevel(level)


def install(pipeline):
    pipeline = Pipeline.from_app(pipeline)
    job = InstallJob(rnd_name(), pipeline)
    get_engine(before_task=before_task).run(job)
    if job.status == InstallJob.FAILED:
        print(job.error_message)
        sys.exit(1)


def checksum(jsonptr, method='sha1'):
    loader = Loader()
    obj = loader.load(jsonptr)
    print(method + '$' + loader.checksum(obj, method))


def build(path='.rabix.yml'):
    if not isfile(path):
        raise RabixError('Config file %s not found!' % path)
    with open(path) as cfg:
        config = yaml.load(cfg)
        run_steps(config)


def main():
    logging.basicConfig(level=logging.WARN)

    args = docopt.docopt(USAGE, version=version)

    set_log_level(args['--verbose'])
    if args["install"]:
        install(from_url(args['<file>']))
    elif args["checksum"]:
        checksum(args['<jsonptr>'], args['--method'])
    elif args["build"]:
        build(args.get("--config"))
    elif args["test"]:
        pass


if __name__ == '__main__':
    main()
