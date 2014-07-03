import os
import sys
import logging

import docopt

from rabix import __version__ as version
from rabix.common import six
from rabix.common.loadsave import JsonLoader
from rabix.common.util import rnd_name, update_config
from rabix.runtime import from_url
from rabix.models import Pipeline
from rabix.runtime.engine import get_engine
from rabix.runtime.jobs import RunJob, InstallJob

log = logging.getLogger(__name__)


USAGE = """
Usage:
  rabix run [-v...] <file>
  rabix install [-v...] <file>
  rabix checksum [--method=(md5|sha1)] <jsonptr>
  rabix -h | --help
  rabix --version

Commands:
  run                       Run pipeline described in <file>. Depending on the
                            pipeline, additional parameters must be provided.
  install                   Install all the apps needed for running pipeline
                            described in <file>.
  checksum                  Calculate and print the checksum of json document
                            (or fragment) pointed by <jsonptr>

Options:
  -h --help                 Display this message.
  -m --method (md5|sha1)    Checksum type [default: sha1]
  --version                 Print version to standard output and quit.
  -v --verbose              Verbosity. More Vs more output.
"""

RUN_TPL = """
Usage: rabix run [-v...] {pipeline} {arguments}

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


def run(path):
    pipeline = Pipeline.from_app(from_url(path))
    pipeline.validate()
    usage_str = make_pipeline_usage_string(pipeline, path)
    args = docopt.docopt(usage_str, version=version)
    set_log_level(args['--verbose'])
    inputs = {
        i[len('--'):]: args[i]
        for i in args if i.startswith('--') and i != '--verbose'
    }
    job_id = rnd_name()
    job = RunJob(job_id, pipeline, inputs=inputs)
    get_engine(before_task=before_task).run(job)
    present_outputs(job.get_outputs())
    if job.status == RunJob.FAILED:
        print(job.error_message or 'Job failed')
        sys.exit(1)


def install(pipeline):
    pipeline = Pipeline.from_app(pipeline)
    job = InstallJob(rnd_name(), pipeline)
    get_engine(before_task=before_task).run(job)
    if job.status == InstallJob.FAILED:
        print(job.error_message)
        sys.exit(1)


def checksum(jsonptr, method='sha1'):
    loader = JsonLoader()
    obj = loader.load(jsonptr)
    print(method + '$' + loader.checksum(obj, method))


def main():
    logging.basicConfig(level=logging.WARN)
    if os.path.isfile('rabix.conf'):
        update_config()

    try:
        args = docopt.docopt(USAGE, version=version)
    except docopt.DocoptExit:
        if len(sys.argv) > 3:
            for a in sys.argv[2:]:
                if not a.startswith('-'):
                    return run(a)
        print(USAGE)
        return
    set_log_level(args['--verbose'])
    if args["run"]:
        pipeline_path = args['<file>']
        pipeline = from_url(pipeline_path)
        pipeline.validate()
        print(make_pipeline_usage_string(pipeline, pipeline_path))
    elif args["install"]:
        install(from_url(args['<file>']))
    elif args["checksum"]:
        checksum(args['<jsonptr>'], args['--method'])


if __name__ == '__main__':
    main()
