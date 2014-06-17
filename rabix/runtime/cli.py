import os
import sys
import logging

from docopt import docopt, DocoptExit

from rabix import VERSION
from rabix.common.util import rnd_name
from rabix.runtime import from_url
from rabix.runtime.models import Pipeline
from rabix.runtime.engine import SequentialEngine, get_engine
from rabix.runtime.jobs import RunJob, InstallJob

log = logging.getLogger(__name__)


USAGE = """
Usage:
  rabix run [-v] <file>
  rabix install [-v] <file>
  rabix -h | --help
  rabix --version

Options:
  -h --help        Display this message.
  --version        Print version to standard output and quit.
  -v --verbose     Log level set to DEBUG
"""

RUN_TPL = """
Usage: rabix run [-v] {pipeline} {arguments}

Options:
  -v --verbose                            Log level set to DEBUG
  {options}
"""


def make_pipeline_usage_string(pipeline, path):
    usage_str, options = [], []
    for inp_id, inp_details in pipeline.get_inputs().iteritems():
        arg = '--%s=<%s_file>' % (inp_id, inp_id) + ('...' if inp_details['list'] else '')
        usage_str.append(arg if inp_details['required'] else '[%s]' % arg)
        options.append('{0: <40}{1}'.format(arg, inp_details.get('description', '')))
    return RUN_TPL.format(pipeline=path, arguments=' '.join(usage_str), options='\n  '.join(options))


def before_task(task):
    print 'Running', task.task_id
    sys.stdout.flush()


def present_outputs(outputs):
    header = False
    row_fmt = '{:<20}{:<80}{:>16}'
    for out_id, file_list in outputs.iteritems():
        for path in file_list:
            if not header:
                print row_fmt.format('Output ID', 'File path', 'File size')
                header = True
            print row_fmt.format(out_id, path, str(os.path.getsize(path)))


def run(path):
    pipeline = Pipeline.from_app(from_url(path))
    args = docopt(make_pipeline_usage_string(pipeline, path), version=VERSION)
    logging.root.setLevel(logging.DEBUG if args['--verbose'] else logging.WARN)
    inputs = {i[len('--'):]: args[i] for i in args if i.startswith('--') and i != '--verbose'}
    job_id = rnd_name()
    job = RunJob(job_id, pipeline, inputs=inputs)
    get_engine(before_task=before_task).run(job)
    present_outputs(job.get_outputs())
    if job.status == RunJob.FAILED:
        print job.error_message or 'Job failed'
        sys.exit(1)


def install(pipeline):
    pipeline = Pipeline.from_app(pipeline)
    job = InstallJob(rnd_name(), pipeline)
    SequentialEngine(before_task=before_task).run(job)
    if job.status == InstallJob.FAILED:
        print job.error_message
        sys.exit(1)


def main():
    logging.basicConfig(level=logging.WARN)
    try:
        args = docopt(USAGE, version=VERSION)
    except DocoptExit:
        if len(sys.argv) > 3:
            for a in sys.argv[2:]:
                if not a.startswith('-'):
                    return run(a)
        print USAGE
        return
    logging.root.setLevel(logging.DEBUG if args['--verbose'] else logging.WARN)
    if args["run"]:
        pipeline_path = args['<file>']
        pipeline = from_url(pipeline_path)
        print make_pipeline_usage_string(pipeline, pipeline_path)
    elif args["install"]:
        install(from_url(args['<file>']))


if __name__ == '__main__':
    main()
