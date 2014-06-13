from __future__ import print_function

import os
import sys
import logging

from docopt import docopt, DocoptExit
from rabix import VERSION
from rabix.common.util import rnd_name
from rabix.runtime import from_url
from rabix.runtime.scheduler import SequentialScheduler, RunJob, InstallJob

log = logging.getLogger(__name__)


USAGE_TPL = """Usage:
rabix run {run_args}
rabix install <pipeline.json>

Options:
-h --help        Display this message.
--version        Print version to standard output and quit.
{options}
"""
USAGE = USAGE_TPL.format(run_args='<pipeline.json>', options='')


def make_pipeline_usage_string(pipeline, path):
    """
    Takes a pipeline path string and returns the usage string for it.
    As this is the usage string for a given pipeline, <pipeline.json>
    is replaced with the actual pipeline (for the 'run' cmd)
    """
    inputs = pipeline.get_inputs()

    usage_str = " "
    options = "\nInputs:\n"
    for inp_id, inp_details in inputs.iteritems():
        arg = "--" + inp_id + "=" + (inp_id + "_file").upper()
        arg += "..." if inp_details["list"] else ""
        usage_str += arg + " " if inp_details["required"] else "[" + arg + "] "
        options += '{0: <40}'.format(arg) + inp_details["description"] + "\n"
    docstring_args = {'run_args': path + usage_str, 'options': options}
    return USAGE_TPL.replace("rabix install <pipeline.json>\n", "").format(**docstring_args)


def before_task(task):
    print('Running', task.task_id)
    sys.stdout.flush()


def present_outputs(outputs):
    row_fmt = '{:<20}{:<80}{:>16}'
    print('')
    print(row_fmt.format('Output ID', 'File path', 'File size'))
    for out_id, file_list in outputs.iteritems():
        for path in file_list:
            print(row_fmt.format(out_id, path, str(os.path.getsize(path))))


def run():
    path = sys.argv[2]
    pipeline = from_url(path)
    args = docopt(make_pipeline_usage_string(pipeline, path), version=VERSION)
    inputs = {i[len('--'):]: args[i] for i in args if i.startswith('--')}
    job_id = rnd_name()
    job = RunJob(job_id, pipeline, inputs=inputs)
    SequentialScheduler(before_task=before_task).submit(job).run()
    present_outputs(job.get_outputs())


def install(pipeline):
    job = InstallJob(rnd_name(), pipeline)
    SequentialScheduler(before_task=before_task).submit(job).run()


def main():
    logging.basicConfig(level=logging.DEBUG)
    try:
        args = docopt(USAGE, version=VERSION)
    except DocoptExit:
        if len(sys.argv) > 3:
            return run()
        print(USAGE)
        return
    if args["run"]:
        pipeline_path = args['<pipeline.json>']
        pipeline = from_url(pipeline_path)
        print(make_pipeline_usage_string(pipeline, pipeline_path))
    elif args["install"]:
        install(from_url(args['<pipeline.json>']))


if __name__ == '__main__':
    main()
