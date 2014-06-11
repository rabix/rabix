from __future__ import print_function

import os
import sys
import logging

from docopt import docopt, DocoptExit
from rabix import VERSION
from rabix.runtime import from_url
from rabix.runtime.graph import JobGraph, RunFailed
from rabix.runtime.runners import RUNNER_MAP
from rabix.runtime.cli_helpers import before_job, after_job, present_outputs

log = logging.getLogger(__name__)


USAGE_TEMPLATE = """Usage:
cli.py run {run_args}
cli.py install <ref>

Options:
-h --help        Display this message.
--version        Print version to standard output and quit.
<ref>            Path to JSON tool or pipeline description.
{options}
"""

DEFAULT_TEMPLATE_ARGS = {'run_args': '<pipeline.json> [-- <pipeline_args>...]',
                         'options': '<pipeline.json>  Path to a JSON file describing a pipeline.'}


def load_pipeline(path):
    """
    Takes a string path and returns a pipeline object obtained by parsing
    the JSON file indicated by the path
    """
    if '://' in path:
        print('Currently can only use local pipeline files')
        sys.exit(1)
    if not os.path.isfile(path):
        print('Not a file', path)
        sys.exit(1)
    return from_url(path)


def make_pipeline_usage_string(pipeline, path):
    """
    Takes a pipeline path string and returns the usage string for it.
    As this is the usage string for a given pipeline, <pipeline.json>
    is replaced with the actual pipeline (for the 'run' cmd)
    """
    inputs = pipeline.get_inputs()

    usage_str = " -- "
    options = "\nPipelne options:\n"
    for i in inputs.keys():
        arg = "--" + i + "=" + (i + "_file").upper()
        arg += "..." if inputs[i]["list"] else ""
        usage_str += arg + " " if inputs[i]["required"] else "[" + arg + "] "
        options += '{0: <40}'.format(arg) + inputs[i]["description"] + "\n"
    pipeline_docstring = {'run_args': path + usage_str,
                          'options': options}
    USAGE_TEMPLATE.replace("run <pipeline.json>", "run " + path) + " "
    return USAGE_TEMPLATE.format(**pipeline_docstring)


def parse(argv=None):
    """
    Read the array of command line arguments and determine if
    the user is requesting info about the pipeline or
    just wants to run the pipeline or wants to do something else.
    """
    args = docopt(USAGE_TEMPLATE.format(**DEFAULT_TEMPLATE_ARGS), argv=argv, version=VERSION)

    if args["run"]:
        pipeline_path = args['<pipeline.json>']
        pipeline = load_pipeline(pipeline_path)
        pipeline_args = args.get("<pipeline_args>")
        pipeline_usage = make_pipeline_usage_string(pipeline, pipeline_path)
        if not pipeline_args:
            # user requests info about the pipeline
            print(pipeline_usage)
        else:
            pipeline_args = docopt(pipeline_usage, argv=argv)
            run(pipeline, pipeline_args)
    elif args["install"]:
        ref = args['<ref>']
        # TODO: make it work for individual tools
        pipeline = load_pipeline(ref)
        install(pipeline)


def run(pipeline, args):
    inputs = {}
    for i in args:
        if i.startswith('--'):
            inputs[i[2:]] = args[i]
    graph = JobGraph.from_pipeline(pipeline, runner_map=RUNNER_MAP)
    try:
        graph.simple_run(inputs, before_job=before_job, after_job=after_job)
    except RunFailed, e:
        print('Failed: %s' % e)
        raise e
    finally:
        present_outputs(graph.get_outputs())


def install(pipeline):
    graph = JobGraph.from_pipeline(pipeline, runner_map=RUNNER_MAP)
    graph.install_tools()


def main():
    logging.basicConfig(level=logging.DEBUG)
    parse()


if __name__ == '__main__':
    main()
