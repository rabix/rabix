import os
import sys
import logging
from docopt import docopt, DocoptExit
from rabix.runtime import from_url
from rabix.runtime.graph import JobGraph, RunFailed
from rabix.runtime.runners import RUNNER_MAP
from rabix.runtime.cli_helpers import before_job, after_job, present_outputs

log = logging.getLogger(__name__)


class Runner(object):
    usage_string = """Usage:
    cli.py run <pipeline.json>"""

    def __init__(self):
        self._pipeline = None

    def _load_pipeline(self, path):
        """
        Takes a string path and returns a pipeline object obtained by parsing
        the JSON file indicated by the path
        """
        if self._pipeline:
            return self._pipeline
        if '://' in path:
            print 'Currently can only use local pipeline files'
            sys.exit(1)
        if not os.path.isfile(path):
            print 'Not a file', path
            sys.exit(1)
        self._pipeline = from_url(path)
        return self._pipeline

    def _make_pipeline_usage_string(self, pipeline_path):
        """
        Takes a pipeline path string and returns the usage string for it.
        As this is the usage string for a given pipeline, <pipeline.json>
        is replaced with the actual pipeline (for the 'run' cmd)
        """
        pipeline = self._load_pipeline(pipeline_path)
        inputs = pipeline.get_inputs()
        usage_str = self.usage_string.replace("run <pipeline.json>", "run " + pipeline_path) + " "
        options = "\n\nOptions:\n"
        for i in inputs.keys():
            arg = "--" + i + "=" + (i + "_file").upper()
            usage_str += arg if inputs[i]["required"] else "[" + arg + "]"
            usage_str += "... " if inputs[i]["list"] else " "
            options += '{0: <40}'.format(arg) + inputs[i]["description"] + "\n"
        usage_str += options
        return usage_str

    def parse(self, argv=None):
        """
        Read the array of command line arguments and determine if
        the user is requesting info about the pipeline or
        just wants to run the pipeline or wants to do something else.
        """
        argv = argv if argv else sys.argv[1:]
        try:
            args = docopt(self.usage_string, argv=argv)
            if args["run"] and args["<pipeline.json>"]:
                # user requests info about the pipeline
                print self._make_pipeline_usage_string(args["<pipeline.json>"])
        except DocoptExit as e:
            # user is attempting to run the pipeline
            if len(argv) > 2 and argv[0] == "run":
                args = docopt(self._make_pipeline_usage_string(argv[1]), argv=argv)
                inputs = {}
                for i in args:
                    if i.startswith('--'):
                        inputs[i[2:]] = args[i]
                graph = JobGraph.from_pipeline(self._load_pipeline(argv[1]), runner_map=RUNNER_MAP)
                try:
                    graph.simple_run(inputs, before_job=before_job, after_job=after_job)
                except RunFailed, e:
                    print 'Failed: %s' % e
                    raise e
                finally:
                    present_outputs(graph.get_outputs())
            else:
                raise e


def main():
    logging.basicConfig(level=logging.DEBUG)
    r = Runner()
    r.parse()


if __name__ == '__main__':
    main()
