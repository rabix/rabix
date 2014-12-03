#!/usr/bin/env python

import json
import argparse
import os

from rabix.common.ref_resolver import from_url
from rabix.cli.adapter import CLIJob


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("tool", type=str)
    parser.add_argument("job_order", type=str)
    parser.add_argument("--conformance-test", action="store_true")
    parser.add_argument("--basedir", type=str)
    parser.add_argument("--no-container", action="store_true")
    args = parser.parse_args()

    tool = from_url(args.tool)
    job = from_url(args.job_order)

    def path_mapper(path):
        return os.path.normpath(os.path.join(args.basedir or '.', path))

    cli_job = CLIJob(job, tool, path_mapper)
    print(json.dumps({
        'args': cli_job.make_arg_list(),
        'stdin': cli_job.stdin,
        'stdout': cli_job.stdout,
    }))


if __name__ == "__main__":
    main()
