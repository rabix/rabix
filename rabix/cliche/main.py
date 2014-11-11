#!/usr/bin/env python

import json
import argparse
import six

from rabix.cliche.ref_resolver import from_url
from rabix.cliche.adapter import Adapter


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("tool", type=str)
    parser.add_argument("job_order", type=str)
    parser.add_argument("--conformance-test", action="store_true")
    args = parser.parse_args()

    tool = from_url(args.tool)
    job = from_url(args.job_order)
    adapter = Adapter(tool)
    base_args = adapter._base_args(job)
    args, stdin = adapter._arg_list_and_stdin(job)
    stdout = adapter._get_stdout_name(job)
    print(json.dumps({
        'args': map(six.text_type, base_args + args),
        'stdin': stdin,
        'stdout': stdout,
    }))


if __name__ == "__main__":
    main()
