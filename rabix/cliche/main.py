#!/usr/bin/env python

import json
import argparse

from rabix.cliche.ref_resolver import from_url
from rabix.cliche.adapter import Adapter


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("tool", type=str)
    parser.add_argument("job_order", type=str)
    parser.add_argument("--conformance-test", action="store_true")
    args = parser.parse_args()

    print json.dumps(Adapter(from_url(args.tool)).get_shell_args(from_url(args.job_order)))


if __name__ == "__main__":
    main()
