#!/usr/bin/env python

import cli
import json
import argparse

from cliche.ref_resolver import from_url, resolve_pointer

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("tool", type=str)
    parser.add_argument("job_order", type=str)
    parser.add_argument("--conformance-test", action="store_true")
    args = parser.parse_args()

    print json.dumps(cli.gen_cli(from_url(args.tool), from_url(args.job_order)))

if __name__ == "__main__":
    main()
