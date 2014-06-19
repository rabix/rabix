import argparse
import os
import sys
import logging


log = logging.getLogger(__name__)


def create_parser():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    sch = subparsers.add_parser(
        'infest',
        help='Install Rabix adapter to docker image.')
    sch.set_defaults(cmd_func=cmd_infest)
    sch.add_argument(
        '-i', '--image',
        help='Base image for this build.')

    sch.add_argument('--output', help='Print schema to this file.', default='')

    run = subparsers.add_parser(
        'test', help='Run wrapper job.')
    run.set_defaults(cmd_func=cmd_test)
    run.add_argument('--cwd', default='.', help='cd here before running job.')
    run.add_argument('-i', '--input', default='__in__.json', help='JSON file that contains arguments for wrapper job.')
    run.add_argument('-o', '--output', default='__out__.json', help='Where to write the job result.')

    return parser


def cmd_infest():
    pass


def cmd_test():
    pass


def main():
    logging.basicConfig(level=logging.DEBUG)
    args = vars(create_parser().parse_args())
    try:
        args['cmd_func'](**args)
    except Exception:
        log.exception("Internal error: %s", args)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
