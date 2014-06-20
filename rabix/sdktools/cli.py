import argparse
import sys
import logging

from os import getcwd
from os.path import abspath, join

from rabix.sdktools.build import init


log = logging.getLogger(__name__)


def create_parser():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    infest = subparsers.add_parser(
        'infest',
        help='Install Rabix adapter to docker image.')

    infest.set_defaults(cmd_func=cmd_infest)

    test = subparsers.add_parser(
        'test', help='Run wrapper job.')
    test.set_defaults(cmd_func=cmd_test)
    test.add_argument('--cwd', default='.',
                      help='cd here before running job.')

    init = subparsers.add_parser(
        'init', help='Initialize project')
    init.set_defaults(cmd_func=cmd_init)
    init.add_argument('path', default='.',
                      help='Where to initialize the project.')

    init.add_argument('base_image', default='ubuntu:14.04',
                      help='Docker image to be used as base.')
    init.add_argument('-f', '--force', default=False,
                      help='Overwrite existing files in target directory.')

    return parser


def cmd_infest():
    pass


def cmd_test():
    pass


def cmd_init(path, base_image, force=False):
    dst = abspath(join(getcwd(), path).rstrip('/'))
    init(dst, base_image)


def main():
    logging.basicConfig(level=logging.DEBUG)
    args = vars(create_parser().parse_args())
    try:
        args.pop('cmd_func')(**args)
    except Exception:
        log.exception("Internal error: %s", args)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
