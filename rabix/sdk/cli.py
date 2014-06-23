import os
import sys
import logging
import argparse

from rabix.common import six
from rabix.common.loadsave import from_url, to_json
from rabix.common.protocol import WrapperJob, JobError
from rabix.common.util import import_name, get_import_name
from rabix.sdk.wrapper import Wrapper, WrapperRunner

log = logging.getLogger(__name__)


def create_parser():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    sch = subparsers.add_parser(
        'schema', help='Print schema for all wrappers on stdout'
    )
    sch.set_defaults(cmd_func=cmd_schema)
    sch.add_argument('--package', help='Look for wrappers in this package.')
    sch.add_argument('--output', help='Print schema to this file.', default='')

    run = subparsers.add_parser('run', help='Run wrapper job.')
    run.set_defaults(cmd_func=cmd_run)
    run.add_argument('--cwd', default='.', help='cd here before running job.')
    run.add_argument('-i', '--input', default='__in__.json',
                     help='JSON file that contains arguments for wrapper job.')
    run.add_argument('-o', '--output', default='__out__.json',
                     help='Where to write the job result.')

    return parser


def get_wrapper_schema_list(package_name):
    package = import_name(package_name)
    package_contents = [getattr(package, var) for var in dir(package)]
    wrapper_cls_list = [
        obj for obj in package_contents
        if isinstance(obj, type) and issubclass(obj, Wrapper)
    ]
    return [
        dict(schema=cls._get_schema(), wrapper_id=get_import_name(cls))
        for cls in wrapper_cls_list
    ]


def cmd_schema(package, output, **_):
    schema_list = get_wrapper_schema_list(package)
    if output:
        with open(output, 'w') as fp:
            to_json(schema_list, fp)
    else:
        print(to_json(schema_list))


def cmd_run(cwd, input, output, **_):
    if not os.path.isdir(cwd):
        raise ValueError('No such directory: %s', cwd)
    os.chdir(cwd)

    job = from_url(input)
    if not isinstance(job, WrapperJob):
        raise TypeError('Input JSON must describe a job.')

    try:
        result = WrapperRunner(job).exec_wrapper_job(job)
    except Exception as e:
        log.exception('Job failed:')
        result = JobError(six.text_type(e))

    with open(output, 'w') as fp:
        to_json(result, fp)


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
