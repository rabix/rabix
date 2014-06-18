import pkg_resources
import argparse
import os
import sys
import logging

from rabix.common.loadsave import from_json, to_json
from rabix.common.protocol import WrapperJob, JobError
from rabix.common.util import import_name
from rabix.common.errors import ResourceUnavailable
from rabix.sdk.wrapper import Wrapper, WrapperRunner

log = logging.getLogger(__name__)


def create_parser():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    sch = subparsers.add_parser('schema', help='Print schema for all wrappers on stdout')
    sch.set_defaults(cmd_func=cmd_schema)
    sch.add_argument('--package', help='Look for wrappers in this package.')
    sch.add_argument('--output', help='Print schema to this file.', default='')

    run = subparsers.add_parser('run', help='Run wrapper job.')
    run.set_defaults(cmd_func=cmd_run)
    run.add_argument('--cwd', default='.', help='cd here before running job.')
    run.add_argument('-i', '--input', default='__in__.json', help='JSON file that contains arguments for wrapper job.')
    run.add_argument('-o', '--output', default='__out__.json', help='Where to write the job result.')

    return parser


def get_wrapper_schemas(package=None):
    group = 'sbgsdk.wrappers'
    map_id_class = {}
    classname = lambda cls: '.'.join([cls.__module__, cls.__name__])
    if package:
        pkg = import_name(package)
        for var in dir(pkg):
            obj = getattr(pkg, var)
            if isinstance(obj, type) and issubclass(obj, Wrapper):
                map_id_class[classname(obj)] = obj

    for entry_point in pkg_resources.iter_entry_points(group=group):
        wrp_cls = entry_point.load()
        full_class_name = classname(wrp_cls)
        map_id_class[full_class_name] = wrp_cls
    return [dict(schema=v._get_schema(), wrapper_id=k) for k, v in map_id_class.iteritems()]


def cmd_schema(output, **kwargs):
    sch = get_wrapper_schemas(kwargs.pop('package', None))
    if output:
        with open(output, 'w') as fp:
            to_json(sch, fp)
    else:
        print(to_json(sch))


def cmd_run(cwd, input, output, **_):
    if not os.path.isdir(cwd):
        raise ValueError('No such directory: %s', cwd)
    os.chdir(cwd)

    if not os.path.isfile(input):
        raise ResourceUnavailable('No such file: %s' % input)
    with open(input) as fp:
        job = from_json(fp)
    if not isinstance(job, WrapperJob):
        raise ValueError('Input JSON must describe a job.')

    try:
        result = WrapperRunner(job).exec_wrapper_job(job)
    except Exception as e:
        result = JobError(e.message or unicode(e))

    with open(output, 'w') as fp:
        to_json(result, fp)


def main():
    logging.basicConfig(level=logging.DEBUG)
    args = vars(create_parser().parse_args())
    try:
        args['cmd_func'](**args)
    except Exception:
        logging.exception("Internal error: %s", args)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())

