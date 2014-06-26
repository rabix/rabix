import multiprocessing
import os
import logging

import docopt

from rabix import __version__
from rabix.common.util import rnd_name

log = logging.getLogger(__name__)


USAGE = """
Usage: run_workers.py [--help] [options]

Creates supervisor.conf file and runs supervisord.
The supervisor keeps track of RQ worker processes.
The redis password will be reused as supervisord password.

Options:
  --node-id=<str>           ID for this node. Randomly generated if unspecified.
  --num-workers=<int>       Number of workers. Defaults to CPU_COUNT+1.
  --redis-host=<address>    Redis host or IP [default: localhost].
  --redis-port=<int>        Redis port [default: 6379].
  --redis-password=<str>    Redis password.
  -v --verbose              Log level set to DEBUG
  -h --help                 Display this message.
  --version                 Print version to stdout and quit.
"""

SUPERVISOR_CONF = """
[supervisord]
logfile=supervisord.log
logfile_maxbytes=50MB
logfile_backups=10
loglevel=info
pidfile=supervisord.pid
identifier=rabix
nocleanup=true

[inet_http_server]
port=127.0.0.1:9001
username=rabix
{password_line}

[supervisorctl]
serverurl=unix:///tmp/supervisor.sock
username=rabix
{password_line}
prompt=supervisor

[program:worker]
command={command}
process_name=%(program_name)s-%(process_num)s
numprocs={num_workers}
autostart=true
autorestart=true
redirect_stderr=true
stdout_logfile={node_id}-%(program_name)s-%(process_num)s.log
stdout_logfile_maxbytes=1MB
stdout_logfile_backups=10
stdout_capture_maxbytes=1MB
"""

WORKER_CMD = ' '.join([
             'rqworker rabix-{node_id}',
             '--host {redis_host}',
             '--port {redis_port}',
             '{password_arg}',
             '--name %(program_name)s-{node_id}-%(process_num)s'])


def main():
    args = docopt.docopt(USAGE, version=__version__)
    if args['--verbose']:
        logging.root.setLevel(logging.DEBUG)
    password = args['--redis-password'] or ''
    conf = {
        'node_id': args['--node-id'] or rnd_name(),
        'num_workers': args['--num-workers'] or multiprocessing.cpu_count() + 1,
        'redis_host': args['--redis-host'] or 'localhost',
        'redis_port': int(args['--redis-port']) or 6379,
        'password_line': 'password={}'.format(*password) if password else '',
        'password_arg': '--password {}'.format(*password) if password else '',
    }
    conf['command'] = WORKER_CMD.format(**conf)
    with open('supervisord.conf', 'w') as fp:
        fp.write(SUPERVISOR_CONF.format(**conf))

    print('Node ID: {node_id}'.format(**conf))
    print('Using redis at {redis_host}:{redis_port}'.format(**conf))
    print('Running supervisord.')

    os.system('supervisord')


if __name__ == '__main__':
    main()
