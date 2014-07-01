import os
import random
import shutil
import time
import logging
import json

import requests

from rabix.registry.store import RethinkStore

log = logging.getLogger(__name__)


def build_task(build_id, mock=False):
    store = RethinkStore()
    build = store.get_build(build_id)
    repo = store.get_repo(build['repo'])
    user = store.get_user(repo['created_by'])
    token = user['token']
    cwd = os.path.abspath('.')
    os.mkdir(build_id)
    os.chdir(build_id)
    try:
        build['status'] = mock_build(build) if mock else do_build(build)
        store.update_build(build)
        if not mock:
            update_github_status(build, token)
    except:
        log.exception('Build error')
        os.chdir(cwd)
        raise
    else:
        os.chdir(cwd)
        os.system('docker run --rm -v $(pwd):/mnt:rw busybox rm -rf /mnt/%s' % build_id)


def mock_build(build):
    with open('../%s.log' % build['id'], 'w') as fp:
        for i in range(10):
            fp.write('%s\n' % i)
            time.sleep(2)
    return random.choice(['success', 'failure'])


def do_build(build):
    owner, name = build['repo'].split('/')
    sha = build['head_commit']['id']
    build_id = build['id']
    cmd_clone = 'git clone https://github.com/%s/%s' % (owner, name)
    cmd_cd = 'cd %s' % name
    cmd_checkout = 'git checkout %s' % sha
    cmd_run = 'rabix-sdk build'
    cmd_list = cmd_clone, cmd_cd, cmd_checkout, cmd_run
    log_file = '../%s.log' % build_id
    cmd = '(' + '&&'.join(cmd_list) + ') 1>%s 2>&1' % log_file
    return 'failure' if os.system(cmd) else 'success'


def update_github_status(build, token):
    log.info('Updating status for %s', build)
    endpoint = 'https://api.github.com/repos/%s/statuses/%s' % (
        build['repo'], build['head_commit']['id']
    )
    status = {
        'state': build['status'],
        'context': 'continuous-integration/rabix',
        'description': build['status'],
        'target_url': build['target_url'],
    }
    headers = {'Authorization': 'token %s' % token}
    r = requests.post(endpoint, data=json.dumps(status), headers=headers)
    r.raise_for_status()
