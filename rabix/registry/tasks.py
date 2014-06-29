import os
import random
import shutil
import time
import logging

from rabix.registry.store import RethinkStore

log = logging.getLogger(__name__)


def build_task(build_id, mock=False):
    store = RethinkStore()
    build = store.get_build(build_id)
    repo = store.get_repo(build['repo'])
    user = store.get_user(repo['created_by'])
    token = user['token']
    os.mkdir(build_id)
    os.chdir(build_id)
    try:
        build['status'] = mock_build(build) if mock else ''
        store.update_build(build)
        if not mock:
            update_github_status(build, token)
    except:
        log.exception('Build error')
        os.chdir('..')
        raise
    else:
        os.chdir('..')
        shutil.rmtree(build_id)


def mock_build(build):
    with open('../%s.log' % build['id'], 'w') as fp:
        for i in range(10):
            fp.write('%s\n' % i)
            time.sleep(2)
    return random.choice(['success', 'failure'])


def update_github_status(build, _):
    log.info('Updating status for %s', build)
