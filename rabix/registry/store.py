import re
import uuid
import operator
import logging

import rethinkdb as r

from rabix.common.errors import ResourceUnavailable

log = logging.getLogger(__name__)


class RethinkStore(object):
    def __init__(self, db_name='rabix_registry'):
        self.db_name = db_name
        self.cn = r.connect(
            host='localhost',
            port=28015,
            db=db_name,
        )
        self.db = r.db(db_name)
        self.apps = self.db.table('apps')
        self.users = self.db.table('users')
        self.builds = self.db.table('builds')
        self.repos = self.db.table('repos')

    def disconnect(self):
        self.cn.close()

    def init_db(self):
        if self.db_name not in r.db_list().run(self.cn):
            r.db_create(self.db_name).run(self.cn)
        if 'apps' not in self.db.table_list().run(self.cn):
            self.db.table_create('apps').run(self.cn)
        if 'users' not in self.db.table_list().run(self.cn):
            self.db.table_create('users', primary_key='username') \
                .run(self.cn)
        if 'builds' not in self.db.table_list().run(self.cn):
            self.db.table_create('builds').run(self.cn)
        if 'repos' not in self.db.table_list().run(self.cn):
            self.db.table_create('repos').run(self.cn)

    def _check_error(self, query_result):
        if query_result['errors']:
            raise RuntimeError(
                'Query failed: %s' % query_result['first_error']
            )

    def _build_text_query(self, terms, fields):
        terms = ['(?i)' + re.escape(term) for term in terms]
        q = r.expr(False)
        for field in fields:
            l = [r.row[field].match(term) for term in terms]
            q |= l[0] if len(l) == 1 else reduce(operator.and_, l)
        return q

    def get_user(self, username):
        return self.users.get(username).run(self.cn)

    def get_user_by_token(self, token):
        res = list(self.users.filter({'token': token}).run(self.cn))
        return res[0] if res else None

    def get_user_by_personal_token(self, token):
        res = list(self.users.filter({'personal_token': token}).run(self.cn))
        return res[0] if res else None

    def make_personal_token(self, username):
        token = str(uuid.uuid4())
        res = self.users.get(username).update({'personal_token': token})\
            .run(self.cn)
        self._check_error(res)
        return token

    def revoke_personal_token(self, username):
        res = self.users.get(username).update({'personal_token': None})\
            .run(self.cn)
        self._check_error(res)

    def create_or_update_user(self, user):
        if self.get_user(user['username']):
            res = self.users.get(user['username']).update(user).run(self.cn)
            self._check_error(res)
            return self.get_user(user['username'])
        self._check_error(self.users.insert(user).run(self.cn))

    def logout(self, username):
        res = self.users.get(username).update({'token': None}).run(self.cn)
        self._check_error(res)

    def insert_app(self, *documents):
        map(operator.methodcaller('pop', 'id', ''), documents)
        result = self.apps.insert(*documents).run(self.cn)
        log.debug('Insert result: %s', result)
        self._check_error(result)
        for app, key in zip(documents, result['generated_keys']):
            app['id'] = key

    def update_app(self, document):
        del document['app']
        del document['app_checksum']
        filter = {'repo': document['repo']}
        result = self.apps.get_all(document['id']). \
            filter(filter).update(document).run(self.cn)
        self._check_error(result)
        if not result['updated']:
            raise ResourceUnavailable(document['id'], 'Not found.')
        return self.get_app(document['id'])

    def get_app(self, app_id):
        return self.apps.get(app_id).run(self.cn)

    def filter_apps(self, filter, text=None, skip=0, limit=25):
        log.debug('Filter apps: %s', filter)
        q = self.apps.without('app').filter(filter)
        if text:
            terms = text.split(' ')
            q = q.filter(self._build_text_query(terms, ('name', 'description')))
        cur = q.skip(skip).limit(limit).run(self.cn)
        count = q.count().run(self.cn)
        return cur, count

    def create_build(self, build):
        build.pop('id', None)
        res = self.builds.insert(build).run(self.cn)
        self._check_error(res)
        build['id'] = res['generated_keys'][0]
        return build

    def update_build(self, build):
        res = self.builds.get(build['id']).update(build).run(self.cn)
        self._check_error(res)
        if not res['updated']:
            raise ResourceUnavailable(build['id'], 'Not found.')
        return self.get_build(build['id'])

    def get_build(self, build_id):
        return self.builds.get(build_id).run(self.cn)

    def filter_builds(self, filter, skip=0, limit=25):
        log.debug('Filter builds: %s', filter)
        q = self.builds.filter(filter)
        cur = q.skip(skip).limit(limit).run(self.cn)
        count = q.count().run(self.cn)
        return cur, count

    def create_repo(self, repo_id, username):
        repo = {
            'id': repo_id,
            'secret': str(uuid.uuid4()),
            'created_by': username,
        }
        return self.repos.insert(
            repo, upsert=True, return_vals=True
        ).run(self.cn)

    def get_repo_secret(self, repo_id):
        repo = self.repos.get(repo_id).run(self.cn)
        if not repo:
            raise ResourceUnavailable(repo_id, 'not found')
        return repo['secret']


if __name__ == '__main__':
    RethinkStore().init_db()
