import logging
import functools
import re
import os
import random

from flask import Flask, request, g, session, redirect, jsonify
from flask.ext.github import GitHub, GitHubError

from rabix import CONFIG
from rabix.common.util import update_config
from rabix.common.errors import ResourceUnavailable
from rabix.registry.util import ApiError, validate_app, add_links
from rabix.registry.store import RethinkStore

if __name__ == '__main__':
    update_config()

dirname = os.path.abspath(os.path.dirname(__file__))
static_dir = os.path.join(dirname, CONFIG['registry']['STATIC_DIR'])
flapp = Flask(__name__, static_folder=static_dir, static_url_path='/static')
flapp.config.update(CONFIG['registry'])
log = flapp.logger
github = GitHub(flapp)
store = RethinkStore()
mock_user = {
    'avatar_url': 'https://avatars.githubusercontent.com/u/125295?',
    'gravatar_id': 'e2e353bd6284cc95fc98d3c1a2c358c5',
    'html_url': 'https://github.com/ntijanic',
    'login': '$mock',
    'name': 'Mock User',
}


class ApiView(object):
    def __init__(self, login_required=False):
        self.login_required = login_required

    def __call__(self, func):
        @functools.wraps(func)
        def decorated(*args, **kwargs):
            if self.login_required and not g.user:
                raise ApiError(403, 'Login required.')
            if not g.json_api:
                return flapp.send_static_file('index.html')
            resp = func(*args, **kwargs)
            if isinstance(resp, dict):
                return jsonify(**resp)
            return resp
        return decorated


@flapp.errorhandler(ApiError)
def error_handler(exc):
    resp = jsonify(message=exc.message)
    resp.status_code = exc.code
    return resp


@flapp.errorhandler(ResourceUnavailable)
def error_handler(exc):
    resp = jsonify(message=unicode(exc))
    resp.status_code = 404
    return resp


@flapp.errorhandler(500)
def server_error(exc):
    rnd = str(random.randint(0, 2 ** 42))
    log.error('%s: %s', rnd, exc)
    resp = jsonify(message='Server error. Mention %s in report.' % rnd)
    resp.status_code = 500
    return resp


@flapp.errorhandler(404)
def handle_404(*_):
    return error_handler(ApiError(404, 'Not found.'))\
        if g.json_api else flapp.send_static_file('index.html')


@flapp.errorhandler(400)
def handle_400(*_):
    return error_handler(ApiError(400, 'Bad request'))


@flapp.before_request
def before_request():
    g.store = RethinkStore()
    g.user = None
    auth = request.headers.get('authorization', '')
    match = re.match(R'^token ([\w\-_=]+)$', auth)
    if match:
        token = match.group(1)
        g.user = g.store.get_user_by_personal_token(token)
        if not g.user:
            raise ApiError(403, 'Invalid token.')
    if not g.user and 'username' in session:
        if not flapp.config.get('MOCK_USER'):
            g.user = g.store.get_user(str(session['username']))
        else:
            g.user = {'username': mock_user['login']}

    g.json_api = 'application/json' in request.headers.get('accept', '')\
        or 'application/json' in request.headers.get('content-type', '') \
        or 'json' in request.args


@flapp.teardown_request
def teardown_request(_):
    g.store.disconnect()


@github.access_token_getter
def token_getter():
    user = g.user
    if user is not None:
        return user.get('token')


@flapp.route('/github-callback')
@github.authorized_handler
def authorized(access_token):
    log.debug(access_token)
    next_url = request.args.get('next') or '/'
    if access_token is None:
        return redirect(next_url)

    user = store.get_user_by_token(access_token)
    if not user:
        g.user = user = {'token': access_token}
        resp = github.get('user')
        user.update({
            'username': resp['login'],
            'avatar_url': resp['avatar_url'],
            'name': resp['name'],
            'github_url': resp['html_url'],
        })
        store.create_or_update_user(user)
    session['username'] = user['username']
    return redirect('/')


@flapp.route('/login')
def login():
    if 'username' in session:
        return redirect('/')
    if flapp.config.get('MOCK_USER'):
        g.store.create_or_update_user({'username': mock_user['login']})
        session['username'] = mock_user['login']
        return redirect('/')
    return github.authorize()


@flapp.route('/logout', methods=['POST'])
def logout():
    username = session.pop('username', None)
    g.store.logout(username)
    return redirect('/')


@flapp.route('/user')
def user_info():
    if not g.user:
        return jsonify()
    if flapp.config.get('MOCK_USER'):
        return jsonify(**mock_user)
    try:
        return jsonify(**github.get('user'))
    except GitHubError:
        log.exception('Failed getting user info.')
        return jsonify()


@flapp.route('/', methods=['GET'])
@ApiView()
def index():
    return {}


@flapp.route('/apps', methods=['GET'])
@ApiView()
def apps_index():
    query = {k: v for k, v in request.args.iteritems()}
    skip = query.pop('skip', 0)
    limit = query.pop('limit', 25)
    query.pop('json', None)
    return {'items': map(add_links, store.filter_apps(query, skip, limit))}


@flapp.route('/search', methods=['GET'])
@ApiView()
def apps_search():
    skip = request.args.get('skip', 0)
    limit = request.args.get('limit', 25)
    terms = request.args.getlist('term', [])
    terms += request.args.get('terms', '').split(' ')
    terms = filter(None, terms)
    return {'items': map(add_links, store.search_apps(terms, skip, limit))}


@flapp.route('/apps/<app_id>', methods=['GET'])
@ApiView()
def app_get(app_id):
    app = store.get_app(app_id)
    if not app:
        raise ApiError(404, 'Not found.')
    return add_links(app)


@flapp.route('/apps', methods=['POST'])
@ApiView(login_required=True)
def apps_insert():
    data = request.get_json(True)
    app = validate_app(data)
    if not data.get('repo', '').startswith(g.user['username']):
        raise ApiError(401, 'Not your repo.')
    store.insert_app(app)
    return add_links(app)


@flapp.route('/apps/<app_id>', methods=['PUT'])
@ApiView(login_required=True)
def app_update(app_id):
    data = request.get_json(True)
    data['id'] = app_id
    if not data.get('repo', '').startswith(g.user['username']):
        raise ApiError(401, 'Not your repo.')
    return add_links(store.update_app(data))


@flapp.route('/token', methods=['GET', 'POST', 'DELETE'])
@ApiView(login_required=True)
def token_crud():
    token = None
    if request.method == 'GET':
        token = g.user.get('personal_token')
    elif request.method == 'POST':
        token = g.store.make_personal_token(g.user['username'])
    elif request.method == 'DELETE':
        g.store.revoke_personal_token(g.user['username'])
    return jsonify(token=token)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    flapp.run(port=4280)
