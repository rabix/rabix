import logging
import logging.config
import functools
import re
import os
import json
import sys
import random

import rq
import redis
from flask import Flask, request, g, session, redirect, jsonify, Response, \
    send_from_directory
from flask.ext.github import GitHub, GitHubError

from rabix import CONFIG
from rabix.common.util import update_config
from rabix.common.errors import ResourceUnavailable
from rabix.registry.tasks import build_task
from rabix.registry.util import ApiError, validate_app, add_links, \
    verify_webhook, get_query_args
from rabix.registry.store import RethinkStore

if __name__ == '__main__':
    update_config()

dirname = os.path.abspath(os.path.dirname(__file__))
static_dir = os.path.join(dirname, CONFIG['registry']['STATIC_DIR'])
flapp = Flask(__name__, static_folder=static_dir, static_url_path='')
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
    return github.authorize(scope='repo:status read:org')


@flapp.route('/logout', methods=['POST'])
def logout():
    username = session.pop('username', None)
    g.store.logout(username)
    return jsonify()


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
    return jsonify()


@flapp.route('/apps', methods=['GET'])
@ApiView()
def apps_index():
    filter, skip, limit = get_query_args()
    text = request.args.get('q')
    apps, total = store.filter_apps(filter, text, skip, limit)
    return {'items': map(add_links, apps), 'total': total}


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


@flapp.route('/github-webhook', methods=['POST'])
def handle_event():
    event = request.get_json()
    delivery_id = request.headers.get('X-Github-Delivery', '')
    event_type = request.headers.get('X-Github-Event', '')
    signature = request.headers.get('X-Hub-Signature', '')
    log.debug('%s:%s:%s:%s', delivery_id, event_type, signature, event)
    log.info('Webhook: %s:%s', event_type, delivery_id)
    if event_type != 'push':
        return jsonify(status='ignored')
    mock = flapp.config.get('MOCK_USER', False)
    if not mock and not verify_webhook(request.data, signature, event):
        raise ApiError(403, 'Failed to verify HMAC.')
    # Submit task
    cmt = event.get('head_commit')
    if not cmt:
        return jsonify(status='ignored')
    repo = event['repository']['owner']['name'], event['repository']['name']
    repo = '/'.join(repo)
    build = {
        'head_commit': cmt,
        'pusher': event.get('pusher', {}).get('name'),
        'status': 'pending',
        'repo': repo,
    }
    build = g.store.create_build(build)
    target_url = request.url_root + 'builds/' + build['id']
    build['target_url'] = target_url
    g.store.update_build(build)
    queue = rq.Queue('builds', 600, redis.Redis())
    queue.enqueue(build_task, build['id'], mock)
    res = 'repo/%s/statuses/%s' % (repo, cmt['id'])
    status = {
        'state': 'pending',
        'context': 'continuous-integration/rabix',
        'description': 'Build pending.',
        'target_url': target_url,
    }
    log.info('New status: %s', status)
    if not mock:
        github.put(res, data=json.dumps(status))
    return jsonify(status='ok', build_id=build['id'])


@flapp.route('/github-repos', methods=['GET'])
@ApiView(login_required=True)
def list_github_repos():
    repos = github.get('user/%s/repos' % g.user['username'])
    repos_short = [{
        'id': repo['full_name'],
        'html_url': repo['html_url'],
    } for repo in repos]
    return jsonify(items=repos_short)


@flapp.route('/repos', methods=['GET'])
@ApiView()
def repo_index():
    filter, skip, limit = get_query_args()
    repos, total = store.filter_repos(filter, skip, limit)
    return {'items': list(repos), 'total': total}


@flapp.route('/repos/<owner>/<name>', methods=['GET'])
@ApiView()
def get_repo(owner, name):
    repo_id = '/'.join([owner, name])
    return jsonify(**g.store.get_repo(repo_id))


@flapp.route('/repos/<owner>/<name>', methods=['PUT'])
@ApiView(login_required=True)
def put_repo(owner, name):
    username = g.user['username']
    if username != owner:
        raise ApiError(403, 'You can only setup repos owned by you.')
    repo_id = '/'.join([owner, name])
    repo = g.store.create_repo(repo_id, username)
    return jsonify(**repo)


@flapp.route('/builds', methods=['GET'])
@ApiView()
def build_index():
    filter, skip, limit = get_query_args()
    builds, total = store.filter_builds(filter, skip, limit)
    return {'items': list(builds), 'total': total}


@flapp.route('/builds/<build_id>', methods=['GET'])
@ApiView()
def get_build(build_id):
    return jsonify(**g.store.get_build(build_id))


@flapp.route('/builds/<build_id>/log', methods=['GET'])
@ApiView()
def get_build_log(build_id):
    builds_dir = flapp.config['BUILDS_DIR']
    log_file = os.path.join(builds_dir, build_id + '.log')
    if not os.path.isfile(log_file):
        return Response('', content_type='text/plain')
    range_header = request.headers.get('range', '')
    if not range_header:
        return send_from_directory(builds_dir, build_id + '.log')
    try:
        range = map(int, range_header[len('bytes='):].split('-'))
        assert len(range) == 2
        assert range[1] - range[0] > 0
        assert os.path.getsize(log_file) >= range[0] + range[1]
    except:
        raise ApiError(416, 'Invalid range.')
    with open(log_file) as fp:
        fp.seek(range[0])
        content = fp.read(range[1] - range[0])
        return Response(content, content_type='text/plain')


if __name__ == '__main__':
    if len(sys.argv) > 1:
        with open(sys.argv[1]) as fp:
            logging.config.dictConfig(json.load(fp))
    else:
        logging.basicConfig(level=logging.DEBUG)
    flapp.run(port=4280)
