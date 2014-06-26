import json
import hmac
import hashlib

from flask import request, g

from rabix.common.loadsave import loader, to_json
from rabix.common.errors import ValidationError
from rabix.models import App


class ApiError(RuntimeError):
    def __init__(self, code, message):
        self.code = code
        self.message = message


def validate_app(data):
    app = loader.classify(data)
    if not isinstance(app, dict):
        raise ApiError(400, 'Submit an object.')
    if not isinstance(app.get('repo'), basestring):
        raise ApiError(400, 'repo not specified or not a string')
    if not isinstance(app.get('name'), basestring):
        raise ApiError(400, 'name not specified or not a string')
    if not isinstance(app.get('description'), basestring):
        raise ApiError(400, 'description not specified or not a string')
    if not isinstance(app.get('app'), App):
        raise ApiError(400, 'not an app')
    try:
        app['app'].validate()
    except ValidationError as e:
        raise ApiError(400, unicode(e))
    result = json.loads(to_json(app))
    result['app_checksum'] = 'sha1$' + loader.checksum(result['app'])
    return result


def add_links(app):
    prefix = request.url_root + 'apps/'
    app['links'] = {
        'self': prefix + app['id'] + '?json',
        'app_ref': prefix + app['id'] + '?json#app',
        'html': prefix + app['id'],
    }
    return app


def verify_webhook(payload, signature, obj):
    obj = obj or json.load(payload)
    username = obj['repository']['owner']['name']
    repo_name = obj['repository']['name']
    repo_id = '/'.join([username, repo_name])
    secret = g.store.get_repo_secret(repo_id)
    calc_sig = hmac.new(secret, request.data, hashlib.sha1).hexdigest()
    return signature == 'sha1=' + calc_sig
