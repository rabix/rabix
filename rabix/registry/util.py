import os
import json
import functools

from flask import g, jsonify, Response, request, send_from_directory

from rabix.common.loadsave import loader, to_json
from rabix.common.errors import ValidationError
from rabix.runtime.models import App


class ApiError(RuntimeError):
    def __init__(self, code, message):
        self.code = code
        self.message = message


class ApiView(object):
    def __init__(self, login_required=False):
        self.login_required = login_required

    def __call__(self, func):
        @functools.wraps(func)
        def decorated(*args, **kwargs):
            if self.login_required and not g.user:
                raise ApiError(403, 'Login required.')
            if not g.json_api:
                return respond_with_client()
            resp = func(*args, **kwargs)
            if isinstance(resp, dict):
                return jsonify(**resp)
            return resp
        return decorated


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


def respond_with_client():
    static = os.path.join(os.path.dirname(__file__), 'static')
    return send_from_directory(static, 'index.html')
