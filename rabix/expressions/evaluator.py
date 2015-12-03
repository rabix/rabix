import json
import execjs
import logging
from rabix.common.errors import RabixError
from rabix.common.util import wrap_in_list

from rabix.common.ref_resolver import resolve_pointer

log = logging.getLogger(__name__)


class ExpressionEngine(object):

    def __init__(self, image, ids, f, engine_config=None):
        super(ExpressionEngine, self).__init__()
        self.image = image
        self.ids = ids
        self.f = f
        self.engine_config = engine_config

    def evaluate(self, expression, job, context=None, outdir=None, tmpdir=None):
        return self.f(expression, job, context, self.engine_config, outdir, tmpdir)


class Evaluator(object):

    def __init__(self, ctx=None, engines=None, default=None):
        self.ctx = ctx
        self.engines = engines or []
        self.default = default

    def get_engine_by_id(self, id):
        return next((e for e in self.engines if id in e.ids), self.default)

    def get_engine_by_image(self, image):
        return next((e for e in self.engines if image == e.image), self.default)

    def evaluate(self, engine, expression, job, context=None):
        pl = self.get_engine_by_id(engine)
        if not pl:
            raise Exception('No expression evaluator %s' % id)
        res = pl.evaluate(expression, job, context)
        if self.ctx:
            return self.ctx.from_dict(res)
        else:
            return res


def evaluate_rabix_js(expression, job, context=None,
                      engine_config=None, outdir=None, tmpdir=None):
    # log.debug("expression: %s" % expression)
    if expression.startswith('{'):
        exp_tpl = '''function () {
        $job = %s;
        $self = %s;
        return function()%s();}()
        '''
    else:
        exp_tpl = '''function () {
        $job = %s;
        $self = %s;
        return %s;}()
        '''
    exp = exp_tpl % (json.dumps(job), json.dumps(context), expression)

    result = execjs.eval(exp)
    log.debug("Expression result: %s" % result)
    return result


def evaluate_cwl_js(expression, job, context=None,
                    engine_config=None, outdir=None, tmpdir=None):
    # log.debug("expression: %s" % expression)
    if expression.startswith('{'):
        exp_tpl = '''
        {config}
        (function () {{
        $job = {job};
        $self = {context};
        return function(){f}();}})()
        '''
    else:
        exp_tpl = '''
        {config}
        (function () {{
        $job = {job};
        $self = {context};
        return {f};}})()
        '''
    config = ''
    if engine_config:
        config = '\n'.join(engine_config)

    j = {}
    j.update(job['inputs'])
    j['allocatedResources'] = job['allocatedResources']
    exp = exp_tpl.format(
        config=config,
        job=json.dumps(j),
        context=json.dumps(context),
        f=expression)

    exp_escaped = json.dumps(exp)
    result = execjs.eval("require('vm').runInNewContext(%s, {})" % exp_escaped)
    log.debug("Expression result: %s" % result)
    return result


def evaluate_json_ptr(expression, job, context=None,
                      engine_config=None, outdir=None, tmpdir=None):
    doc = {
        'job': job.get('inputs', {}),
        'context': context
    }
    return resolve_pointer(doc, expression)


ExpressionEvaluator = Evaluator()
ExpressionEvaluator.engines.extend([
    ExpressionEngine(
        'rabix/js-engine',
        {'#cwl-js-engine', 'javascript', 'cwl-js-engine'}, evaluate_rabix_js, []),
    ExpressionEngine(
        'commonworkflowlanguage/nodejs-engine',
        {'node-engine.cwl'}, evaluate_cwl_js, []),
    ExpressionEngine(
        None,
        {'cwl:JsonPointer'}, evaluate_json_ptr, [])
])


class ExpressionEngineRequirement(object):

    def __init__(self, id=None, docker_image=None, engine_config=None):
        self.id = id
        self.docker_image = docker_image
        self.engine_config = engine_config

    def to_dict(self, context=None):
        d = {
            "class": "ExpressionEngineRequirement",
            "id": self.id,
            "engineConfig": self.engine_config
        }

        if self.docker_image:
            d["requirements"] = [{
                "class": "DockerRequirement",
                "dockerImageId": self.docker_image
            }]

        return d

    @classmethod
    def from_dict(cls, context, d):
        id = d.get('id')
        ec = d.get('engineConfig')
        engine_config = wrap_in_list(ec) if ec else None
        docker_image = None
        for r in d.get('requirements', []):
            if r.get('class') == 'DockerRequirement':
                docker_image = r.get('dockerImageId', r.get('dockerPull'))

        return cls(id, docker_image, engine_config)


class ValueResolver(object):
    def __init__(self, job):
        self.job = job

    def resolve(self, expr_or_value, context=None):
        val = expr_or_value
        if not isinstance(val, dict) or ('engine' not in val and 'script' not in val):
            return val
        engine, script = val['engine'], val['script']
        return ExpressionEvaluator.evaluate(engine, script, self.job.to_dict(), context)


def update_engines(process):
    eer = process.get_requirement(ExpressionEngineRequirement)
    if not eer:
        return

    engine = None
    if eer.id:
        engine = ExpressionEvaluator.get_engine_by_id(eer.id)

    if not engine and eer.docker_image:
        engine = ExpressionEvaluator.get_engine_by_image(eer.docker_image)

    if not engine:
        raise RabixError("Unsupported expression engine: {}".format(
                         eer.id or eer.docker_image))

    engine.ids.add(eer.id)
    if eer.engine_config:
        engine.engine_config = eer.engine_config
