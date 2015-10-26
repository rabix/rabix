import json
import logging

import execjs

from rabix.expressions import evaluator

log = logging.getLogger(__name__)


class JSEval(evaluator.ExpressionEvalPlugin):

    def __init__(self):
        super(JSEval, self).__init__()

    def evaluate(self, expression=None, job=None, context=None, *args,
                 **kwargs):
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
        # log.debug("exec code: %s" % exp)

        result = execjs.eval(exp)
        log.debug("Expression result: %s" % result)
        return result
