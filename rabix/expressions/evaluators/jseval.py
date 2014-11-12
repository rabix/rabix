import json

import execjs

from rabix.expressions import evaluator


class JSEval(evaluator.ExpressionEvalPlugin):

    def __init__(self):
        super(JSEval, self).__init__()

    def evaluate(self, expression=None, job=None, context=None, *args,
                 **kwargs):
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
        return execjs.eval(exp)
