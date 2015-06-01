from rabix.common.errors import RabixError
from rabix.common.models import Process
from rabix.expressions.evaluator import Evaluator


class ExpresionTool(Process):

    def __init__(self, app_id, inputs, outputs, requirements, hints, script,
                 context, engine, label, description):
        super(ExpresionTool, self).__init__(
            app_id, inputs, outputs,
            hints=hints,
            requirements=requirements,
            label=label,
            description=description
        )
        self.script = script
        self.evaluator = Evaluator()
        self.context = context
        self.engine = engine


    def run(self, job):
        if isinstance(self.script, dict):
            lang = self.script['engine']
            expr = self.script['script']
        else:
            raise RabixError("invalid script")

        result = self.evaluator.evaluate(lang, expr, job.to_dict(self.context), None)
        return self.construct_outputs(result)

    def to_dict(self, context):
        d = super(ExpresionTool, self).to_dict(context)
        d.update({
            "class": "ExpressionTool",
            "script": self.script,
            "engine": "javascript"
        })
        return d

    @classmethod
    def from_dict(cls, context, d):
        return cls(d['id'],
                   inputs=context.from_dict(d['inputs']),
                   outputs=context.from_dict(d['outputs']),
                   requirements=context.from_dict(d.get('requirements'), []),
                   hints=context.from_dict(d.get('hints'), []),
                   script=d['script'],
                   engine=d['engine'],
                   context=context,
                   label=d.get('label'),
                   description=d.get('description'))
