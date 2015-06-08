import six
from rabix.common.errors import RabixError
from rabix.common.models import Process
from rabix.expressions.evaluator import Evaluator


class ExpresionTool(Process):

    def __init__(self, process_id, inputs, outputs, requirements, hints,
                 script, context, engine, label, description):
        super(ExpresionTool, self).__init__(
            process_id, inputs, outputs,
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

        result = self.evaluator.evaluate(lang, expr, job.to_primitive(self.context), None)
        return self.construct_outputs(result)

    def to_dict(self, context):
        d = super(ExpresionTool, self).to_dict(context)
        d.update({
            "class": "ExpressionTool",
            "script": self.script,
            "engine": self.engine
        })
        return d

    @classmethod
    def from_dict(cls, context, d):
        converted = {k: context.from_dict(v) for k, v in six.iteritems(d)}
        kwargs = Process.kwarg_dict(converted)
        kwargs.update({
            'script': d['script'],
            'engine': d['engine'],
            'context': context
        })
        return cls(**kwargs)
