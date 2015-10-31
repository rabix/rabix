import six
from rabix.common.models import Process, InputParameter, OutputParameter
from rabix.expressions.evaluator import ExpressionEvaluator


class ExpressionTool(Process):

    def __init__(self, process_id, inputs, outputs, requirements, hints,
                 script, context, engine, label, description):
        super(ExpressionTool, self).__init__(
            process_id, inputs, outputs,
            hints=hints,
            requirements=requirements,
            label=label,
            description=description
        )
        self.script = script
        self.context = context
        self.engine = engine

    def run(self, job):
        self.add_content(job)
        result = ExpressionEvaluator.evaluate(self.engine, self.script, job.to_dict(self.context), None)
        return result

    def to_dict(self, context):
        d = super(ExpressionTool, self).to_dict(context)
        d.update({
            "class": "ExpressionTool",
            "expression": {
                "script": self.script,
                "engine": self.engine
            }
        })
        return d

    @classmethod
    def from_dict(cls, context, d):
        converted = {k: context.from_dict(v) for k, v in six.iteritems(d)}
        kwargs = Process.kwarg_dict(converted)
        kwargs.update({
            'script': d['expression']['script'],
            'engine': d['expression']['engine'],
            'context': context,
            'inputs': [InputParameter.from_dict(context, inp)
                       for inp in converted.get('inputs', [])],
            'outputs': [OutputParameter.from_dict(context, inp)
                        for inp in converted.get('outputs', [])]
        })
        return cls(**kwargs)
