import six

from uuid import uuid4

from rabix.common.errors import RabixError
from rabix.common.models import App
from rabix.expressions.evaluator import Evaluator


class ScriptApp(App):

    def __init__(self, app_id, inputs, outputs, script, context,
                 app_description=None,
                 annotations=None,
                 platform_features=None):
        super(ScriptApp, self).__init__(
            app_id, inputs, outputs,
            app_description=app_description,
            annotations=annotations,
            platform_features=platform_features
        )
        self.script = script
        self.evaluator = Evaluator()
        self.context = context

    def run(self, job):
        if isinstance(self.script, six.string_types):
            lang = 'javascript'
            expr = self.script
        elif isinstance(self.script, dict):
            lang = self.script['lang']
            expr = self.script['value']
        else:
            raise RabixError("invalid script")

        result = self.evaluator.evaluate(lang, expr, job.to_dict(self.context), None)
        return result

    def to_dict(self, context):
        d = super(ScriptApp, self).to_dict(context)
        d.update({
            "@type": "Script",
            "script": self.script,
            "inputs": self.inputs.schema,
            "outputs": self.outputs.schema
        })
        return d

    @classmethod
    def from_dict(cls, context, d):
        return cls(d.get('@id', six.text_type(uuid4())),
                   context.from_dict(d['inputs']),
                   context.from_dict(d['outputs']),
                   d['script'],
                   context,
                   app_description=d.get('appDescription'),
                   annotations=d.get('annotations'),
                   platform_features=d.get('platform_features'))
