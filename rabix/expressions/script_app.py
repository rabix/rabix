import six

from uuid import uuid4

from rabix.common.errors import RabixError
from rabix.common.models import App
from rabix.schema import IOSchema
from rabix.expressions.evaluator import Evaluator


class ScriptApp(App):

    def __init__(self, app_id, inputs, outputs, script,
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

    def run(self, job):
        if isinstance(self.script, six.string_types):
            lang = 'javascript'
            expr = self.script
        elif isinstance(self.script, dict):
            lang = self.script['lang']
            expr = self.script['value']
        else:
            raise RabixError("invalid script")

        result = self.evaluator.evaluate(lang, expr, job.to_dict(), None)
        return result

    def to_dict(self):
        d = super(ScriptApp, self).to_dict()
        d.update({
            'script': self.script
        })
        return d

    @classmethod
    def from_dict(cls, context, d):
        return cls(d.get('@id', str(uuid4())),
                   IOSchema(d['inputs']),
                   IOSchema(d['outputs']),
                   d['script'],
                   app_description=d.get('appDescription'),
                   annotations=d.get('annotations'),
                   platform_features=d.get('platform_features'))


def init(context):
    context.add_type('Script', ScriptApp.from_dict)
