
class Expression(object):

    def __init__(self, engine, script):
        self.engine = engine
        self.script = script

    def to_dict(self, context):
        return {
            'engine': context.to_primitive(self.engine),
            'script': self.script
        }

    @classmethod
    def from_dict(cls, context, d):
        cls(d['engine'], d['script'])
