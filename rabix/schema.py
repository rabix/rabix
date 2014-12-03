import six
from jsonschema.validators import Draft4Validator
from rabix.common.models import IO


class JsonSchema(object):

    def __init__(self, context, schema):
        self.schema = schema
        self.schema['@type'] = 'JsonSchema'
        required = schema.get('required', [])
        self.io = [IO(k, 0, Draft4Validator(v), k in required)
                   for k, v in six.iteritems(schema['properties'])]

    def __iter__(self, *args, **kwargs):
        return self.io.__iter__(*args, **kwargs)

    def to_dict(self):
        return self.schema


def init(context):
    context.add_type('JsonSchema', JsonSchema)
