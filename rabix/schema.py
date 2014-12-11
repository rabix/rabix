import six
from jsonschema.validators import Draft4Validator
from rabix.common.models import IO


class JsonSchema(object):

    def __init__(self, context, schema):
        self.schema = schema
        self.schema['@type'] = 'JsonSchema'
        required = schema.get('required', [])
        self.io = [IO(context, k, v, constructor=v['type'],
                      required=k in required, annotations=v.get('adapter'),
                      items=v.get('items'))
                   for k, v in six.iteritems(schema['properties'])]

    def __iter__(self, *args, **kwargs):
        return self.io.__iter__(*args, **kwargs)

    def to_dict(self):
        return self.schema


def init(context):
    context.add_type('JsonSchema', JsonSchema)
