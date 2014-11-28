import six
from jsonschema.validators import Draft4Validator
from rabix.common.models import IO


class IOSchema(object):

    def __init__(self, schema):
        self.schema = schema
        required = schema.get('required', [])
        self.io = [IO(k, 0, Draft4Validator(v), k in required)
                   for k, v in six.iteritems(schema['properties'])]

    def __iter__(self, *args, **kwargs):
        return self.io.__iter__(*args, **kwargs)

    def to_dict(self):
        return self.schema


def init(context):
    context.add_type('IOSchema', IOSchema)
