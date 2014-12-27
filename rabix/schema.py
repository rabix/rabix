import six
from rabix.common.models import IO
from rabix.common.errors import RabixError


class JsonSchema(object):

    def __init__(self, context, schema):

        if 'type' not in schema:
            raise RabixError(
                "Invalid JSON schema: schema doesn't have a type.")

        if schema['type'] != 'object':
            raise RabixError("Invalid JSON schema: schema type isn't 'object'")

        if 'properties' not in schema:
            raise RabixError(
                "Invalid JSON schema: schema doesn't have properties")

        self.schema = schema
        self.schema['@type'] = 'JsonSchema'
        required = schema.get('required', [])
        self.io = [
            IO.from_dict(context, {
                '@id': k,
                'schema': v,
                'required': k in required,
                'annotations': v.get('adapter')
            })
            for k, v in six.iteritems(schema['properties'])
        ]

    def __iter__(self, *args, **kwargs):
        return self.io.__iter__(*args, **kwargs)

    def to_dict(self):
        return self.schema


def init(context):
    context.add_type('JsonSchema', JsonSchema)
