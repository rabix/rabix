import json

from pkgutil import get_data
from jsonschema.validators import validator_for


def check_schema(schema):
    cls = validator_for(schema)
    cls.check_schema(schema)


class Schemas(object):

    def __init__(self):
        self.protocol = json.loads(get_data("spec", "protocol-schema.json"))
        self.validator = validator_for(self.protocol)(self.protocol)
        self.resolver = self.validator.resolver

    def add_schema(self, schema):
        pass
