import yaml
from jsonschema.validators import Draft4Validator
from cliche.ref_resolver import from_url


def load(path):
    with open(path) as fp:
        return yaml.load(fp)


META_SCHEMA = load('metaschema.json')
TOOL_SCHEMA = load('tool.json')


def validate_schema(schema):
    Draft4Validator.check_schema(schema)


def validate_tool(tool):
    Draft4Validator(TOOL_SCHEMA).validate(tool)
    Draft4Validator(META_SCHEMA).validate(tool['inputs'])
    Draft4Validator(META_SCHEMA).validate(tool['outputs'])


def validate_all():
    validate_schema(TOOL_SCHEMA)
    validate_schema(META_SCHEMA)
    validate_tool(from_url('../examples/bwa-mem.yml')['tool'])
    validate_tool(from_url('../examples/tmap.yml')['mapall'])


if __name__ == '__main__':
    validate_all()