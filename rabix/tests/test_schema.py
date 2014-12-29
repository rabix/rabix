import nose
import six

from nose.tools import *

from rabix.schema import JsonSchema
from rabix.main import init_context
from rabix.common.errors import RabixError


def test_simple_json_schema():
    schema = {
        "type": "object",
        "properties": {
            "a": {
                "type": "integer"
            }
        }
    }

    js = JsonSchema(init_context(), schema)
    a = next(iter(js))
    assert_equal(a.id, 'a')
    assert_equal(a.constructor, int)
    assert_false(a.required)

    to_dict = js.to_dict()
    for k, v in six.iteritems(schema):
        assert_equal(to_dict[k], v)


def test_invalid_json_schema():
    ctx = init_context()

    no_type = {
        "properties": {
            "a": {
                "type": "integer"
            }
        }
    }
    assert_raises(RabixError, JsonSchema, ctx, no_type)

    not_object = {
        "type": "int",
        "properties": {
            "a": {
                "type": "integer"
            }
        }
    }
    assert_raises(RabixError, JsonSchema, ctx, not_object)

    no_properties = {
        "type": "object",
    }
    assert_raises(RabixError, JsonSchema, ctx, no_properties)


if __name__ == '__main__':
    nose.run()
