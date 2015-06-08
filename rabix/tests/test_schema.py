import nose
import six

from nose.tools import *

from rabix.main import init_context
from rabix.common.errors import RabixError
from rabix.common.models import make_constructor


def test_simple_avro_schema():
    schema = {
        "type": "record",
        "name": "Rec",
        "fields": [{
            "name": "a",
            "type": "int"
        }]
    }
    ctx = init_context()
    cons = make_constructor(schema)
    a = next(iter(cons.fields))
    assert_equal(a.name, 'a')
    assert_equal(a.constructor.name, 'int')
    assert_false(a.required)

    to_dict = js.to_primitive()
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
