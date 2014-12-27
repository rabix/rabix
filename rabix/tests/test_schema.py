
import nose
import mock
from nose.tools import *

from rabix.schema import JsonSchema
from rabix.main import init_context


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


def test_invalid_json_schema():
    pass

if __name__ == '__main__':
    nose.run()
