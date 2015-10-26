import nose
import six

from nose.tools import *

from rabix.common.models import make_avro


def test_simple_avro_schema():
    schema = {
        "type": "record",
        "name": "Rec",
        "fields": [{
            "name": "a",
            "type": "int"
        }]
    }
    cons = make_avro(schema, []).schemas[0]
    a = next(iter(cons.fields))
    assert_equal(a.name, 'a')
    assert_equal(a.type.type, 'int')

    to_dict = cons.to_json()
    for k, v in six.iteritems(schema):
        assert_equal(to_dict[k], v)


if __name__ == '__main__':
    nose.run()
