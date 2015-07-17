import rabix
import rabix.common

from nose.tools import *
from rabix.common.util import *


def test_wrap():
    assert_equal(wrap_in_list(1, 2), [1, 2])
    assert_equal(wrap_in_list([], 1, 2), [1, 2])
    assert_equal(wrap_in_list([1, 2], 3, 4), [1, 2, 3, 4])
    assert_equal(wrap_in_list([1, 2], [3, 4]), [1, 2, [3, 4]])


def test_import_name():
    m = import_name('rabix')
    assert_equal(m, rabix)

    m = import_name('rabix.common')
    assert_equal(m, rabix.common)

    assert_raises(ImportError, import_name, 'rabix.badmodule')


def test_dot_update_dict():
    assert_equal(dot_update_dict({}, {}), {})
    assert_equal(dot_update_dict({"a": 5}, {}), {"a": 5})
    assert_equal(dot_update_dict({}, {"a": 5}), {"a": 5})
    assert_equal(dot_update_dict({"b": 4}, {"a": 5}), {"a": 5, "b": 4})
    assert_equal(dot_update_dict({"a": 4}, {"a": 5}), {"a": 5})
    assert_equal(dot_update_dict({"a": 4}, {"a": 5}), {"a": 5})

    assert_equal(dot_update_dict({"a": 4}, {"b.c": 5}),
                 {"a": 4, "b": {"c": 5}})

    assert_equal(dot_update_dict({"b": {"a": 4}}, {"b.c": 5}),
                 {"b": {"c": 5, "a": 4}})

    assert_equal(dot_update_dict({"b": {"a": 4}}, {"b": {"c": 5}}),
                 {"b": {"c": 5, "a": 4}})

    assert_equal(dot_update_dict({"b": {"a": 4}}, {"b.c": {"d": 5}}),
                 {"b": {"c": {"d": 5}, "a": 4}})


def test_rnd_name():
    syllables = 6
    name = rnd_name(syllables)
    assert isinstance(name, six.string_types)
    assert_equal(len(name), syllables * 2)


def test_log_level():
    assert_equals(log_level(-5), logging.WARN)
    assert_equals(log_level(0), logging.WARN)
    assert_equals(log_level(1), logging.INFO)
    assert_equals(log_level(2), logging.DEBUG)
    assert_equals(log_level(99), logging.DEBUG)
    assert_raises(RabixError, log_level, 0.5)


def test_sec_files_naming_conv():
    assert_equals(sec_files_naming_conv("bla.ext", '.a'), "bla.ext.a")
    assert_equals(sec_files_naming_conv("bla.ext", '^.b'), "bla.b")
