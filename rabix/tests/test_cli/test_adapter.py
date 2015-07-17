from rabix.cli.adapter import *

import mock
from nose.tools import *


def test_input_adapter():
    ev = mock.Mock()
    ev.resolve.side_effect = lambda x: x
    adapter = InputAdapter(5, ev, {})
    cli = adapter.arg_list()
    assert_equal(cli, ['5'])


def test_meta():
    ev = mock.Mock()
    meta("path", {}, ev, {})


def test_secondary_files():
    eval = ExpressionEvaluator({})
    sf = secondary_files("main_path", {"secondaryFiles": [".bai"]}, eval)
    assert_equal(os.path.basename(sf[0]['path']), 'main_path.bai')
