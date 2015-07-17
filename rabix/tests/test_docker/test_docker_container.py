import mock
import rabix.docker.container

from nose.tools import *
from rabix.docker.container import *


def test_make_config():
    bla_cfg = make_config(bla=5)
    assert 'bla' not in bla_cfg
    assert 'Bla' not in bla_cfg

    cfg = make_config(hostname='my_host')
    assert 'Hostname' in cfg
    assert_equal(cfg['Hostname'], 'my_host')

    ep_cfg = make_config(entrypoint='my cmd')
    assert_equal(ep_cfg['Entrypoint'], ['my', 'cmd'])


def test_match_image():
    pass


def test_find_image():
    pass


def test_get_image():
    pass


def test_container():
    rabix.docker.container.get_image = lambda *args, **kwargs: "image_id"
    docker_client = mock.Mock()
    c = Container(docker_client, 'image_id', 'image_uri', 'cmd')


def test_container_start():
    pass


def test_container_commit():
    pass
