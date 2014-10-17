import mock

from nose.tools import raises, eq_

from docker.errors import APIError

from rabix.executors.container import Container

__test__ = False


def dockmock(**kwargs):
    docker = mock.Mock()
    docker.inspect_image = mock.Mock(return_value={'config': kwargs})
    docker.create_container_from_config = mock.Mock(
        return_value='container_id'
    )
    return docker


def test_init():
    docker = dockmock(Cmd=['cmd'])

    cont = Container(docker, 'image_id', mount_point='mount_point')

    eq_(cont.config['Image'], 'image_id')
    eq_(cont.config['Volumes'], {'mount_point': {}})
    eq_(cont.config['WorkingDir'], 'mount_point')
    eq_(cont.base_cmd, ['cmd'])

    docker.inspect_image.assert_called_with('image_id')


def test_run_simple():
    docker = dockmock(Cmd=['cmd'], Entrypoint=['ep'])

    cont = Container(docker, 'image_id')
    cont.binds = 'binds'
    cont.config = {'Image': 'image_id'}

    cont.run(['cmd'])

    docker.create_container_from_config.assert_called_with(
        {'Cmd': ['cmd'], 'Image': 'image_id'}
    )
    docker.start.assert_called_with(container='container_id', binds='binds')

    eq_(cont.container, 'container_id')


def test_run_override_no_ep():
    docker = dockmock(Cmd=['cmd'], Entrypoint=None)

    cont = Container(docker, 'image_id')
    cont.binds = 'binds'
    cont.config = {'Image': 'image_id'}

    cont.run(['cmd'], override_entrypoint=True)

    docker.create_container_from_config.assert_called_with(
        {'Cmd': ['cmd'], 'Image': 'image_id'}
    )
    docker.start.assert_called_with(container='container_id', binds='binds')

    eq_(cont.container, 'container_id')


def test_run_override_ep():
    docker = dockmock(Cmd=['cmd'], Entrypoint=['ep'])

    cont = Container(docker, 'image_id')
    cont.binds = 'binds'
    cont.config = {'Image': 'image_id'}

    cont.run(['cmd'], override_entrypoint=True)

    docker.create_container_from_config.assert_called_with(
        {'Cmd': [], 'Image': 'image_id', 'Entrypoint': ['cmd']}
    )
    docker.start.assert_called_with(container='container_id', binds='binds')

    eq_(cont.container, 'container_id')


def test_run_override_ep2():
    docker = dockmock(Cmd=['cmd'], Entrypoint=['ep'])

    cont = Container(docker, 'image_id')
    cont.binds = 'binds'
    cont.config = {'Image': 'image_id'}

    cont.run(['cmd', '--param'], override_entrypoint=True)

    docker.create_container_from_config.assert_called_with(
        {'Cmd': ['--param'], 'Image': 'image_id', 'Entrypoint': ['cmd']}
    )
    docker.start.assert_called_with(container='container_id', binds='binds')

    eq_(cont.container, 'container_id')


def test_run_missing_image():
    docker = mock.Mock()
    response = mock.Mock()
    response.status_code = 404
    docker.inspect_image = mock.Mock(
        side_effect=[APIError("", response),
                     {'config': {'Cmd': ['cmd'],
                                 'Entrypoint': ['ep']}}]
    )
    docker.create_container_from_config = mock.Mock(
        return_value='container_id'
    )
    docker.images = mock.Mock(return_value=[{'Id': 'image_id'}])

    cont = Container(docker, 'image_id')
    cont.binds = 'binds'
    cont.config = {'Image': 'image_id'}

    cont.run(['cmd'])

    eq_(cont.container, 'container_id')
    eq_(cont.config['Image'], 'image_id')
    docker.start.assert_called_with(container='container_id', binds='binds')
