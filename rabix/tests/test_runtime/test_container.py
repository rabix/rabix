import mock

from nose.tools import raises, eq_

import rabix.common.six as six

from rabix.runtime.builtins.dockr import Container


def test_init():
    docker = mock.Mock()
    docker.inspect_image = mock.Mock(return_value={'config': {'Cmd': ['cmd']}})

    cont = Container(docker, 'image_id', mount_point='mount_point')

    eq_(cont.config['Image'], 'image_id')
    eq_(cont.config['Volumes'], {'mount_point': {}})
    eq_(cont.config['WorkingDir'], 'mount_point')
    eq_(cont.base_cmd, ['cmd'])

    docker.inspect_image.assert_called_with('image_id')


def test_run_simple():
    docker = mock.Mock()
    docker.inspect_image = mock.Mock(
        return_value={'config': {'Entrypoint': ['ep'], 'Cmd': ['cmd']}}
    )

    docker.create_container_from_config = mock.Mock(
        return_value='container_id'
    )

    cont = Container(docker, 'image_id', mount_point='mount_point')
    cont.binds = 'binds'
    cont.config = {'Image': 'image_id'}

    cont.run(['cmd'])

    docker.create_container_from_config.assert_called_with(
        {'Cmd': ['cmd'], 'Image': 'image_id'}
    )
    docker.start.assert_called_with(container='container_id', binds='binds')

    eq_(cont.container, 'container_id')


def test_run_override():
    pass
