import mock

from nose.tools import raises, eq_

import rabix.sdktools.steps as steps
from rabix.common.errors import RabixError


@mock.patch('rabix.sdktools.steps.Container')
def test_run_ok(container_mock):
    container_mock().is_success = mock.Mock(return_value=True)

    steps.run('docker_client', 'image_id', ['cmd'])

    assert_run(container_mock)


@raises(RabixError)
@mock.patch('rabix.sdktools.steps.Container')
def test_run_fail(container_mock):
    container_mock().is_success = mock.Mock(return_value=False)

    try:
        steps.run('docker_client', 'image_id',
                  ['cmd'], mount_point=steps.MOUNT_POINT)
    finally:
        assert_run(container_mock)


def assert_run(container):
    container.assert_called_with('docker_client', 'image_id',
                                 {}, mount_point=steps.MOUNT_POINT)
    instance = container()
    instance.run.assert_called_with(['cmd'])
    instance.is_success.assert_called_with()


@mock.patch('rabix.sdktools.steps.Container')
def test_build_ok(container_mock):
    c = container_mock()
    c.is_success = mock.Mock(return_value=True)

    steps.build('docker_client', 'image_id', ['cmd'], message='message',
                register={'repo': 'repo', 'tag': 'tag'})

    container_mock.assert_called_with('docker_client', 'image_id',
                                      {'Entrypoint': ['/bin/sh', '-c']},
                                      mount_point=steps.MOUNT_POINT)
    c.run.assert_called_with(['cmd'])
    c.is_success.assert_called_with()
    c.commit.assert_called_with('message', {'Cmd': []},
                                tag='tag', repository='repo')


@mock.patch('rabix.sdktools.steps.Container')
def test_build_ok(container_mock):
    container_mock().is_success = mock.Mock(return_value=True)

    steps.build('docker_client', 'image_id', ['cmd'], message='message',
                register={'repo': 'repo', 'tag': 'tag'})

    assert_build(container_mock)
    container_mock().commit.assert_called_with('message', {'Cmd': []},
                                               tag='tag', repository='repo')


@raises(RabixError)
@mock.patch('rabix.sdktools.steps.Container')
def test_build_fail(container_mock):
    container_mock().is_success = mock.Mock(return_value=False)

    try:
        steps.build('docker_client', 'image_id', ['cmd'], message='message',
                    register={'repo': 'repo', 'tag': 'tag'})
    finally:
        assert_build(container_mock)
        assert not container_mock().commit.called


def assert_build(container):
    container.assert_called_with('docker_client', 'image_id',
                                 {'Entrypoint': ['/bin/sh', '-c']},
                                 mount_point=steps.MOUNT_POINT)
    instance = container()
    instance.run.assert_called_with(['cmd'])
    instance.is_success.assert_called_with()


def test_resolve():
    r = steps.Runner(None, context={"x": "5"})
    eq_(r.resolve("something"), "something")
    eq_(r.resolve("${x}"), "5")
    eq_(r.resolve(["${x}", "y"]), ["5", "y"])
    eq_(r.resolve({"${y}": "${x}"}), {"${y}": "5"})


def test_make_config():
    eq_(steps.make_config(), {})
    eq_(steps.make_config(something='nothing'), {})
    eq_(steps.make_config(hostname='hostname'), {'Hostname': 'hostname'})
    eq_(steps.make_config(entrypoint='cmd'), {'Entrypoint': ['cmd']})
