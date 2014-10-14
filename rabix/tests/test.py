import os
import docker
from rabix.executors.runner import DockerRunner, NativeRunner
from rabix.executors.container import provide_image
from nose.tools import nottest, raises
from rabix.tests import mock_app_bad_repo, mock_app_good_repo
from rabix.cliche.ref_resolver import from_url
from rabix.executors.cli import get_tool


def test_docker_runner():
    command = ['bash', '-c', 'grep -r chr > output.txt']
    runner = DockerRunner(tool=mock_app_good_repo)
    runner.run(command)
    pass


@nottest
def test_native_runner():
    command = ['grep', '-r', 'chr']
    pass


@raises(Exception)
def test_provide_image_bad_repo():
    uri = mock_app_good_repo["tool"]["requirements"]["environment"][
        "container"]["uri"]
    imageId = mock_app_good_repo["tool"]["requirements"]["environment"][
        "container"]["imageId"]
    docker_client = docker.Client(version='1.12')
    provide_image(imageId, uri, docker_client)


def test_provide_image_good_repo():
    uri = mock_app_good_repo["tool"]["requirements"]["environment"][
        "container"]["uri"]
    imageId = mock_app_good_repo["tool"]["requirements"]["environment"][
        "container"]["imageId"]
    docker_client = docker.Client(version='1.12')
    provide_image(imageId, uri, docker_client)


def test_cmd_line():
    CMD1 = ['run', '--job', '/home/sinisa/devel/CWL/rabix/rabix/tests/'
                            'test-cmdline/bwa-mem.yml']
    tool1 = get_tool(CMD1)
    assert tool1
    CMD2 = ['run', '-v', '--job=/home/sinisa/devel/CWL/rabix/rabix/tests/'
                         'test-cmdline/bwa-mem.yml']
    tool2 = get_tool(CMD2)
    assert tool2
    CMD3 = ['run', '-v', '--job', './rabix/tests/test-cmdline/bwa-mem.yml']
    tool3 = get_tool(CMD3)
    assert tool3
    CMD4 = ['run', '--job=./rabix/tests/test-cmdline/bwa-mem.yml']
    tool4 = get_tool(CMD4)
    assert tool4
    CMD5 = ['run', '--job', './rabix/tests/test-cmdline/bwa-mem-job.yml',
            '--tool', './rabix/tests/test-cmdline/bwa-mem-tool.yml']
    tool5 = get_tool(CMD5)
    assert tool5
    CMD6 = ['run', '--job',
            'https://s3.amazonaws.com/rabix/rabix-test/bwa-mem-job.json',
            '--tool',
            'https://s3.amazonaws.com/rabix/rabix-test/bwa-mem-tool.json']
    tool6 = get_tool(CMD6)
    assert tool6
    CMD7 = ['run', '--job',
            'https://s3.amazonaws.com/rabix/rabix-test/bwa-mem.json']
    tool7 = get_tool(CMD7)
    assert tool7
    CMD8 = ['run', '--job',
            '/home/sinisa/devel/CWL/rabix/rabix/tests/test-cmdline/'
            'bwa-mem-toolurl.yml']
    tool8 = get_tool(CMD8)
    assert tool8
