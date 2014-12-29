from nose.tools import nottest, raises

from rabix.docker import docker_client, get_image
from rabix.tests import mock_app_bad_repo, mock_app_good_repo
from rabix.main import get_tool, dry_run_parse


@nottest
@raises(Exception)
def test_provide_image_bad_repo():
    uri = mock_app_bad_repo["tool"]["requirements"]["environment"][
        "container"]["uri"]
    imageId = mock_app_bad_repo["tool"]["requirements"]["environment"][
        "container"]["imageId"]
    docker = docker_client()
    get_image(docker, image_id=imageId, repo=uri)


def test_provide_image_good_repo():
    uri = mock_app_good_repo["tool"]["requirements"]["environment"][
        "container"]["uri"]
    imageId = mock_app_good_repo["tool"]["requirements"]["environment"][
        "container"]["imageId"]
    docker = docker_client()
    get_image(docker, image_id=imageId, repo=uri)


@nottest
def test_cmd_line():
    cmd1 = dry_run_parse(['https://s3.amazonaws.com/rabix/rabix-test/'
                          'bwa-mem.json',
                          '-i', './rabix/tests/test-cmdline/inputs.json'])
    tool1 = get_tool(cmd1)
    assert tool1
