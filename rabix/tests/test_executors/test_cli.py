import os
import sys
import json
import shutil

from nose.tools import nottest, raises

from rabix.docker.container import ensure_image
from rabix.docker import docker_client
from rabix.tests import mock_app_bad_repo, mock_app_good_repo
from rabix.main import get_tool, main, dry_run_parse


@nottest
@raises(Exception)
def test_provide_image_bad_repo():
    uri = mock_app_bad_repo["tool"]["requirements"]["environment"][
        "container"]["uri"]
    imageId = mock_app_bad_repo["tool"]["requirements"]["environment"][
        "container"]["imageId"]
    docker = docker_client()
    ensure_image(docker, imageId, uri)


@nottest
def test_provide_image_good_repo():
    uri = mock_app_good_repo["tool"]["requirements"]["environment"][
        "container"]["uri"]
    imageId = mock_app_good_repo["tool"]["requirements"]["environment"][
        "container"]["imageId"]
    docker = docker_client()
    ensure_image(docker_client, imageId, uri)


def test_cmd_line():
    cmd1 = dry_run_parse(['https://s3.amazonaws.com/rabix/rabix-test/'
                          'bwa-mem.json',
                          '-i', './rabix/tests/test-cmdline/inputs.json'])
    tool1 = get_tool(cmd1)
    assert tool1


@nottest
def test_expr_and_meta():
    sys.argv = ['rabix', '-i', './rabix/tests/test-cmdline/inputs.json',
                './rabix/tests/test-expr/bwa-mem1.json#tool',
                '--dir', 'test1']
    main()
    with open(os.path.abspath('./test1') + '/output.sam.meta') as m:
        meta = json.load(m)
        assert meta['expr_test'] == 'successful'
    shutil.rmtree(os.path.abspath('./test1'))
    sys.argv = ['rabix', '-i', './rabix/tests/test-cmdline/inputs.json',
                './rabix/tests/test-expr/bwa-mem2.json#tool',
                '--dir', 'test2']
    main()
    with open(os.path.abspath('./test2') + '/output.sam.meta') as m:
        meta = json.load(m)
        assert meta['expr_test'] == 'successful'
    shutil.rmtree(os.path.abspath('./test2'))


@nottest
def test_fetch_remote_files():
    sys.argv = ['rabix', '--dir', 'testdir',
                './rabix/tests/test-cmdline/bwa-mem-tool.yml#tool', '--',
                '--reads',
                'https://s3.amazonaws.com/rabix/rabix-test/'
                'example_human_Illumina.pe_1.fastq', '--reads',
                'https://s3.amazonaws.com/rabix/rabix-test/'
                'example_human_Illumina.pe_2.fastq', '--reference',
                './rabix/tests/test-files/chr20.fa']
    main()
    assert os.path.exists(os.path.abspath('./testdir') + '/output.sam')
    shutil.rmtree(os.path.abspath('./testdir'))


@nottest
def test_params_from_input_file():
    sys.argv = ['rabix', '-i', 'rabix/tests/test-cmdline/inputs.json',
                'rabix/tests/test-expr/bwa-mem1.json#tool',
                '-d', 'testdir']
    main()
    assert os.path.exists(os.path.abspath('./testdir') + '/output.sam')
    shutil.rmtree(os.path.abspath('./testdir'))


@nottest
def test_override_input():
    sys.argv = ['rabix', '-i', 'rabix/tests/test-cmdline/inputs.json', '--d',
                'testdir', 'rabix/tests/test-expr/bwa-mem1.json#tool', '--',
                '--reference', 'rabix/tests/test-files/chr20.fa.rbx.json']
    main()
    assert os.path.exists(os.path.abspath('./testdir') + '/output.sam')
    shutil.rmtree(os.path.abspath('./testdir'))
