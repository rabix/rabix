import os
import sys
import json
import docker
import shutil
from rabix.executors.container import ensure_image
from nose.tools import nottest, raises
from rabix.tests import mock_app_bad_repo, mock_app_good_repo
from rabix.executors.cli import get_tool, main, dry_run_parse


@nottest
@raises(Exception)
def test_provide_image_bad_repo():
    uri = mock_app_bad_repo["tool"]["requirements"]["environment"][
        "container"]["uri"]
    imageId = mock_app_bad_repo["tool"]["requirements"]["environment"][
        "container"]["imageId"]
    docker_client = docker.Client(version='1.12')
    ensure_image(docker_client, imageId, uri)


@nottest
def test_provide_image_good_repo():
    uri = mock_app_good_repo["tool"]["requirements"]["environment"][
        "container"]["uri"]
    imageId = mock_app_good_repo["tool"]["requirements"]["environment"][
        "container"]["imageId"]
    docker_client = docker.Client(version='1.12')
    ensure_image(docker_client, imageId, uri)


def test_cmd_line():
    cmd1 = dry_run_parse(['--job', './rabix/tests/test-cmdline/bwa-mem.yml#job'])
    tool1 = get_tool(cmd1)
    assert tool1
    cmd2 = dry_run_parse(['-v', '--job=./rabix/tests/'
                         'test-cmdline/bwa-mem.yml#job'])
    tool2 = get_tool(cmd2)
    assert tool2
    cmd3 = dry_run_parse(['-v', '--job',
                          './rabix/tests/test-cmdline/bwa-mem.yml#job'])
    tool3 = get_tool(cmd3)
    assert tool3
    cmd4 = dry_run_parse(['--job=./rabix/tests/test-cmdline/bwa-mem.yml#job'])
    tool4 = get_tool(cmd4)
    assert tool4
    cmd5 = dry_run_parse(['--job',
                          './rabix/tests/test-cmdline/bwa-mem-job.yml#job',
                          '--tool',
                          './rabix/tests/test-cmdline/bwa-mem-tool.yml#tool'])
    tool5 = get_tool(cmd5)
    assert tool5
    cmd6 = dry_run_parse(
        ['--job',
         'https://s3.amazonaws.com/rabix/rabix-test/bwa-mem-job.json#job',
         '--tool',
         'https://s3.amazonaws.com/rabix/rabix-test/bwa-mem-tool.json#tool'])
    tool6 = get_tool(cmd6)
    assert tool6
    cmd7 = dry_run_parse(
        ['--job',
         'https://s3.amazonaws.com/rabix/rabix-test/bwa-mem.json#job'])
    tool7 = get_tool(cmd7)
    assert tool7
    cmd8 = dry_run_parse(
        ['--job',
         './rabix/tests/test-cmdline/bwa-mem-toolurl.yml#job'])
    tool8 = get_tool(cmd8)
    assert tool8


@nottest
def test_expr_and_meta():
    sys.argv = ['rabix', '--job',
                './rabix/tests/test-expr/bwa-mem1.json#job',
                '--dir', 'test1']
    main()
    with open(os.path.abspath('./test1') + '/output.sam.meta') as m:
        meta = json.load(m)
        assert meta['expr_test'] == 'successful'
    shutil.rmtree(os.path.abspath('./test1'))
    sys.argv = ['rabix', '--job',
                './rabix/tests/test-expr/bwa-mem2.json#job',
                '--dir', 'test2']
    main()
    with open(os.path.abspath('./test2') + '/output.sam.meta') as m:
        meta = json.load(m)
        assert meta['expr_test'] == 'successful'
    shutil.rmtree(os.path.abspath('./test2'))


@nottest
def test_fetch_remote_files():
    sys.argv = ['rabix', '--dir', 'testdir', '-t',
                './rabix/tests/test-cmdline/bwa-mem-tool.yml#tool', '--', '--reads',
                'https://s3.amazonaws.com/rabix/rabix-test/example_human_Illumina.pe_1.fastq', '--reads',
                'https://s3.amazonaws.com/rabix/rabix-test/example_human_Illumina.pe_2.fastq', '--reference',
                './rabix/tests/test-files/chr20.fa']
    main()
    assert os.path.exists(os.path.abspath('./testdir') + '/output.sam')
    shutil.rmtree(os.path.abspath('./testdir'))
