import os
import sys
import json
import docker
import shutil
from rabix.executors.container import ensure_image
from nose.tools import nottest, raises
from rabix.tests import mock_app_bad_repo, mock_app_good_repo
from rabix.executors.cli import get_tool, main


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
    cmd1 = ['run', '--job', './rabix/tests/test-cmdline/bwa-mem.yml']
    tool1 = get_tool(cmd1)
    assert tool1
    cmd2 = ['run', '-v', '--job=./rabix/tests/'
                         'test-cmdline/bwa-mem.yml']
    tool2 = get_tool(cmd2)
    assert tool2
    cmd3 = ['run', '-v', '--job', './rabix/tests/test-cmdline/bwa-mem.yml']
    tool3 = get_tool(cmd3)
    assert tool3
    cmd4 = ['run', '--job=./rabix/tests/test-cmdline/bwa-mem.yml']
    tool4 = get_tool(cmd4)
    assert tool4
    cmd5 = ['run', '--job', './rabix/tests/test-cmdline/bwa-mem-job.yml',
            '--tool', './rabix/tests/test-cmdline/bwa-mem-tool.yml']
    tool5 = get_tool(cmd5)
    assert tool5
    cmd6 = ['run', '--job',
            'https://s3.amazonaws.com/rabix/rabix-test/bwa-mem-job.json',
            '--tool',
            'https://s3.amazonaws.com/rabix/rabix-test/bwa-mem-tool.json']
    tool6 = get_tool(cmd6)
    assert tool6
    cmd7 = ['run', '--job',
            'https://s3.amazonaws.com/rabix/rabix-test/bwa-mem.json']
    tool7 = get_tool(cmd7)
    assert tool7
    cmd8 = ['run', '--job',
            './rabix/tests/test-cmdline/bwa-mem-toolurl.yml']
    tool8 = get_tool(cmd8)
    assert tool8


@nottest
def test_expr_and_meta():
    sys.argv = ['rabix', '--job', '/home/sinisa/devel/CWL/rabix/rabix/tests/test-expr/bwa-mem1.json', '--dir',
                'test1']
    main()
    with open(os.path.abspath('./test1') + '/output.sam.meta') as m:
        meta = json.load(m)
        assert meta['expr_test'] == 'successful'
    shutil.rmtree(os.path.abspath('./test1'))
    sys.argv = ['rabix', '--job', '/home/sinisa/devel/CWL/rabix/rabix/tests/test-expr/bwa-mem2.json', '--dir',
                'test2']
    main()
    with open(os.path.abspath('./test2') + '/output.sam.meta') as m:
        meta = json.load(m)
        assert meta['expr_test'] == 'successful'
    shutil.rmtree(os.path.abspath('./test2'))


@nottest
def test_fetch_remote_files():
    sys.argv = ['rabix', '--tool', '/home/sinisa/devel/CWL/rabix/rabix/tests/test-cmdline/bwa-mem-tool.yml', '--reads',
                'https://s3.amazonaws.com/rabix/rabix-test/example_human_Illumina.pe_1.fastq', '--reads',
                'https://s3.amazonaws.com/rabix/rabix-test/example_human_Illumina.pe_2.fastq', '--reference',
                './rabix/tests/test-files/chr20.fa', '--dir', 'testdir']
    main()
    assert os.path.exists(os.path.abspath('./testdir') + '/output.sam')
    shutil.rmtree(os.path.abspath('./testdir'))
