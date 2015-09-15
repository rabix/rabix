import os
import re
import sys
import six
import json
import shutil

from nose.tools import raises, nottest

from rabix.tests import mock_app_bad_repo, mock_app_good_repo, \
    result_parallel_workflow, result_nested_workflow
from rabix.main import main
from rabix.docker import docker_client, get_image


@nottest
@raises(Exception)
def test_provide_image_bad_repo():
    uri = mock_app_bad_repo["tool"]["requirements"]["environment"][
        "container"]["uri"]
    imageId = mock_app_bad_repo["tool"]["requirements"]["environment"][
        "container"]["imageId"]
    docker = docker_client()
    get_image(docker, image_id=imageId, repo=uri)


@nottest
def test_provide_image_good_repo():
    uri = mock_app_good_repo["tool"]["requirements"]["environment"][
        "container"]["uri"]
    imageId = mock_app_good_repo["tool"]["requirements"]["environment"][
        "container"]["imageId"]
    docker = docker_client()
    get_image(docker, image_id=imageId, repo=uri)


def test_expr_and_meta():
    sys.argv = ['rabix', './rabix/tests/test-expr/bwa-mem.json',
                '-i', './rabix/tests/test-cmdline/inputs.json',
                '--dir', 'test1', '--']
    main()
    with open(os.path.abspath('./test1') + '/aligned.sam.rbx.json') as m:
        meta = json.load(m)
        assert meta['metadata']['expr_test'] == 'successful'
    shutil.rmtree(os.path.abspath('./test1'))
    sys.argv = ['rabix', '-i', './rabix/tests/test-cmdline/inputs.json',
                './rabix/tests/test-expr/bwa-mem.json',
                '--dir', 'test2']
    main()
    with open(os.path.abspath('./test2') + '/aligned.sam.rbx.json') as m:
        meta = json.load(m)
        assert meta['metadata']['expr_test'] == 'successful'
    shutil.rmtree(os.path.abspath('./test2'))


def test_fetch_remote_files():
    sys.argv = ['rabix', '--dir', 'test_fetch_remote',
                './rabix/tests/test-expr/bwa-mem.json', '--',
                '--reads',
                'https://s3.amazonaws.com/rabix/rabix-test/'
                'example_human_Illumina.pe_1.fastq', '--reads',
                'https://s3.amazonaws.com/rabix/rabix-test/'
                'example_human_Illumina.pe_2.fastq', '--reference',
                './rabix/tests/test-files/chr20.fa']
    main()
    assert os.path.exists(os.path.abspath('./test_fetch_remote') + '/aligned.sam')
    shutil.rmtree(os.path.abspath('./test_fetch_remote'))


def test_params_from_input_file():
    sys.argv = ['rabix', '-i', 'rabix/tests/test-cmdline/inputs.json',
                'rabix/tests/test-expr/bwa-mem.json',
                '-d', 'test_from_input_file']
    main()
    assert os.path.exists(os.path.abspath('./test_from_input_file') + '/aligned.sam')
    shutil.rmtree(os.path.abspath('./test_from_input_file'))


def test_override_input():
    sys.argv = ['rabix', '-i', 'rabix/tests/test-cmdline/inputs.json', '-d',
                'test_override_input', 'rabix/tests/test-expr/bwa-mem.json', '--',
                '--reference', 'rabix/tests/test-files/chr20.fa']
    main()
    assert os.path.exists(os.path.abspath('./test_override_input') + '/aligned.sam')
    shutil.rmtree(os.path.abspath('./test_override_input'))


def check_result(dir, res):

    def compare_file(myfile, resfile):
        for k, v in six.iteritems(myfile):
            if k == 'path':
                print(resfile)
                assert re.match(resfile.get('path'), os.path.basename(v))
            elif k == 'secondaryFiles':
                compare_output(v, resfile.get('secondaryFiles'))
            else:
                assert v == resfile.get(k)

    def compare_output(myoutput, resoutput):
        if isinstance(myoutput, list):
            for out in myoutput:
                compare_file(out, resoutput)
        else:
            compare_file(myoutput, resoutput)

    with open('/'.join([dir, 'cwl.output.json']), 'r') as f:
        dct = json.load(f)
        for k, v in six.iteritems(dct):
            compare_output(v, res.get(k))


@nottest
def test_parallelization():
    '''
    Testing implicit parallelization in workflows
    '''
    cwd = os.getcwd()
    try:
        os.mkdir('test_parralelization')
        sys.argv = ['rabix', '../rabix/tests/test_workflows/parallelization_workflow.json',
                    '--', '--input', '../rabix/tests/test-files/chr20.fa']

        os.chdir('./test_parralelization')
        main()
        dir = filter(lambda x: os.path.isdir(x) and 'index_file' in x, os.walk('.').next()[1])
        for d in dir:
            check_result(d, result_parallel_workflow)
    finally:
        os.chdir(cwd)
        shutil.rmtree(os.path.abspath('./test_parralelization'))


@nottest
def test_nested_workflow():
    '''
    Testing nested workflows, inputs type directory and
    tools which creates index files
    '''
    cwd = os.getcwd()
    try:
        os.mkdir('test_workflow')
        sys.argv = ['rabix', '../rabix/tests/test_workflows/nested_workflow.json',
                    '--', '--input', '../rabix/tests/test-files/chr20.fa']
        os.chdir('./test_workflow')
        main()
        dir = filter(lambda x: os.path.isdir(x) and 'index_file' in x,
                     os.walk('.').next()[1])
        for d in dir:
            check_result(d, result_nested_workflow)
    finally:
        os.chdir(cwd)
        shutil.rmtree(os.path.abspath('./test_workflow'))
