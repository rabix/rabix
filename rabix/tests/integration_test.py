import os
import sys
import json
import shutil

from rabix.main import main


def test_expr_and_meta():
    sys.argv = ['rabix', './rabix/tests/test-expr/bwa-mem.json',
                '-i', './rabix/tests/test-cmdline/inputs.json',
                '--dir', 'test1', '--']
    main()
    with open(os.path.abspath('./test1') + '/output.sam.rbx.json') as m:
        meta = json.load(m)
        assert meta['metadata']['expr_test'] == 'successful'
    shutil.rmtree(os.path.abspath('./test1'))
    sys.argv = ['rabix', '-i', './rabix/tests/test-cmdline/inputs.json',
                './rabix/tests/test-expr/bwa-mem.json',
                '--dir', 'test2']
    main()
    with open(os.path.abspath('./test2') + '/output.sam.rbx.json') as m:
        meta = json.load(m)
        assert meta['metadata']['expr_test'] == 'successful'
    shutil.rmtree(os.path.abspath('./test2'))


def test_fetch_remote_files():
    sys.argv = ['rabix', '--dir', 'test_fetch_remote',
                './rabix/tests/test-cmdline/bwa-mem.json#tool', '--',
                '--reads',
                'https://s3.amazonaws.com/rabix/rabix-test/'
                'example_human_Illumina.pe_1.fastq', '--reads',
                'https://s3.amazonaws.com/rabix/rabix-test/'
                'example_human_Illumina.pe_2.fastq', '--reference',
                './rabix/tests/test-files/chr20.fa']
    main()
    assert os.path.exists(os.path.abspath('./test_fetch_remote') + '/output.sam')
    shutil.rmtree(os.path.abspath('./test_fetch_remote'))


def test_params_from_input_file():
    sys.argv = ['rabix', '-i', 'rabix/tests/test-cmdline/inputs.json',
                'rabix/tests/test-expr/bwa-mem.json',
                '-d', 'test_from_input_file']
    main()
    assert os.path.exists(os.path.abspath('./test_from_input_file') + '/output.sam')
    shutil.rmtree(os.path.abspath('./test_from_input_file'))


def test_override_input():
    sys.argv = ['rabix', '-i', 'rabix/tests/test-cmdline/inputs.json', '-d',
                'test_override_input', 'rabix/tests/test-expr/bwa-mem.json', '--',
                '--reference', 'rabix/tests/test-files/chr20.fa']
    main()
    assert os.path.exists(os.path.abspath('./test_override_input') + '/output.sam')
    shutil.rmtree(os.path.abspath('./test_override_input'))
