import os
import copy
from executors.runner import DockerRunner, NativeRunner
from executors.container import provide_image
from nose.tools import nottest, raises
from tests import mock_app_bad_repo, mock_app_good_repo
from cliche.ref_resolver import from_url
from executors.cli import get_tool

@nottest
def test_docker_runner():
    command = ['bash', '-c', 'grep -r chr > output.txt']
    runner = DockerRunner(tool=mock_app_good_repo)
    runner.run(command)
    pass

@nottest
def test_native_runner():
    command = ['grep', '-r', 'chr']
    pass

@nottest
@raises(Exception)
def test_provide_image_bad_repo():
    tool = copy.deepcopy(mock_app_bad_repo)
    runner = DockerRunner(tool['tool'])
    runner.provide_image()

@nottest
def test_provide_image_good_repo():
    tool = copy.deepcopy(mock_app_good_repo)
    runner = DockerRunner(tool['tool'])
    runner.provide_image()


def test_cmd_line():
    #rabix run [-v] (--job=<job> [--tool=<tool> {inputs}] | --tool=<tool> {inputs})
    #job containing tool
    CMD1 = ['run', '--job', '/home/sinisa/devel/CWL/executors/experiments/tests/test-cmdline/bwa-mem.yml'] # abspath --job, no verbose
    tool1 = get_tool(CMD1)
    CMD2 = ['run', '-v', '--job=/home/sinisa/devel/CWL/executors/experiments/tests/test-cmdline/bwa-mem.yml'] # abspath --job=, verbose
    tool2 = get_tool(CMD2)
    CMD3 = ['run', '-v', '--job', './test-cmdline/bwa-mem.yml'] # relpath --job, verbose
    tool3 = get_tool(CMD3)
    CMD4 = ['run', '--job=./test-cmdline/bwa-mem.yml'] # relpath --job=, no verbose
    tool4 = get_tool(CMD4)
    CMD5 = ['run', '--job', './test-cmdline/bwa-mem-job.yml', '--tool', './test-cmdline/bwa-mem-tool.yml']
    tool5 = get_tool(CMD5)
    CMD6 = ['run', '--job', 'https://s3.amazonaws.com/rabix/rabix-test/bwa-mem-job.json', '--tool', 'https://s3.amazonaws.com/rabix/rabix-test/bwa-mem-tool.json']
    tool6 = get_tool(CMD6)
    CMD7 = ['run', '--job', 'https://s3.amazonaws.com/rabix/rabix-test/bwa-mem.json']
    tool7 = get_tool(CMD7)
    CMD8 = ['run', '--job', '/home/sinisa/devel/CWL/executors/experiments/tests/test-data/test-cmdline/bwa-mem-toolurl.yml']
    tool8 = get_tool(CMD8)
