from rabix.main import get_tool, dry_run_parse


def test_cmd_line():
    cmd1 = dry_run_parse(['https://s3.amazonaws.com/rabix/rabix-test/'
                          'bwa-mem.json',
                          '-i', './rabix/tests/test-cmdline/inputs.json'])
    tool1 = get_tool(cmd1)
    assert tool1
