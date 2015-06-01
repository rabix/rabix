from .cli_app import CommandLineTool
from .adapter import CLIJob


def init(context):
    context.add_type('CommandLine', CommandLineTool.from_dict)
    context.add_type('CliApp', CommandLineTool.from_dict)
    context.add_type('CliTool', CommandLineTool.from_dict)
