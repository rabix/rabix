from .cli_app import CommandLineTool
from .adapter import CLIJob


def init(context):
    context.add_type('CommandLineTool', CommandLineTool.from_dict)
