from .cli_app import CommandLineTool, CreateFileRequirement
from .adapter import CLIJob


def init(context):
    context.add_type('CommandLineTool', CommandLineTool.from_dict)
    context.add_type('CreateFileRequirement', CreateFileRequirement.from_dict)
