from .cli_app import (
    CommandLineTool, CreateFileRequirement, EnvVarRequirement,
    Container, CpuRequirement, MemRequirement
)
from .adapter import CLIJob


def init(context):
    context.add_type('CommandLineTool', CommandLineTool.from_dict)
    context.add_type('CreateFileRequirement', CreateFileRequirement.from_dict)
    context.add_type('EnvVarRequirement', EnvVarRequirement.from_dict)
    context.add_type('MemRequirement', MemRequirement.from_dict)
    context.add_type('CPURequirement', CpuRequirement.from_dict)
