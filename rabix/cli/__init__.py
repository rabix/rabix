from .cli_app import CliApp, Requirements


def init(context):
    context.add_type('CommandLine', CliApp.from_dict)
    context.add_type('CliApp', CliApp.from_dict)
    context.add_type('CliTool', CliApp.from_dict)
    context.add_type('Requirements', Requirements.from_dict)
