from .cli_app import CliApp


def init(context):
    context.add_type('CliApp', CliApp.from_dict)
