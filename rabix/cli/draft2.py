from rabix.common.models import App

__author__ = 'luka'


class CommandLineTool(App):

    def __init__(self, app_id, inputs, outputs, requirements, hints):
        super(CommandLineTool, self).__init__(
            app_id, inputs, outputs, requirements, hints
        )
