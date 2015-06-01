from .script_app import ExpresionTool
from .evaluator import Evaluator


def init(context):
    context.add_type('Script', ExpresionTool.from_dict)
