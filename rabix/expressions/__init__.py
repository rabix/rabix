from .script_app import ScriptApp
from .evaluator import Evaluator


def init(context):
    context.add_type('Script', ScriptApp.from_dict)
