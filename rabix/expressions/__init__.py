from .expression_tool import ExpressionTool
from .evaluator import Evaluator, ExpressionEvaluator


def init(context):
    context.add_type('ExpressionTool', ExpressionTool.from_dict)
