from .expression_tool import ExpressionTool
from .evaluator import (
    ExpressionEvaluator, ExpressionEngine, ValueResolver,
    ExpressionEngineRequirement, update_engines
)


def init(context):
    context.add_type('ExpressionTool', ExpressionTool.from_dict)
    context.add_type(
        'ExpressionEngineRequirement',
        ExpressionEngineRequirement.from_dict
    )