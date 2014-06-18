from rabix.sdk import schema as _
from wrapper import Wrapper

Inputs = _.SchemaBased
input = _.IOAttr

Outputs = _.SchemaBased
output = _.IOAttr

Params = _.SchemaBased
integer = _.IntAttr
boolean = _.BoolAttr
enum = _.EnumAttr
real = _.RealAttr
string = _.StringAttr
struct = _.StructAttr
__wrapper__ = Wrapper
