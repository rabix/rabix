from rabix.sdk import schema as _
from rabix.sdk.wrapper import Wrapper

Inputs = _.IODef
input = _.IOAttr

Outputs = _.IODef
output = _.IOAttr

Params = _.SchemaBased
integer = _.IntAttr
boolean = _.BoolAttr
enum = _.EnumAttr
real = _.RealAttr
string = _.StringAttr
struct = _.StructAttr
__wrapper__ = Wrapper
