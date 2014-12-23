import os
from os.path import splitext

from rabix.common.ref_resolver import from_url, resolve_pointer

# TODO: lists, transforms


ARGUMENT_TYPES = {}


def make_argument(arg, input_name=None):

    if input_name:
        arg_class = ARGUMENT_TYPES.get(arg.get('type'), InputArgument)
        return arg_class(input_name, arg)
    else:
        return ArgumentAdapter(arg)


class ArgumentAdapter(object):
    def __init__(self, adapter):
        self.adapter = adapter
        self.value = adapter.get('value')
        self.transforms = {
            'strip_ext': lambda x: splitext(x)[0]
        }

    def bind(self, job):
        value_from = self.adapter.get('valueFrom')
        if value_from:
            self.value = resolve_pointer(job, value_from[1:], None)
        return self

    def _cli(self, prefix, separator, value):
        transform = self.transforms.get(self.adapter.get('transform'), str)
        value = transform(value)

        if separator != ' ':
            return [prefix + separator + value]

        if prefix:
            return [prefix, value]

        return [value]

    def cli(self):
        return self._cli(self.prefix, self.separator, self.value)

    @property
    def weight(self):
        return self.adapter.get('order', 99)

    @property
    def prefix(self):
        return self.adapter.get('prefix') or ''

    @property
    def separator(self):
        return self.adapter.get('separator') or ' '


class ObjectHandler(object):

    def __init__(self, name, schema):
        self.name = name
        self.schema = schema


class InputArgument(ArgumentAdapter):

    def __init__(self, name, schema):
        adapter = schema.get('adapter', {})
        super(InputArgument, self).__init__(adapter)
        self.schema = schema
        self.name = name

    def bind(self, job):
        if not self.value:
            self.value = resolve_pointer(job, "inputs/" + self.name, None)
        return self


class FileArgument(InputArgument):
    def __init__(self, name, schema):
        super(FileArgument, self).__init__(name, schema)

    def cli(self):
        return super(FileArgument, self)._cli(
            self.prefix,
            self.separator,
            self.value['path'])
ARGUMENT_TYPES['file'] = FileArgument


class ArrayArgument(InputArgument):
    def __init__(self, name, schema):
        super(ArrayArgument, self).__init__(name, schema)

    def cli(self):
        list_separator = self.adapter.get('item_separator')
        if list_separator:
            item_arg = make_argument(self.schema['items'], 'name')
            values = []
            for val in self.value:
                item_arg.value = val
                values += item_arg.cli()

            return self._cli(self.prefix,
                             self.separator,
                             list_separator.join(values))
        else:
            items = self.schema['items']
            if not items.get('adapter'):
                items['adapter'] = {}

            items['adapter']['prefix'] = self.adapter.get('prefix')
            items['adapter']['separator'] = self.adapter.get('separator')
            item_arg = make_argument(items, 'name')
            values = []
            for val in self.value:
                item_arg.value = val
                values += item_arg.cli()

            return values
ARGUMENT_TYPES['array'] = ArrayArgument


class ObjectArgument(InputArgument):

    def __init__(self, name, schema):
        self.schemas.append(super(ObjectArgument, self).__init__(name, schema))
        if schema.get('properties'):
            pass
        elif schema.get('oneOf'):
            pass
        else:
            raise RuntimeError('Invalid object type')

    def cli(self):
        [make_argument(input_spec, input_name)
         for input_name, input_spec
         in self.schema['properties'].iteritems()]
        pass
ARGUMENT_TYPES['object'] = ObjectArgument


class Adapter(object):
    def __init__(self, tool):
        self.tool = tool
        self.args = []

        if tool['adapter'].get('args'):
            self.args += [make_argument(arg) for arg
                          in tool['adapter']['args']]

        self.args += [make_argument(input_spec, input_name)
                      for input_name, input_spec
                      in tool['inputs']['properties'].iteritems()]
        sorted(self.args, key=lambda a: a.weight)

    def cli(self, job):
        for req in self.tool['inputs']['required']:
            if req not in job['inputs']:
                raise RuntimeError("Required input not provided: " + req)

        cli = list(self.tool['adapter']['baseCmd'])
        for arg in self.args:
            arg.bind(job)
            if arg.value is not None:
                cli += arg.cli()
        return cli


def gen_cli(tool, job):
    a = Adapter(tool)
    return a.cli(job)


# # Here be tests
from nose.tools import eq_

TOOL_STUB = {
    'inputs': {
        'type': 'object',
        'required': [],
        'properties': {},
        'adapter': {
            'baseCmd': [],
            'args': []
        }
    }
}


def test_simple_argument():
    arg = make_argument({'order': 1, 'value': 5})
    eq_(arg.cli(), ['5'])


def test_ref_argument():
    arg = make_argument({'order': 1, 'valueFrom': '#ref'})
    arg.bind({'ref': 'value'})
    eq_(arg.cli(), ['value'])


def test_argument_separator():
    arg = make_argument({'order': 1, 'value': 'str', 'prefix': '-x'})
    eq_(arg.cli(), ['-x', 'str'])

    arg = make_argument({'value': 'str', 'prefix': '-x', 'separator': '='})
    eq_(arg.cli(), ['-x=str'])


def test_argument_transform():
    arg = make_argument({'value': 'str.ext',
                         'transform': 'strip_ext'})
    eq_(arg.cli(), ['str'])


def test_file_argument():
    arg = make_argument({'type': 'file'}, 'ref')
    arg.bind({'inputs': {'ref': {'path': 'a/path'}}})
    eq_(arg.cli(), ['a/path'])


def test_list_argument():
    arg = make_argument({'type': 'array',
                         'items': {'type': 'number'},
                         'adapter': {'prefix': '-x'}
                         }, 'ref')
    arg.bind({'inputs': {'ref': [1, 2, 3]}})
    eq_(arg.cli(), ['-x', '1', '-x', '2', '-x', '3'])

    arg = make_argument({'type': 'array',
                         'items': {'type': 'number'},
                         'adapter': {'prefix': '-x', 'item_separator': ','}
                         }, 'ref')
    arg.bind({'inputs': {'ref': [1, 2, 3]}})
    eq_(arg.cli(), ['-x', '1,2,3'])


def test_list_argument_file_transform():
    arg = make_argument({'adapter': {'prefix': '-x'},
                         'type': 'array',
                         'items': {'type': 'file',
                                   'adapter': {'transform': 'strip_ext'}}
                         }, 'value')
    arg.bind({'inputs': {'value': [{'path': 'a/b.txt'}, {'path': 'c/d.txt'}]}})
    eq_(arg.cli(), ['-x', 'a/b', '-x', 'c/d'])


def test_bwa_mem():
    path = os.path.join(os.path.dirname(__file__), '../examples/bwa-mem.yml')
    doc = from_url(path)
    tool, job = doc['tool'], doc['job']
    print(gen_cli(tool, job))


def test_tmap_mapall():
    path = os.path.join(os.path.dirname(__file__), '../examples/tmap.yml')
    doc = from_url(path)
    tool, job = doc['mapall'], doc['exampleJob']
    print(gen_cli(tool, job))


# test_bwa_mem()
# test_tmap_mapall()
