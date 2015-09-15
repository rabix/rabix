import operator
import six
import logging
import os
import glob
import re
import hashlib

# noinspection PyUnresolvedReferences
from six.moves import reduce
from itertools import chain
from avro.schema import UnionSchema, Schema, ArraySchema

if six.PY2:
    from avro.io import validate
else:
    from avro.io import Validate as validate

from rabix.common.util import sec_files_naming_conv, wrap_in_list, to_abspath
from rabix.expressions import ExpressionEvaluator

log = logging.getLogger(__name__)


def intersect_dicts(d1, d2):
    return {k: v for k, v in six.iteritems(d1) if v == d2.get(k)}


def meta(path, inputs, eval, outputBinding):
    meta, result = outputBinding.get('metadata', {}), {}
    inherit = meta.pop('__inherit__', None)
    if inherit:
        src = inputs.get(inherit)
        if isinstance(src, list):
            result = reduce(intersect_dicts, [x.meta for x in src]) \
                if len(src) > 1 else src[0].meta
        else:
            result = src.meta
    result.update(**meta)
    for k, v in six.iteritems(result):
        result[k] = eval.resolve(v, context={"class": "File", "path": path})
    return result


def secondary_files(p, outputBinding, evaluator):
    secondaryFiles = []
    secFiles = wrap_in_list(evaluator.resolve(outputBinding.get('secondaryFiles', [])))
    for s in secFiles:
        path = sec_files_naming_conv(p, s)
        secondaryFiles.append({'path': to_abspath(path)})
    return secondaryFiles


class InputAdapter(object):
    def __init__(self, value, evaluator, schema, input_binding=None, key=''):
        self.evaluator = evaluator
        self.schema = schema
        self.adapter = input_binding or (
            isinstance(self.schema, Schema) and
            self.schema.props.get('inputBinding')
        )
        self.has_adapter = self.adapter is not None
        self.adapter = self.adapter or {}
        self.key = key
        if isinstance(self.schema, UnionSchema):
            for opt in self.schema.schemas:
                if validate(opt, value):
                    self.schema = opt
                    break
        expr = self.adapter.get('valueFrom')
        json = value.to_dict() if hasattr(value, 'to_dict') else value
        self.value = evaluator.resolve(expr, json) if expr else value

    __str__ = lambda self: six.text_type(self.value)
    __repr__ = lambda self: 'InputAdapter(%s)' % self
    position = property(lambda self: (self.adapter.get('position', 0), self.key))
    prefix = property(lambda self: self.adapter.get('prefix'))
    item_separator = property(lambda self: self.adapter.get('itemSeparator', ','))
    separate = property(lambda self: self.adapter.get('separate', True))

    def arg_list(self):
        if isinstance(self.value, dict):
            return self.as_dict()
        if isinstance(self.value, list):
            return self.as_list()
        return self.as_primitive()

    def as_primitive(self):
        if self.value is None or self.value is False:
            return []
        if self.value is True:
            if not self.prefix:
                raise ValueError('Boolean arguments must have a prefix.')
            return [self.prefix]
        if not self.prefix:
            return [six.text_type(self.value)]
        if self.separate:
            return [self.prefix, self.value]
        return [self.prefix + six.text_type(self.value)]

    def as_dict(self):
        sch = lambda key: next(field for field in self.schema.fields
                               if field.name == key)
        adapters = [InputAdapter(v, self.evaluator, sch(k), key=k)
                    for k, v in six.iteritems(self.value)]
        adapters = [adp for adp in adapters if adp.has_adapter]
        res = reduce(
            operator.add,
            [a.arg_list() for a in sorted(adapters, key=lambda x: x.position)],
            []
        )
        return res

    def as_toplevel(self, mix_with):
        sch = lambda key: next(inp for inp in self.schema
                               if inp.id == key.split('.')[-1])
        adapters = mix_with + [
            InputAdapter(v, self.evaluator, sch(k).validator,
                         sch(k).input_binding, key=k)
            for k, v in six.iteritems(self.value)
            ]

        res = reduce(
            operator.add,
            [a.arg_list()
             for a in sorted(adapters, key=lambda x: x.position)
             if a.has_adapter],
            []
        )
        return res

    def as_list(self):
        # on top-level, I have type and depth, for easier parallelism,
        # but this is hacky as hell
        schema = (
            self.schema.items if isinstance(self.schema, ArraySchema)
            else self.schema
        )
        items = [InputAdapter(item, self.evaluator, schema)
                 for item in self.value]

        if not self.prefix:
            return reduce(operator.add, [a.arg_list() for a in items], [])

        if self.separate and self.item_separator is None:
            return reduce(operator.add, [[self.prefix] + a.arg_list()
                                         for a in items], [])

        if not self.separate and self.item_separator is None:
            return [self.prefix + a.list_item()
                    for a in items if a.list_item() is not None]

        joined = self.item_separator.join(
            filter(None, [a.list_item() for a in items])
        )

        if self.separate and self.item_separator is not None:
            return [self.prefix, joined]
        return [self.prefix + joined]

    def list_item(self):
        as_arg_list = self.arg_list()
        if not as_arg_list:
            return None
        if len(as_arg_list) > 1 or isinstance(as_arg_list[0], (dict, list)):
            raise ValueError('Only lists of primitive values can use itemSeparator.')
        return six.text_type(as_arg_list[0])


class CLIJob(object):
    def __init__(self, job):
        self.job = job
        self.app = job.app
        self._stdin = self.app.stdin
        self._stdout = self.app.stdout
        self.base_cmd = self.app.base_command
        if isinstance(self.base_cmd, six.string_types):
            self.base_cmd = self.base_cmd.split(' ')
        self.args = self.app.arguments
        self.eval = ExpressionEvaluator(job)

    @property
    def stdin(self):
        if self._stdin:
            return self.eval.resolve(self._stdin)

    @property
    def stdout(self):
        if self._stdout:
            return self.eval.resolve(self._stdout)

    def make_arg_list(self):
        adapters = [InputAdapter(a.get('valueFrom'), self.eval, {}, a)
                    for a in self.args]
        ia = InputAdapter(self.job.inputs, self.eval, self.app.inputs)
        args = ia.as_toplevel(adapters)
        base_cmd = [self.eval.resolve(item) for item in self.base_cmd]

        return [six.text_type(arg) for arg in base_cmd + args]

    def cmd_line(self):
        a = self.make_arg_list()

        if self._stdin:
            a += ['<', self.stdin]
        if self._stdout:
            a += ['>', self.eval.resolve(self.stdout)]
        return ' '.join(a)  # TODO: escape

    @staticmethod
    def glob_or(pattern):
        """
        >>> CLIJob.glob_or("simple")
        ['simple']

        >>> CLIJob.glob_or("{option1,option2}")
        ['option1', 'option2']

        :param pattern:
        :return:
        """
        if re.match('^\{[^,]+(,[^,]+)*\}$', pattern):
            return pattern.strip('{}').split(',')
        return [pattern]

    def get_outputs(self, job_dir, job):
        result, outs = {}, self.app.outputs
        eval = ExpressionEvaluator(job)
        for out in outs:
            out_binding = out.output_binding
            ret = os.getcwd()
            os.chdir(job_dir)
            pattern = eval.resolve(out_binding.get('glob')) or ""
            patterns = chain(*[self.glob_or(p) for p in wrap_in_list(pattern)])
            files = chain(*[glob.glob(p) for p in patterns])

            result[out.id] = [
                {
                    'path': os.path.abspath(p),
                    'size': os.stat(p).st_size,
                    # 'checksum': 'sha1$' +
                    # hashlib.sha1(open(os.path.abspath(p)).read()).hexdigest(),
                    'metadata': meta(p, job.inputs, eval, out_binding),
                    'secondaryFiles': secondary_files(p, out_binding, eval)
                } for p in files]
            os.chdir(ret)
            if out.depth == 0:
                result[out.id] = result[out.id][0] if result[out.id] else None
        return result
