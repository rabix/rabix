import operator
import six
import logging
import os
import glob
import copy
import shlex

from jsonschema import Draft4Validator
import jsonschema.exceptions
from six.moves import reduce

from rabix.common.ref_resolver import resolve_pointer
from rabix.common.util import sec_files_naming_conv, wrap_in_list
from rabix.expressions import Evaluator

log = logging.getLogger(__name__)


class AdapterEvaluator(object):

    def __init__(self, job):
        self.job = job
        self.ev = Evaluator()

    def evaluate(self, expr_object, context=None, job=None):
        if isinstance(expr_object, dict):
            return self.ev.evaluate(
                expr_object.get('lang', 'javascript'),
                expr_object.get('value'),
                job.to_dict() if job else self.job.to_dict(),
                context
            )
        else:
            return self.ev.evaluate('javascript', expr_object,
                                    job.to_dict() if job else
                                    self.job.to_dict(), context)

    def resolve(self, val, context=None, job=None):
        if not isinstance(val, dict):
            return val
        if 'expr' in val or '$expr' in val:
            v = val.get('expr') or val.get('$expr')
            return self.evaluate(v, context, job=job)
        if 'job' in val or '$job' in val:
            v = val.get('job') or val.get('$job')
            return resolve_pointer(self.job.to_dict(), v)
        return val

    def __deepcopy__(self, memo):
        return AdapterEvaluator(copy.deepcopy(self.job, memo))


def intersect_dicts(d1, d2):
    return {k: v for k, v in six.iteritems(d1) if v == d2.get(k)}


def meta(path, inputs, eval, adapter):
    meta, result = adapter.get('metadata', {}), {}
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
        result[k] = eval.resolve(v, context=path)
    return result


def secondary_files(p, adapter, evaluator):
    secondaryFiles = []
    secFiles = wrap_in_list(evaluator.resolve(adapter.get('secondaryFiles', [])))
    for s in secFiles:
        path = sec_files_naming_conv(p, s)
        secondaryFiles.append({'path': path})
    return secondaryFiles


class InputAdapter(object):
    def __init__(self, value, evaluator, schema, adapter_dict=None, key=''):
        self.evaluator = evaluator
        self.schema = schema or {}
        self.adapter = adapter_dict or self.schema.get('adapter')
        self.has_adapter = self.adapter is not None
        self.adapter = self.adapter or {}
        self.key = key
        if 'oneOf' in self.schema:
            for opt in self.schema['oneOf']:
                validator = Draft4Validator(opt)
                try:
                    validator.validate(value)
                    self.schema = opt
                except jsonschema.exceptions.ValidationError:
                    pass
        self.value = evaluator.resolve(value)

    __str__ = lambda self: six.text_type(self.value)
    __repr__ = lambda self: 'InputAdapter(%s)' % self
    position = property(lambda self: (self.adapter.get('order', 9999999), self.key))
    prefix = property(lambda self: self.adapter.get('prefix'))
    item_separator = property(lambda self: self.adapter.get('itemSeparator', ','))

    @property
    def separator(self):
        sep = self.adapter.get('separator')
        if sep == ' ':
            sep = None
        return sep

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
        if self.separator in [' ', None]:
            return [self.prefix, self.value]
        return [self.prefix + self.separator + six.text_type(self.value)]

    def as_dict(self, mix_with=None):
        sch = lambda key: self.schema.get('properties', {}).get(key, {})
        adapters = [InputAdapter(v, self.evaluator, sch(k), key=k)
                    for k, v in six.iteritems(self.value)]
        adapters = (mix_with or []) + [adp for adp in adapters if adp.has_adapter]
        res = reduce(
            operator.add,
            [a.arg_list() for a in sorted(adapters, key=lambda x: x.position)],
            []
        )
        return res

    def as_list(self):
        items = [InputAdapter(item, self.evaluator, self.schema.get('items', {}))
                 for item in self.value]

        if not self.prefix:
            return reduce(operator.add, [a.arg_list() for a in items], [])

        if self.separator is None and self.item_separator is None:
            return reduce(operator.add, [[self.prefix] + a.arg_list()
                                         for a in items], [])

        if self.separator is not None and self.item_separator is None:
            return [self.prefix + self.separator + a.list_item()
                    for a in items if a.list_item() is not None]

        joined = self.item_separator.join(
            filter(None, [a.list_item() for a in items])
        )

        if self.separator is None and self.item_separator is not None:
            return [self.prefix, joined]
        return [self.prefix + self.separator + joined]

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
        self.adapter = self.app.adapter or {}
        self._stdin = self.adapter.get('stdin')
        self._stdout = self.adapter.get('stdout')
        self.base_cmd = self.adapter.get('baseCmd', [])
        if isinstance(self.base_cmd, six.string_types):
            self.base_cmd = self.base_cmd.split(' ')
        self.args = self.adapter.get('args', [])
        self.input_schema = self.app.inputs.schema
        self.output_schema = self.app.outputs.schema
        self.eval = AdapterEvaluator(job)

    @property
    def stdin(self):
        if self._stdin:
            return self.eval.resolve(self._stdin)

    @property
    def stdout(self):
        if self._stdout:
            return self.eval.resolve(self._stdout)

    def make_arg_list(self):
        adapters = [InputAdapter(a['value'], self.eval, {}, a)
                    for a in self.args]
        ia = InputAdapter(self.job.inputs, self.eval, self.input_schema)
        args = ia.as_dict(adapters)
        base_cmd = [self.eval.resolve(item) for item in self.base_cmd]

        return [six.text_type(arg) for arg in base_cmd + args]

    def cmd_line(self):
        a = self.make_arg_list()

        if self._stdin:
            a += ['<', self.stdin]
        if self._stdout:
            a += ['>', self.eval.resolve(self.stdout)]
        return ' '.join(a)  # TODO: escape

    def get_outputs(self, job_dir, job):
        result, outs = {}, self.output_schema.get('properties', {})
        for k, v in six.iteritems(outs):
            adapter = v['adapter']
            ret = os.getcwd()
            os.chdir(job_dir)
            pattern = self.eval.resolve(adapter.get('glob'), job=job) or ""
            files = glob.glob(pattern)
            result[k] = [{'path': os.path.abspath(p),
                          'metadata': meta(p, job.inputs, self.eval, adapter),
                          'secondaryFiles': secondary_files(p, adapter, self.eval)} for p in files]
            os.chdir(ret)
            if v['type'] != 'array':
                result[k] = result[k][0] if result[k] else None
        return result
