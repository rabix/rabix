import operator
import six
import logging
import os
import glob

from jsonschema import Draft4Validator
import jsonschema.exceptions

from six.moves import reduce
from rabix.common.ref_resolver import resolve_pointer
from rabix.common.util import sec_files_naming_conv
from rabix.expressions import Evaluator

ev = Evaluator()
log = logging.getLogger(__name__)


def evaluate(expr_object, job, context=None, *args, **kwargs):
    if isinstance(expr_object, dict):
        return ev.evaluate(expr_object.get('lang', 'javascript'), expr_object.get(
            'value'), job.to_dict(), context, *args, **kwargs)
    else:
        return ev.evaluate('javascript', expr_object, job.to_dict(), context, *args, **kwargs)


def intersect_dicts(d1, d2):
    return {k: v for k, v in six.iteritems(d1) if v == d2.get(k)}


def eval_resolve(val, job, context=None):
    if not isinstance(val, dict):
        return val
    if 'expr' in val or '$expr' in val:
        v = val.get('expr') or val.get('$expr')
        return evaluate(v, job, context)
    if 'job' in val or '$job' in val:
        v = val.get('job') or val.get('$job')
        return resolve_pointer(job.to_dict(context), v)
    return val


def meta(path, job, adapter):
    meta, result = adapter.get('metadata', {}), {}
    inherit = meta.pop('__inherit__', None)
    if inherit:
        src = job.inputs.get(inherit)
        if isinstance(src, list):
            result = reduce(intersect_dicts, [x.meta for x in src]) \
                if len(src) > 1 else src[0].meta
        else:
            result = src.meta
    result.update(**meta)
    for k, v in six.iteritems(result):
        result[k] = eval_resolve(v, job, context=path)
    return result


def secondaryFiles(p, adapter):
    secondaryFiles = []
    secFiles = adapter.get('secondaryFiles', [])
    for s in secFiles:
        path = sec_files_naming_conv(p, s)
        secondaryFiles.append({'path': path})
    return secondaryFiles


class InputAdapter(object):
    def __init__(self, value, job, schema, adapter_dict=None, key=''):
        self.job = job
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
        self.value = eval_resolve(value, self.job)
        if self.is_file():
            self.value = self.value['path']

    __str__ = lambda self: str(self.value)
    __repr__ = lambda self: 'InputAdapter(%s)' % self
    position = property(lambda self: (self.adapter.get('order', 9999999), self.key))
    # transform = property(lambda self: self.adapter.get('value'))
    prefix = property(lambda self: self.adapter.get('prefix'))
    item_separator = property(lambda self: self.adapter.get('itemSeparator', ','))

    @property
    def separator(self):
        sep = self.adapter.get('separator')
        if sep == ' ':
            sep = None
        return sep

    def is_file(self):
        return isinstance(self.value, dict) and 'path' in self.value

    def arg_list(self):
        if isinstance(self.value, dict):
            return self.as_dict()
        if isinstance(self.value, list):
            return self.as_list()
        return self.as_primitive()

    def as_primitive(self):
        if self.value in (None, False):
            return []
        if self.value is True:
            if not self.prefix:
                raise ValueError('Boolean arguments must have a prefix.')
            return [self.prefix]
        if not self.prefix:
            return [self.value]
        if self.separator in [' ', None]:
            return [self.prefix, self.value]
        return [self.prefix + self.separator + six.text_type(self.value)]

    def as_dict(self, mix_with=None):
        sch = lambda key: self.schema.get('properties', {}).get(key, {})
        adapters = [InputAdapter(v, self.job, sch(k), key=k)
                    for k, v in six.iteritems(self.value)]
        adapters = (mix_with or []) + [adp for adp in adapters if adp.has_adapter]
        return reduce(
            operator.add,
            [a.arg_list() for a in sorted(adapters, key=lambda x: x.position)],
            []
        )

    def as_list(self):
        items = [InputAdapter(item, self.job, self.schema.get('items', {}))
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
        self.stdin = eval_resolve(self.adapter.get('stdin'), self.job)
        self.stdout = eval_resolve(self.adapter.get('stdout'), self.job)
        self.base_cmd = self.adapter.get('baseCmd', [])
        if isinstance(self.base_cmd, six.string_types):
            self.base_cmd = self.base_cmd.split(' ')
        self.args = self.adapter.get('args', [])
        self.input_schema = self.app.inputs.schema
        self.output_schema = self.app.outputs.schema

    def make_arg_list(self):
        adapters = [InputAdapter(a['value'], self.job, {}, a)
                    for a in self.args]
        args = InputAdapter(self.job.inputs, self.job, self.input_schema).as_dict(adapters)
        base_cmd = [eval_resolve(item, self.job) for item in self.base_cmd]

        return [six.text_type(arg) for arg in base_cmd + args]

    def cmd_line(self):
        a = self.make_arg_list()
        if self.stdin:
            a += ['<', self.stdin]
        if self.stdout:
            a += ['>', self.stdout]
        return ' '.join(a)  # TODO: escape

    def get_outputs(self, job_dir, job):
        result, outs = {}, self.output_schema.get('properties', {})
        for k, v in six.iteritems(outs):
            adapter = v['adapter']
            ret = os.getcwd()
            os.chdir(job_dir)
            files = glob.glob(eval_resolve(adapter['glob'], job))
            result[k] = [{'path': os.path.abspath(p), 'metadata': meta(p, job, adapter),
                          'secondaryFiles': secondaryFiles(p, adapter)} for p in files]
            os.chdir(ret)
            if v['type'] != 'array':
                result[k] = result[k][0] if result[k] else None
        return result
