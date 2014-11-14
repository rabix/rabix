<<<<<<< HEAD
import os
import copy
import operator
import glob

import six
import copy
import operator
import six
import logging

from jsonschema import Draft4Validator
import jsonschema.exceptions

from six.moves import reduce
from rabix.common.ref_resolver import from_url
from rabix.expressions.evaluator import Evaluator

ev = Evaluator()


def evaluate(expr_object, job, context=None, *args, **kwargs):
    return ev.evaluate(expr_object.get('lang', 'javascript'), expr_object.get('value'), job, context, *args, **kwargs)


def eval_resolve(val, job, context=None):
    if not isinstance(val, dict):
        return val
    if '$expr' in val:
        return evaluate(val['$expr'], job, context)
    if '$job' in val:
        return resolve_pointer(job, val['$job'], default=None)
    return val


class InputAdapter(object):
    def __init__(self, value, job_dict, schema, adapter_dict=None, key=None):
        self.job = job_dict
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
        if self.transform:
            self.value = eval_resolve(self.transform, self.job, self.value)
        elif self.is_file():
            self.value = self.value['path']

    __str__ = lambda self: str(self.value)
    __repr__ = lambda self: 'InputAdapter(%s)' % self
    position = property(lambda self: (self.adapter.get('order', 9999999), self.key))
    transform = property(lambda self: self.adapter.get('transform'))
    prefix = property(lambda self: self.adapter.get('prefix'))
    item_separator = property(lambda self: self.adapter.get('itemSeparator', ','))

    @property
    def separator(self):
        sep = self.adapter.get('separator', '')
        if sep == ' ':
            return None
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
        if self.separator is None:
            return [self.prefix, self.value]
        return [self.prefix + self.separator + six.text_type(self.value)]

    def as_dict(self, mix_with=None):
        sch = lambda key: self.schema.get('properties', {}).get(key, {})
        adapters = [InputAdapter(v, self.job, sch(k), key=k) for k, v in six.iteritems(self.value)]
        adapters = (mix_with or []) + filter(lambda adp: adp.has_adapter, adapters)
        return reduce(operator.add, [a.arg_list() for a in sorted(adapters, key=lambda x: x.position)], [])

    def as_list(self):
        items = [InputAdapter(item, self.job, self.schema.get('items', {}))
                 for item in self.value]
        if not self.prefix:
            return reduce(operator.add, [a.arg_list() for a in items], [])
        if self.separator is None and self.item_separator is None:
            return reduce(operator.add, [[self.prefix] + a.arg_list() for a in items], [])
        if self.separator is not None and self.item_separator is None:
            return [self.prefix + self.separator + a.list_item() for a in items if a.list_item() is not None]
        joined = self.item_separator.join(filter(None, [a.list_item() for a in items]))
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
    def __init__(self, job_dict, tool_dict, path_mapper=lambda x: x):
        self.job = copy.deepcopy(job_dict)
        self.tool = tool_dict
        self.path_mapper = path_mapper
        self.rewrite_paths(self.job['inputs'])
        self.adapter = self.tool.get('adapter', {})
        self.stdin = eval_resolve(self.adapter.get('stdin'), self.job)
        self.stdout = eval_resolve(self.adapter.get('stdout'), self.job)
        self.base_cmd = self.adapter.get('baseCmd', [])
        if isinstance(self.base_cmd, six.string_types):
            self.base_cmd = self.base_cmd.split(' ')
        self.args = self.adapter.get('args', [])
        self.input_schema = self.tool.get('inputs', {})
        self.output_schema = self.tool.get('outputs', {})

    def make_arg_list(self):
        adapters = [InputAdapter(a['value'], self.job, {}, a) for a in self.args]
        args = InputAdapter(self.job['inputs'], self.job, self.input_schema).as_dict(adapters)
        base_cmd = [eval_resolve(item, self.job) for item in self.base_cmd]
        return map(six.text_type, base_cmd + args)

    def rewrite_paths(self, val):
        if isinstance(val, list):
            for item in val:
                self.rewrite_paths(item)
        elif isinstance(val, dict) and 'path' in val:
            val['path'] = self.path_mapper(val['path'])
        elif isinstance(val, dict):
            for item in six.itervalues(val):
                self.rewrite_paths(item)
