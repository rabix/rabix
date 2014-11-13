import os
import copy
import operator
import glob

import six
from jsonschema import Draft4Validator

from six.moves import reduce
from rabix.common.ref_resolver import from_url
from rabix.expressions.evaluator import Evaluator

ev = Evaluator()


def evaluate(expr_object, job, context, *args, **kwargs):
    error_msg = 'Expression needs to be an object with "$expr". Got: %s' % expr_object
    if not isinstance(expr_object, dict):
        raise TypeError(error_msg)
    expression = expr_object.get('$expr', {}).get('value')
    lang = expr_object.get('$expr', {}).get('lang', 'javascript')
    if not isinstance(expression, six.string_types) or not isinstance(lang, six.string_types):
        raise ValueError(error_msg)
    return ev.evaluate(lang, expression, job, context, *args, **kwargs)


def intersect_dicts(d1, d2):
    return {k: v for k, v in six.iteritems(d1) if v == d2.get(k)}


def sort_args(args):
    with_indices = sorted(enumerate(args), key=lambda a: [a[1].position, a[0]])
    return [a[1] for a in with_indices]


class Argument(object):
    def __init__(self, job, value, schema, adapter=None, basedir='.'):
        self.job = job
        self.basedir = basedir
        self.schema = schema or {}
        if 'oneOf' in self.schema:
            self.schema = self._schema_from_opts(schema['oneOf'], value)
        self.adapter = adapter or self.schema.get('adapter', {})
        self.position = self.adapter.get('order', 9999999)
        self.prefix = self.adapter.get('prefix')
        self.separator = self.adapter.get('separator', '')  # TODO: default to ' '?
        if self.separator == ' ':
            self.separator = None
        self.item_separator = self.adapter.get('itemSeparator', ',')
        self.transform = self.adapter.get('transform')
        if self.transform:
            value = evaluate(self.transform, self.job, value)
        elif self.schema.get('type') in ('file', 'directory'):
            if not value['path']:
                raise ValueError('path must be set for file types.')
            value = os.path.normpath(os.path.join(basedir, value['path']))
        self.value = value

    def __int__(self):
        return bool(self.arg_list())

    def __unicode__(self):
        return six.text_type(self.value)

    def _schema_for(self, key):
        """ If value is object, get schema for its property. """
        if key in self.schema.get('properties', {}):
            return self.schema['properties'][key]

    def get_args(self, adapter_mixins=None):
        args = [Argument(self.job, v, self._schema_for(k), basedir=self.basedir) for k, v in
                sorted(six.iteritems(self.value)) if 'adapter' in self._schema_for(k)]
        args = (adapter_mixins or []) + args
        args = sort_args(args)
        return reduce(operator.add, [a.arg_list() for a in args], [])

    def arg_list(self):
        if isinstance(self.value, dict):
            return self._as_dict()
        if isinstance(self.value, list):
            return self._as_list()
        return self._as_primitive()

    def _as_primitive(self):
        if self.value in (None, False):
            return []
        if self.value is True and (self.separator or not self.prefix):
            raise Exception('Boolean arguments must have a prefix and '
                            'no separator.')
        if not self.prefix:
            return [self.value]
        if self.separator is None:
            return [self.prefix] if self.value is True \
                else [self.prefix, self.value]
        return [self.prefix + self.separator + six.text_type(self.value)]

    def _as_dict(self):
        args = [Argument(self.job, v, self._schema_for(k), basedir=self.basedir) for k, v
                in sorted(six.iteritems(self.value)) if 'adapter' in self._schema_for(k)]
        args = sort_args(args)
        return reduce(operator.add, [a.arg_list() for a in args], [])

    def _as_list(self):
        item_schema = self.schema.get('items', {})
        args = [Argument(self.job, item, item_schema, basedir=self.basedir) for item in self.value]
        if not self.prefix:
            return reduce(operator.add, [a.arg_list() for a in args], [])
        if self.separator is None and self.item_separator is None:
            return reduce(operator.add, [[self.prefix] + a.arg_list() for a in args], [])
        if self.separator is not None and self.item_separator is None:
            return [self.prefix + self.separator + a._list_item() for a in args if a._list_item() is not None]
        args_as_strings = filter(None, [a._list_item() for a in args])
        joined = self.item_separator.join(args_as_strings)
        if self.separator is None and self.item_separator is not None:
            return [self.prefix, joined]
        return [self.prefix + self.separator + joined]

    def _list_item(self):
        as_arg_list = self.arg_list()
        if not as_arg_list:
            return None
        if len(as_arg_list) > 1:
            raise Exception('Multiple arguments as part '
                            'of str-separated list.')
        return six.text_type(as_arg_list[0])

    @staticmethod
    def _schema_from_opts(options, value):
        for opt in options:
            validator = Draft4Validator(opt)
            try:
                validator.validate(value)
                return opt
            except:
                pass
        raise Exception('No options valid for supplied value.')


class Adapter(object):
    def __init__(self, tool, basedir):
        self.tool = tool
        self.basedir = basedir
        self.adapter = tool.get('adapter', {})
        self.base_cmd = self.adapter.get('baseCmd', [])
        if isinstance(self.base_cmd, six.string_types):
            self.base_cmd = self.base_cmd.split(' ')
        self.stdout = self.adapter.get('stdout')
        self.stdin = self.adapter.get('stdin')
        self.args = self.adapter.get('args', [])
        self.input_schema = self.tool.get('inputs', {})
        self.output_schema = self.tool.get('outputs', {})

    def _arg_list_and_stdin(self, job):
        adapter_args = [Argument(job, self._get_value(a, job), {}, a, basedir=self.basedir)
                        for a in self.args]

        stdin = self._get_value({"value": self.stdin}, job) if self.stdin else None
        stdin = stdin if stdin is None else os.path.normpath(os.path.join(self.basedir, stdin))

        return Argument(job, job['inputs'], self.input_schema, basedir=self.basedir).\
            get_args(adapter_args), stdin

    def _resolve_job_resources(self, job):
        resolved = copy.deepcopy(job)
        res = self.tool.get('requirements', {}).get('resources', {})
        for k, v in six.iteritems(res):
            if isinstance(v, dict):
                resolved['allocatedResources'][k] =\
                    evaluate(v, resolved, None)
        return resolved

    def _base_args(self, job):
        e = lambda x: evaluate(x, job, None) if isinstance(x, dict) else x
        return map(e, self.base_cmd)

    def get_shell_args(self, job):
        job = self._resolve_job_resources(job)
        arg_list, stdin = self._arg_list_and_stdin(job)
        stdout = self._get_stdout_name(job)
        stdin = ['<', stdin] if stdin else []
        stdout = ['>', stdout] if stdout else []
        return map(six.text_type, self._base_args(job) + arg_list + stdin + stdout)

    def cmd_line(self, job):
        return ' '.join(self.get_shell_args(job))

    def _get_stdout_name(self, job):
        if self.stdout is None:
            return None
        if isinstance(self.stdout, six.string_types):
            return self.stdout
        if '$expr' in self.stdout:
            return evaluate(self.stdout, job, None)
        return self.stdout['path']  # TODO: remove?

    @staticmethod
    def _get_value(arg, job):
        value = arg.get('value')
        if not value:
            raise Exception('Value not specified for arg %s' % arg)
        if isinstance(value, dict) and '$expr' in value:
            value = evaluate(value, job, None)
        elif isinstance(value, dict) and '$job' in value:
            value = resolve_pointer(job, value["$job"])
        return value

    @staticmethod
    def _make_meta(file, adapter, job):
        meta, result = adapter.get('meta', {}), {}
        inherit = meta.pop('__inherit__', None)
        if inherit:
            src = job['inputs'].get(inherit)
            if src and isinstance(src, list):
                result = reduce(intersect_dicts, [x.get('meta', {})
                                                  for x in src])\
                    if len(src) > 1 else src[0].get('meta', {})
            elif src:
                result = src.get('meta', {})
        result.update(**meta)
        for k, v in six.iteritems(result):
            if isinstance(v, dict) and '$expr' in v:
                result[k] = evaluate(v, job, file)
        return result

    def get_outputs(self, job_dir, job):
        result, outs = {}, self.output_schema.get('properties', {})
        for k, v in six.iteritems(outs):
            adapter = v['adapter']
            if adapter.get('stdout'):
                files = [os.path.join(job_dir, self._get_stdout_name(job))]
            else:
                files = glob.glob(os.path.join(job_dir, adapter['glob']))
            result[k] = [{'path': p, 'meta': self._make_meta(p, adapter, job)}
                         for p in files]
            if v['type'] != 'array':
                result[k] = result[k][0] if result[k] else None
        return result


def cmd_line(doc_path, tool_key='tool', job_key='job'):
    doc = from_url(doc_path)
    tool, job = doc[tool_key], doc[job_key]
    return Adapter(tool).cmd_line(job)


def test_cmd_line(tool, job, test):
    result = Adapter(tool).cmd_line(job)
    if result == test['expected_cmd_line']:
        return True
    else:
        print('Got:', result)
        print('Expected:', test['expected_cmd_line'])
        return False


def run_tests(doc_path, tool_key='tool', test_key='tests'):
    doc = from_url(doc_path)
    tests = doc[test_key]
    for test in tests:
        tool = test.get(tool_key)
        if test_cmd_line(tool, test.get('test_job'), test):
            print('Test %s completed successfully!' % test.get('id'))
        else:
            print('Test %s completed failed!' % test.get('id'))


if __name__ == '__main__':
    print(cmd_line(os.path.join(os.path.dirname(__file__),
                                '../examples/tmap.yml'),
                   'mapall', 'exampleJob'))
    print(cmd_line(os.path.join(os.path.dirname(__file__),
                                '../examples/bwa-mem.yml')))
    run_tests(os.path.join(os.path.dirname(__file__),
                           '../examples/bwa-mem-test.yml'))
