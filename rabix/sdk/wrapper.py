import os
import uuid
import logging
import tempfile

import rabix.common.six as six
from rabix.common.errors import ValidationError
from rabix.common.protocol import WrapperJob, Resources, Outputs
from rabix.common.util import import_name, get_import_name
from rabix.common.loadsave import to_json
from rabix.sdk.schema import SchemaBased, IODef

log = logging.getLogger(__name__)


class Wrapper(object):
    class Inputs(IODef):
        pass

    class Outputs(IODef):
        pass

    class Params(SchemaBased):
        pass

    def __init__(self, inputs, params, context=None, resources=None):
        self.inputs = self.Inputs(**inputs)
        self.params = self.Params(**params)
        self.outputs = self.Outputs()
        self.context = context or {}
        self.resources = resources or Resources()
        self.inputs._load_meta()

    def execute(self):
        pass

    def __call__(self, method, args):
        method = method or '_entry_point'
        if not callable(getattr(self, method, None)):
            raise ValueError('Wrapper has no method %s' % method)
        result = getattr(self, method)(**args)
        if result is None:
            self.outputs._save_meta()
            out_dict = self.outputs.__json__()
            for item in self.outputs:
                if item.id not in out_dict or not item.value:
                    out_dict[item.id] = []
            return Outputs(out_dict)
        return result

    def get_requirements(self):
        return _get_method_requirements(self, 'execute') or Resources()

    def _entry_point(self):
        errors = self.inputs._validate()
        errors.update(self.params._validate())
        if errors:
            raise ValidationError(six.text_type(errors))
        return self.job('execute', requirements=self.get_requirements())

    @classmethod
    def _get_schema(cls):
        return {
            '$$type': 'schema/app/sbgsdk',
            'inputs': cls.Inputs._get_schema(),
            'outputs': cls.Outputs._get_schema(),
            'params': cls.Params._get_schema(),
        }

    def job(self, method=None, args=None, requirements=None):
        full_args = {
            '$method': method,
            '$inputs': self.inputs.__json__(),
            '$params': self.params.__json__(),
        }
        full_args.update(args or {})
        return WrapperJob(
            wrapper_id=get_import_name(self.__class__),
            resources=(
                requirements or _get_method_requirements(self, method) or
                Resources()
            ),
            args=full_args
        )

    def test(self):
        test_exec_dir = tempfile.mkdtemp(
            prefix='test_%s_' % self.__class__.__name__, dir='.'
        )
        os.chdir(test_exec_dir)
        try:
            self.inputs._validate(assert_=True)
            self.params._validate(assert_=True)
            self.inputs._save_meta()
            initial_job = self.job()
            outputs = WrapperRunner(initial_job).exec_full(initial_job).outputs
            self.outputs = self.Outputs(**outputs)
            self.outputs._load_meta()
            self.outputs._validate(assert_=True)
        finally:
            os.chdir('..')
        return self.outputs

    def get_allocated_memory(self, units='MB'):
        units = units.upper()
        converter = {
            'B': lambda b: b,
            'KB': lambda b: b / 1024,
            'MB': lambda b: b / 1024**2,
            'GB': lambda b: b / 1024**3,
        }
        if units not in converter:
            raise ValueError(
                'Units argument must be one of: %s' % list(converter.keys())
            )
        return converter[units](self.resources.mem_mb * 1024**2)


def _get_method_requirements(wrapper, method_name):
    if not method_name:
        return getattr(wrapper, '_requirements', None)
    m = getattr(wrapper, method_name, None)
    return (getattr(m, '_requirements', None) or
            getattr(wrapper, '_requirements', None))


class WrapperRunner(object):
    def __init__(self, job):
        self.initial_job = job

    def exec_full(self, job):
        if not job.job_id:
            job.job_id = uuid.uuid4()
        for key, val in six.iteritems(job.args):
            job.args[key] = self.resolve(val)
        result = self.exec_wrapper_job(job)
        if isinstance(result, WrapperJob):
            return self.exec_full(result)
        return result

    def exec_wrapper_job(self, job):
        log.debug('Job started: %s' % to_json(job))
        cls = import_name(job.wrapper_id)
        wrp = cls(inputs=job.args.pop('$inputs', {}),
                  params=job.args.pop('$params', {}),
                  context=self.initial_job.context,
                  resources=job.resources)
        result = wrp(job.args.pop('$method', None), job.args)
        log.debug('Job result: %s' % to_json(result))
        return (result if result is not None
                else Outputs(wrp.outputs.__json__()))

    def resolve(self, val):
        if isinstance(val, WrapperJob):
            return self.exec_full(val)
        if isinstance(val, list):
            return [self.resolve(item) for item in val]
        if isinstance(val, dict):
            return {
                k: self.resolve(v)
                for k, v in six.iteritems(val)
            }
        return val
