import re
from copy import deepcopy
from keyword import iskeyword
import logging

import networkx as nx

from rabix.common.errors import ValidationError

log = logging.getLogger(__name__)


class Model(dict):
    """ Inherit this class for an easy way to turn JSON objects with $$type fields to classes. """
    TYPE = None

    def __init__(self, obj):
        super(dict, self).__init__()
        self.update(obj)

    def _validate(self):
        """ Override to validate. Raise AssertionError or ValidationError or return array of errors. Or return None. """
        pass

    def _check_field(self, field, field_type=None, null=True, look_in=None):
        obj = look_in or self
        assert field in obj, 'Must have a "%s" field' % field
        val = obj[field]
        if val is None:
            assert null, '%s cannot be null'
            return
        assert isinstance(val, field_type), '%s cannot be of type %s' % (val, val.__class__.__name__)

    def validate(self):
        try:
            errors = self._validate()
        except AssertionError, e:
            raise ValidationError(unicode(e))
        if errors:
            raise ValidationError('. '.join(errors))

    def __json__(self):
        return dict({'$$type': self.TYPE}, **self)

    @classmethod
    def from_dict(cls, obj):
        return cls(obj)


class Pipeline(Model):
    TYPE = 'app/pipeline'

    apps = property(lambda self: self['apps'])
    steps = property(lambda self: self['steps'])

    def __init__(self, obj):
        super(Pipeline, self).__init__(obj)
        self.nx = None

    def draw(self, file_name='pipeline.png'):
        if not self.nx:
            self.validate()
        try:
            agr = nx.to_agraph(self.nx)
        except ImportError:
            log.error('Failed to import pygraphviz, cannot draw.')
            return
        agr.layout()
        agr.draw(file_name if file_name.endswith('png') else file_name+'.png')

    def _build_nx(self):
        g = nx.DiGraph()
        inputs, outputs = set(), set()
        for step in self.steps:
            g.add_node(step['id'], step)
            for inp_id, src_list in step.get('inputs', {}).iteritems():
                src_list = src_list if isinstance(src_list, list) else filter(None, [src_list])
                for src in src_list:
                    if '.' not in src:
                        inputs.add(src)
                        g.add_node(src, id=src, app='$$input')
                        g.add_edge(src, step['id'], out_id='io', inp_id=inp_id)
                    else:
                        src_id, out_id = src.split('.')
                        g.add_edge(src_id, step['id'], out_id=out_id, inp_id=inp_id)
            for out_id, dst_list in step.get('outputs', {}).iteritems():
                dst_list = dst_list if isinstance(dst_list, list) else filter(None, [dst_list])
                for dst in dst_list:
                    # assert '.' not in dst, 'output contains dot'
                    if '.' in dst:
                        continue
                    outputs.add(dst)
                    g.add_node(dst, id=dst, app='$$output')
                    g.add_edge(step['id'], dst, out_id=out_id, inp_id='io')
        step_ids = set(s['id'] for s in self.steps)
        assert not inputs.intersection(step_ids), 'Some inputs have same id as steps'
        assert not outputs.intersection(step_ids), 'Some outputs have same id as steps'
        assert nx.is_directed_acyclic_graph(g), 'Cycles in pipeline'
        self.nx = g
        return g

    def _validate(self):
        self._check_field('apps', dict, null=False)
        self._check_field('steps', list, null=False)
        for step in self.steps:
            self._check_field('id', basestring, null=False, look_in=step)
            self._check_field('app', basestring, null=False, look_in=step)
            assert step['app'] in self['apps'], '%s app not specified' % step['app']
        for app in self['apps'].itervalues():
            app._validate()
        assert self.apps, 'No apps'
        assert self.steps, 'No steps'
        self._build_nx()

    def get_app_for_step(self, step_or_id):
        if isinstance(step_or_id, basestring):
            step_or_id = filter(lambda s: s['id'] == step_or_id, self.steps)[0]
        return self['apps'][step_or_id['app']]

    def get_inputs(self):
        """
        Returns a dict that maps input names to dict objects obtained from the input schema of referenced apps.
        Incoming connections to steps with no '.' in identifier (not outputs of steps) are assumed to be inputs.

        Note: There is no validation performed here. Assuming pipeline is in the correct format and schema is valid.
        """
        to_list = lambda o: o if isinstance(o, list) else [] if o is None else [o]
        inputs = {}
        for step in self['steps']:
            for app_inp_id, incoming in step['inputs'].iteritems():
                incoming = to_list(incoming)
                for conn in incoming:
                    if '.' in conn:
                        continue
                    # look in the app to get the schema for this input
                    app_inputs = self['apps'][step['app']].schema.inputs
                    schema = filter(lambda i: i['id'] == app_inp_id, app_inputs)[0]
                    if conn not in inputs:
                        inputs[conn] = deepcopy(schema)
                    else:
                        # Input goes to multiple steps, check again for list/required
                        inp = inputs[conn]
                        if schema.get('required'):
                            inp['required'] = True
                        if not schema.get('list'):
                            inp['list'] = False
        return inputs


class DockerApp(Model):
    TYPE = 'app/tool/docker'

    image_ref = property(lambda self: self['docker_image_ref'])
    wrapper_id = property(lambda self: self['wrapper_id'])
    schema = property(lambda self: self['schema'])

    def _validate(self):
        self._check_field('docker_image_ref', dict, null=False)
        self._check_field('wrapper_id', basestring, null=False)
        self._check_field('schema', AppSchema, null=False)
        self.schema._validate()


class MockApp(Model):
    TYPE = 'app/mock/python'

    importable = property(lambda self: self['importable'])

    def _validate(self):
        self._check_field('importable', basestring, null=False)
        chunks = self['importable'].split('.')
        assert len(chunks) > 1, 'importable cannot be a module'
        for chunk in chunks:
            assert not iskeyword(chunk), '"%s" is a Python keyword' % chunk
            assert re.match('^[A-Za-z_][A-Za-z0-9_]*$', chunk), '"%s" is not a valid Python identifier' % chunk


class AppSchema(Model):
    TYPE = 'schema/app/sbgsdk'

    inputs = property(lambda self: self['inputs'])
    params = property(lambda self: self['params'])
    outputs = property(lambda self: self['outputs'])

    def _check_is_list_and_has_unique_ids(self, field, required_element_fields=None):
        self._check_field(field, list, null=False)
        for el in self[field]:
            for el_field in ['id'] + (required_element_fields or []):
                self._check_field(el_field, basestring, null=False, look_in=el)
        assert len(set(el['id'] for el in self[field])) == len(self[field]), '%s IDs must be unique' % field

    def _validate(self):
        self._check_is_list_and_has_unique_ids('inputs')
        self._check_is_list_and_has_unique_ids('outputs')
        self._check_is_list_and_has_unique_ids('params', required_element_fields=['type'])
