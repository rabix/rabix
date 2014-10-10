import copy
import logging

import networkx as nx
import six

from rabix.common.errors import ValidationError
from rabix.models.base import Model
from rabix.models.apps import App

log = logging.getLogger(__name__)


class Pipeline(Model):
    TYPE = 'app/pipeline'
    SCHEMA = 'schema/pipeline.json'

    apps = property(lambda self: self.get('apps', {}))
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
            app = self.get_app_for_step(step)
            g.add_node(step['id'], step=step, app=app)
            for inp_id, src_list in six.iteritems(step.get('inputs', {})):
                src_list = (src_list if isinstance(src_list, list)
                            else [x for x in [src_list] if x])
                for src in src_list:
                    if '.' not in src:
                        inputs.add(src)
                        g.add_node(src, id=src, app='$$input')
                        conns = g.get_edge_data(src, step['id'],
                                                default={'conns': []})['conns']
                        conns.append([None, inp_id])
                        g.add_edge(src, step['id'], conns=conns)
                    else:
                        src_id, out_id = src.split('.')
                        conns = g.get_edge_data(src, step['id'],
                                                default={'conns': []})['conns']
                        conns.append([out_id, inp_id])
                        g.add_edge(src_id, step['id'], conns=conns)
            for out_id, dst_list in six.iteritems(step.get('outputs', {})):
                dst_list = (dst_list if isinstance(dst_list, list)
                            else [x for x in [dst_list] if x])
                for dst in dst_list:
                    if '.' in dst:
                        log.warn('Ignoring invalid output value: %s', dst)
                        continue
                    outputs.add(dst)
                    g.add_node(dst, id=dst, app='$$output')
                    conns = g.get_edge_data(step['id'], dst,
                                            default={'conns': []})['conns']
                    conns.append([out_id, None])
                    g.add_edge(step['id'], dst, conns=conns)
        step_ids = set(s['id'] for s in self.steps)
        assert not inputs.intersection(step_ids), (
            'Some inputs have same id as steps')
        assert not outputs.intersection(step_ids), (
            'Some outputs have same id as steps')
        assert nx.is_directed_acyclic_graph(g), 'Cycles in pipeline'
        self.nx = g
        return g

    def _validate(self):
        if 'apps' in self:
            self._check_field('apps', dict, null=False)
        self._check_field('steps', list, null=False)
        for step in self.steps:
            self._check_field(
                'id', six.string_types, null=False, look_in=step
            )
            self._check_field(
                'app', (six.string_types, App), null=False, look_in=step
            )
            self.get_app_for_step(step)
        for app in six.itervalues(self.apps):
            if not isinstance(app, App):
                raise ValidationError('Bad app: %s' % app)
            app.validate()
        assert self.steps, 'No steps'
        self._build_nx()

    def get_app_for_step(self, step_or_id):
        if isinstance(step_or_id, six.string_types):
            step_or_id = [s for s in self.steps if s['id'] == step_or_id][0]

        if isinstance(step_or_id['app'], App):
            return step_or_id['app']
        try:
            return self.apps[step_or_id['app']]
        except:
            raise ValidationError('No app for step %s' % step_or_id['id'])

    def get_inputs(self):
        """Returns a dict that maps input names to dict objects obtained from
        the input schema of referenced apps.

        Incoming connections to steps with no '.' in identifier (not outputs
        of steps) are assumed to be inputs.

        Note: There is no validation performed here. Assuming pipeline is in
        the correct format and schema is valid.
        """
        to_list = lambda o: (o if isinstance(o, list)
                             else [] if o is None else [o])
        inputs = {}
        for step in self['steps']:
            for app_inp_id, incoming in six.iteritems(step.get('inputs', {})):
                incoming = to_list(incoming)
                for conn in incoming:
                    if '.' in conn:
                        continue
                    # Look in the app to get the schema for this input
                    app = step['app']
                    if isinstance(app, six.string_types):
                        app = self.apps[app]
                    inp_desc = app.schema.describe_input(app_inp_id)
                    if conn not in inputs:
                        inputs[conn] = copy.deepcopy(inp_desc)
                    else:
                        # Input goes to multiple steps, check list/required:
                        inp = inputs[conn]
                        if inp_desc.get('required'):
                            inp['required'] = True
                        if not inp_desc.get('list'):
                            inp['list'] = False
        return inputs

    @classmethod
    def from_app(cls, app):
        if isinstance(app, Pipeline):
            app.validate()
            return app
        pipeline = cls({
            'apps': app.apps,
            'steps': [{
                'id': list(app.apps.keys())[0],
                'app': list(app.apps.keys())[0],
                'inputs': {
                    inp_id: 'inp_' + inp_id for inp_id in app.schema.inputs
                },
                'outputs': {
                    out_id: 'out_' + out_id for out_id in app.schema.outputs
                },
            }]
        })
        pipeline.validate()
        return pipeline
