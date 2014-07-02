import logging

from rabix.common import six
from rabix.models.base import Model

log = logging.getLogger(__name__)


class App(Model):
    schema = property(lambda self: self['schema'])
    apps = property(lambda self: {'app': self})


class AppSchema(Model):
    TYPE = 'schema/app/sbgsdk'
    SCHEMA = 'schema/sbgsdk.json'

    inputs = property(lambda self: [i['id'] for i in self.get('inputs', [])])
    params = property(lambda self: [i['id'] for i in self.get('outputs', [])])
    outputs = property(lambda self: [i['id'] for i in self.get('outputs', [])])

    def _check_is_list_and_has_unique_ids(self, field,
                                          required_element_fields=None):
        self._check_field(field, list, null=False)
        for el in self[field]:
            for el_field in ['id'] + (required_element_fields or []):
                self._check_field(
                    el_field, six.string_types, null=False, look_in=el
                )
        assert len(set(el['id'] for el in self[field])) == len(self[field]), (
            '%s IDs must be unique' % field)

    def _validate(self):
        self._check_is_list_and_has_unique_ids('inputs')
        self._check_is_list_and_has_unique_ids('outputs')
        self._check_is_list_and_has_unique_ids(
            'params', required_element_fields=['type']
        )

    def describe_input(self, inp_id):
        return [i for i in self.get('inputs', []) if i['id'] == inp_id][0]


class AppJsonSchema(AppSchema):
    TYPE = 'schema/app'
    SCHEMA = 'schema/ioschema.json'

    @property
    def inputs(self):
        return self.get('inputs', {}).get('properties', {}).keys()

    @property
    def params(self):
        return self.get('params', {}).get('properties', {}).keys()

    @property
    def outputs(self):
        return self.get('outputs', {}).get('properties', {}).keys()

    def _validate(self):
        pass

    def describe_input(self, inp_id):
        d = self['inputs']['properties'][inp_id]
        return {
            'name': d.get('title', inp_id),
            'description': d.get('description', ''),
            'required': inp_id in self['inputs']['required'],
            'list': d.get('type') in (['array', None], 'array'),
        }
