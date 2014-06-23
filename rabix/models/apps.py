import logging

from rabix.common import six
from rabix.models.base import Model

log = logging.getLogger(__name__)


class App(Model):
    schema = property(lambda self: self['schema'])
    apps = property(lambda self: {'app': self})

    def get_inputs(self):
        return {inp['id']: inp for inp in self.schema.inputs}


class AppSchema(Model):
    TYPE = 'schema/app/sbgsdk'
    SCHEMA = 'schema/sbgsdk.json'

    inputs = property(lambda self: self['inputs'])
    params = property(lambda self: self['params'])
    outputs = property(lambda self: self['outputs'])

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
