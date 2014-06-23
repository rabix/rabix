import os
import logging

import jsonschema

from rabix.common import six
from rabix.common.errors import ValidationError
from rabix.common.loadsave import from_url

log = logging.getLogger(__name__)


class Model(dict):
    """Helper class to turn JSON objects with $$type fields to classes."""
    TYPE = None
    SCHEMA = None

    def __init__(self, obj):
        super(dict, self).__init__()
        self.update(obj)
        self['$$type'] = self.TYPE

    def _validate(self):
        """
        Override to validate. Raise AssertionError or ValidationError
        or return array of errors. Or return None.
        """
        return []

    def _check_field(self, field, field_type=None, null=True, look_in=None):
        obj = look_in or self
        assert field in obj, 'Must have a "%s" field' % field
        val = obj[field]
        if val is None:
            assert null, '%s cannot be null' % val
            return
        if field_type:
            assert isinstance(val, field_type), (
                '%s is %s, expected %s' % (val, val.__class__.__name__,
                                           field_type)
            )

    def validate(self):
        if self.SCHEMA:
            base_url = 'file://' + os.path.abspath(__file__)
            schema = from_url(self.SCHEMA, base_url)
            validator = jsonschema.Draft4Validator(schema)
            try:
                validator.validate(self)
            except jsonschema.ValidationError as e:
                raise ValidationError(six.text_type(e))
        try:
            errors = self._validate()
        except AssertionError as e:
            raise ValidationError(six.text_type(e))
        if errors:
            raise ValidationError('. '.join(errors))

    def __json__(self):
        return dict({'$$type': self.TYPE}, **self)

    @classmethod
    def from_dict(cls, obj):
        return cls(obj)
