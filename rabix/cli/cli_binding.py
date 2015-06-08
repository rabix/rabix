__author__ = 'luka'

class CommandLineBinding(object):

    def __init__(self, position, prefix, separate, item_separator, value_from):
        self.position = position
        self.prefix = prefix
        self.separate = separate
        self.item_separator = item_separator
        self.value_from = value_from

    def to_dict(self, context=None):
        return {
            'position': self.position,
            'prefix': self.prefix,
            'separate': self.separate,
            'itemSeparator': self.item_separator,
            'valueFrom': self.value_from
        }

    @classmethod
    def from_dict(cls, context, d):
        return cls()