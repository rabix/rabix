import six


class RabixError(BaseException):
    def __init__(self, message=''):
        super(RabixError, self).__init__(message)
        self.message = message


class ValidationError(RabixError):
    def __init__(self, message):
        super(ValidationError, self).__init__(message)


class ResourceUnavailable(RabixError):
    def __init__(self, uri, message='', cause=None):
        msg = 'Unable to load "%s".' % uri
        if message:
            msg = ' '.join([msg, message])
        if cause:
            msg = ' '.join([msg, 'Reason:', six.text_type(cause)])
        super(ResourceUnavailable, self).__init__(msg)
        self.__cause__ = cause
        self.uri = uri
