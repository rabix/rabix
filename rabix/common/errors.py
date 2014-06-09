class ValidationError(RuntimeError):
    def __init__(self, message):
        super(ValidationError, self).__init__(message)


class ResourceUnavailable(RuntimeError):
    def __init__(self, message):
        super(ResourceUnavailable, self).__init__(message)
