

class cached_property(object):
    """
    A property that is only computed once per instance and then replaces itself
    with an ordinary attribute. Deleting the attribute resets the property.
    Source: https://github.com/bottlepy/bottle/commit/fa7733e075da0d790d809aa3d2f53071897e6f76
    """  # noqa

    def __init__(self, func):
        self.__doc__ = getattr(func, '__doc__')
        self.func = func

    def __get__(self, obj, cls):
        if obj is None:
            return self
        value = obj.__dict__[self.func.__name__] = self.func(obj)
        return value


class class_property(object):
    """
    Read-only class property
    """

    def __init__(self, func):
        self.func = func

    def __get__(self, instance, cls):
        return self.func(cls)

class class_cached_property(object):

    def __init__(self, func):
        self.func = func

    def __get__(self, obj, cls):
        value = self.func(cls)
        setattr(cls, self.func.__name__, value)
        return value