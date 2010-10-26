def raises_exception(exc_class, msg):
    def decorator(func):
        def wrapper(self, *args, **keyargs):
            try:
                func(self, *args, **keyargs)
                self.assert_(False, "expected exception %s wasn't raised" % exc_class.__name__)
            except exc_class, e:
                self.assertEqual(e.args[0], msg, "incorrect exception message. expected '%s', got '%s'"
                % (msg, e.message))
        wrapper.__name__ = func.__name__
        return wrapper
    return decorator