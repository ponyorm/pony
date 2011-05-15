def raises_exception(exc_class, msg=None):
    def decorator(func):
        def wrapper(self, *args, **keyargs):
            try:
                func(self, *args, **keyargs)
                self.assert_(False, "expected exception %s wasn't raised" % exc_class.__name__)
            except exc_class, e:
                if not e.args: self.assertEqual(msg, None)
                elif msg is not None:
                    self.assertEqual(e.args[0], msg, "incorrect exception message. expected '%s', got '%s'" % (msg, e.args[0]))
        wrapper.__name__ = func.__name__
        return wrapper
    return decorator