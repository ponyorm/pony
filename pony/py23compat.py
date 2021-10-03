import sys, platform

PY2 = sys.version_info[0] == 2
PYPY = platform.python_implementation() == 'PyPy'
PYPY2 = PYPY and PY2
PY37 = sys.version_info[:2] >= (3, 7)

if PY2:
    from future_builtins import zip as izip, map as imap
    import __builtin__ as builtins
    import cPickle as pickle
    from cStringIO import StringIO
    xrange = xrange
    basestring = basestring
    unicode = unicode
    buffer = buffer
    int_types = (int, long)
    cmp = cmp

    def iteritems(dict):
        return dict.iteritems()

    def itervalues(dict):
        return dict.itervalues()

    def items_list(dict):
        return dict.items()

    def values_list(dict):
        return dict.values()

else:
    import builtins, pickle
    from io import StringIO
    izip, imap, xrange = zip, map, range
    basestring = str
    unicode = str
    buffer = bytes
    int_types = (int,)

    def cmp(a, b):
        return (a > b) - (a < b)

    def iteritems(dict):
        return iter(dict.items())

    def itervalues(dict):
        return iter(dict.values())

    def items_list(dict):
        return list(dict.items())

    def values_list(dict):
        return list(dict.values())

# Armin's recipe from http://lucumr.pocoo.org/2013/5/21/porting-to-python-3-redux/
def with_metaclass(meta, *bases):
    class metaclass(meta):
        __call__ = type.__call__
        __init__ = type.__init__
        def __new__(cls, name, this_bases, d):
            if this_bases is None:
                return type.__new__(cls, name, (), d)
            return meta(name, bases, d)
    return metaclass('temporary_class', None, {})
