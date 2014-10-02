import sys

PY2 = sys.version_info[0] == 2

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
