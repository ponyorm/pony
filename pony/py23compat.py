import sys

PY2 = sys.version_info[0] == 2

if PY2:
    from future_builtins import zip as izip, map as imap
    xrange = xrange
    basestring = basestring
    cmp = cmp

    def iteritems(dict):
        return dict.iteritems()

    def itervalues(dict):
        return dict.itervalues()

else:
    izip, imap, xrange = zip, map, range
    basestring = str

    def cmp(a, b):
        return (a > b) - (a < b)

    def iteritems(dict):
        return iter(dict.items())

    def itervalues(dict):
        return iter(dict.values())
