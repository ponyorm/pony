import sys

PY2 = sys.version_info[0] == 2

if PY2:
    from future_builtins import zip as izip, map as imap
    xrange = xrange
else:
    izip, imap, xrange = zip, map, range

if PY2:
    def iteritems(dict):
        return dict.iteritems()

    def itervalues(dict):
        return dict.itervalues()

else:
    def iteritems(dict):
        return iter(dict.items())

    def itervalues(dict):
        return iter(dict.values())
