import sys

PY2 = sys.version_info[0] == 2

if PY2:
    from itertools import izip, imap
else:
    izip, imap = zip, map

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
