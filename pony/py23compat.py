import sys

PY2 = sys.version_info[0] == 2

if PY2:
    from itertools import izip, imap
else:
    izip, imap = zip, map

if PY2:
    def itervalues(dict):
        return dict.itervalues()

else:
    def itervalues(dict):
        return iter(dict.values())
