try:
    from itertools import izip, imap
except ImportError:
    izip, imap = zip, map