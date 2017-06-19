from __future__ import unicode_literals
from pony.py23compat import PY2

class CircularDependencyError(Exception):
    """
    Raised when there's an impossible-to-resolve circular dependency.
    """
    pass


class NodeNotFoundError(LookupError):
    """
    Raised when an attempt on a node is made that is not available in the graph.
    """
    def __init__(self, message, node, origin=None):
        self.message = message
        self.origin = origin
        self.node = node

    def __unicode__(self):
        return self.message

    def __str__(self):
        return self.message.encode('utf-8') if PY2 else self.message

    def __repr__(self):
        return "NodeNotFoundError(%r)" % (self.node, )

class MergeAborted(Exception):
    pass
