import sys, platform

PYPY = platform.python_implementation() == 'PyPy'
PY37 = sys.version_info[:2] >= (3, 7)
PY38 = sys.version_info[:2] >= (3, 8)
PY39 = sys.version_info[:2] >= (3, 9)
PY310 = sys.version_info[:2] >= (3, 10)

import builtins, pickle
from io import StringIO
basestring = str
unicode = str
buffer = bytes
int_types = (int,)

def cmp(a, b):
    return (a > b) - (a < b)

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
