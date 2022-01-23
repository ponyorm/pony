import sys, platform

PYPY = platform.python_implementation() == 'PyPy'
PY37 = sys.version_info[:2] >= (3, 7)
PY38 = sys.version_info[:2] >= (3, 8)
PY39 = sys.version_info[:2] >= (3, 9)
PY310 = sys.version_info[:2] >= (3, 10)

unicode = str
buffer = bytes
int_types = (int,)

def cmp(a, b):
    return (a > b) - (a < b)
