import sys, __builtin__, traceback, code

from pony.utils import restore_escapes

def displayhook(x):
    if x is None: return
    __builtin__._ = None
    print restore_escapes(repr(x))
    __builtin__._ = x

old_displayhook = sys.displayhook
sys.displayhook = displayhook

def excepthook(type, value, tb):
    s = ''.join(traceback.format_exception(type, value, tb))
    s = restore_escapes(s)
    sys.stderr.write(s)

old_excepthook = sys.excepthook
sys.excepthook = excepthook

def write(self, data):  # because displayhook does not work in PythonWin 
    data = restore_escapes(data)
    sys.stderr.write(data)
code.InteractiveInterpreter.write = write
