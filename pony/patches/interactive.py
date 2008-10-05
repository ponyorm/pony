import re, sys, __builtin__, locale, traceback, code

from pony import options

escape_re = re.compile(r'''
    (?<!\\)\\         # single backslash
    (?:
        x[0-9a-f]{2}  # byte escaping
    |   u[0-9a-f]{4}  # unicode escaping
    |   U[0-9a-f]{8}  # long unicode escaping
    )  
    ''', re.VERBOSE)

def restore_escapes(s, encoding=None):
    encoding = (encoding or getattr(sys.stderr, 'encoding', None)
                         or getattr(sys.stdout, 'encoding', None)
                         or options.CONSOLE_ENCODING
                         or locale.getpreferredencoding())
    def f(match):
        esc = match.group()
        code = int(esc[2:], 16)
        if esc.startswith('\\x'):
            if code < 32: return esc
            return chr(code)
        char = unichr(code)
        try: return char.encode(encoding)
        except UnicodeEncodeError: return esc
    return escape_re.sub(f, s)

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
