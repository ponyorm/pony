#coding: cp1251

import re, os, os.path, sys, time, datetime, types, linecache

from itertools import imap, ifilter
from operator import itemgetter
from inspect import isfunction
from time import strptime
from os import urandom
from codecs import BOM_UTF8, BOM_LE, BOM_BE
from locale import getpreferredencoding
from bisect import bisect

import pony
from pony import options

try: from pony.thirdparty import etree
except ImportError: etree = None

if pony.MODE.startswith('GAE-'): localbase = object
else: from threading import local as localbase

def copy_func_attrs(new_func, old_func, decorator_name=None):
    if new_func is not old_func:
        new_func.__name__ = old_func.__name__
        new_func.__doc__ = old_func.__doc__
        new_func.__module__ = old_func.__module__
        d = old_func.__dict__.copy()
        d.update(new_func.__dict__)
        new_func.__dict__.update(d)
        if not hasattr(old_func, 'original_func'):
            new_func.original_func = old_func
    if not hasattr(new_func, 'decorators'):
        new_func.decorators = getattr(old_func, 'decorators', set()).copy()
    if decorator_name: new_func.decorators.add(decorator_name)
    return new_func

def simple_decorator(old_dec):
    def new_dec(old_func):
        def new_func(*args, **keyargs):
            return old_dec(old_func, *args, **keyargs)
        return copy_func_attrs(new_func, old_func, old_dec.__name__)
    return copy_func_attrs(new_dec, old_dec, 'simple_decorator')

@simple_decorator
def decorator(old_dec, old_func):
    new_func = old_dec(old_func)
    return copy_func_attrs(new_func, old_func, old_dec.__name__)

@simple_decorator
def decorator_with_params(old_dec, *args, **keyargs):
    if len(args) == 1 and isfunction(args[0]) and not keyargs:
        old_func = args[0]
        new_func = old_dec(old_func)
        return copy_func_attrs(new_func, old_func, old_dec.__name__)
    def you_should_never_see_this_decorator(old_func):
        new_func = old_dec(old_func, *args, **keyargs)
        return copy_func_attrs(new_func, old_func, old_dec.__name__)
    return you_should_never_see_this_decorator

@decorator
def cut_traceback(old_func):
    def new_func(*args, **keyargs):
        if not (pony.MODE == 'INTERACTIVE' and options.CUT_TRACEBACK):
            return old_func(*args, **keyargs)
        try:
            return old_func(*args, **keyargs)
        except Exception:
            exc_type, exc, tb = sys.exc_info()
            last_pony_tb = None
            try:
                while tb.tb_next:
                    module_name = tb.tb_frame.f_globals['__name__']
                    if module_name == 'pony' or module_name.startswith('pony.'):
                        last_pony_tb = tb
                    tb = tb.tb_next
                assert last_pony_tb
                if tb.tb_frame.f_globals['__name__'] == 'pony.utils' and tb.tb_frame.f_code.co_name == 'throw':
                    raise exc_type, exc, last_pony_tb
                raise exc  # Set "pony.options.CUT_TRACEBACK = False" to see full traceback
            finally:
                del tb, last_pony_tb
    return new_func

def throw(exc_type, *args, **keyargs):
    if isinstance(exc_type, Exception):
        assert not args and not keyargs
        exc = exc_type
    else: exc = exc_type(*args, **keyargs)
    if not (pony.MODE == 'INTERACTIVE' and options.CUT_TRACEBACK):
        raise exc
    else:
        raise exc  # Set "pony.options.CUT_TRACEBACK = False" to see full traceback

_cache = {}
MAX_CACHE_SIZE = 1000

@simple_decorator
def cached(f, *args, **keyargs):
    key = (f, args, tuple(sorted(keyargs.items())))
    value = _cache.get(key)
    if value is not None: return value
    if len(_cache) == MAX_CACHE_SIZE: _cache.clear()
    return _cache.setdefault(key, f(*args, **keyargs))

def error_method(*args, **kwargs):
    raise TypeError

_ident_re = re.compile(r'^[A-Za-z_]\w*\Z')

# is_ident = ident_re.match
def is_ident(string):
    'is_ident(string) -> bool'
    return bool(_ident_re.match(string))

def import_module(name):
    "import_module('a.b.c') -> <module a.b.c>"
    mod = sys.modules.get(name)
    if mod is not None: return mod
    mod = __import__(name)
    components = name.split('.')
    for comp in components[1:]: mod = getattr(mod, comp)
    return mod

if sys.platform == 'win32':
      _absolute_re = re.compile(r'^(?:[A-Za-z]:)?[\\/]')
else: _absolute_re = re.compile(r'^/')

def is_absolute_path(filename):
    return bool(_absolute_re.match(filename))

def absolutize_path(filename, frame_depth=2):
    if is_absolute_path(filename): return filename
    code_filename = sys._getframe(frame_depth).f_code.co_filename
    if not is_absolute_path(code_filename):
        if code_filename.startswith('<') and code_filename.endswith('>'):
            if pony.MODE == 'INTERACTIVE': raise ValueError(
                'When in interactive mode, please provide absolute file path. Got: %r' % filename)
            raise EnvironmentError('Unexpected module filename, which is not absolute file path: %r' % code_filename)
    code_path = os.path.dirname(code_filename)
    return os.path.join(code_path, filename)

def shortened_filename(filename):
    if pony.MAIN_DIR is None: return filename
    maindir = pony.MAIN_DIR + os.sep
    if filename.startswith(maindir): return filename[len(maindir):]
    return filename

def get_mtime(filename):
    stat = os.stat(filename)
    mtime = stat.st_mtime
    if sys.platform == "win32": mtime -= stat.st_ctime
    return mtime

coding_re = re.compile(r'coding[:=]\s*([-\w.]+)')

def detect_source_encoding(filename):
    for i, line in enumerate(linecache.getlines(filename)):
        if i == 0 and line.startswith(BOM_UTF8): return 'utf-8'
        if not line.lstrip().startswith('#'): continue
        match = coding_re.search(line)
        if match is not None: return match.group(1)
    else: return options.SOURCE_ENCODING or getpreferredencoding()

escape_re = re.compile(r'''
    (?<!\\)\\         # single backslash
    (?:
        x[0-9a-f]{2}  # byte escaping
    |   u[0-9a-f]{4}  # unicode escaping
    |   U[0-9a-f]{8}  # long unicode escaping
    )  
    ''', re.VERBOSE)

def restore_escapes(s, console_encoding=None, source_encoding=None):
    if not options.RESTORE_ESCAPES: return s
    if source_encoding is None:
        source_encoding = options.SOURCE_ENCODING or getpreferredencoding()
    if console_encoding is None:
        try: console_encoding = getattr(sys.stderr, 'encoding', None)
        except: console_encoding = None  # workaround for PythonWin win32ui.error "The MFC object has died."
        console_encoding = console_encoding or options.CONSOLE_ENCODING
        console_encoding = console_encoding or getpreferredencoding()
    try: s = s.decode(source_encoding).encode(console_encoding)
    except (UnicodeDecodeError, UnicodeEncodeError): pass
    def f(match):
        esc = match.group()
        code = int(esc[2:], 16)
        if esc.startswith('\\x'):
            if code < 32: return esc
            try: return chr(code).decode(source_encoding).encode(console_encoding)
            except (UnicodeDecodeError, UnicodeEncodeError): return esc
        char = unichr(code)
        try: return char.encode(console_encoding)
        except UnicodeEncodeError: return esc
    return escape_re.sub(f, s)

def current_timestamp():
    return datetime2timestamp(datetime.datetime.now())

def datetime2timestamp(d):
    result = d.isoformat(' ')
    if len(result) == 19: return result + '.000'
    elif len(result) > 23: return result[:23]
    return result

def timestamp2datetime(t):
    time_tuple = strptime(t[:19], '%Y-%m-%d %H:%M:%S')
    microseconds = int((t[20:26] + '000000')[:6])
    return datetime.datetime(*(time_tuple[:6] + (microseconds,)))

def read_text_file(fname, encoding=None):
    text = file(fname).read()
    for bom, enc in [ (BOM_UTF8, 'utf8'), (BOM_LE, 'utf-16le'), (BOM_BE, 'utf-16be') ]:
        if text[:len(bom)] == bom: return text[len(bom):].decode(enc)
    try: return text.decode('utf8')
    except UnicodeDecodeError:
        try: return text.decode(encoding or getpreferredencoding())
        except UnicodeDecodeError:
            return text.decode('ascii', 'replace')

def compress(s):
    zipped = s.encode('zip')
    if len(zipped) < len(s): return 'Z' + zipped
    return 'N' + s

def decompress(s):
    first = s[0]
    if first == 'N': return s[1:]
    elif first == 'Z': return s[1:].decode('zip')
    raise ValueError('Incorrect data')

nbsp_re = re.compile(ur"\s+(и|с|в|от)\s+")

def markdown(s):
    from pony.templating import Html, quote
    from pony.thirdparty.markdown import markdown
    s = quote(s)[:]
    result = markdown(s, html4tags=True)
    result = nbsp_re.sub(r" \1&nbsp;", result)
    return Html(result)

class JsonString(unicode): pass

def json(obj, **keyargs):
    from pony.thirdparty import simplejson
    result = JsonString(simplejson.dumps(obj, **keyargs))
    result.media_type = 'application/json'
    if 'encoding' in keyargs: result.charset = keyargs['encoding']
    return result

def new_guid():
    'new_guid() -> new_binary_guid'
    return buffer(urandom(16))

def guid2str(guid):
    """guid_binary2str(binary_guid) -> string_guid

    >>> guid2str(unxehlify('ff19966f868b11d0b42d00c04fc964ff'))
    '6F9619FF-8B86-D011-B42D-00C04FC964FF'
    """
    assert isinstance(guid, buffer) and len(guid) == 16
    guid = str(guid)
    return '%s-%s-%s-%s-%s' % tuple(map(hexlify, (
        guid[3::-1], guid[5:3:-1], guid[7:5:-1], guid[8:10], guid[10:])))

def str2guid(s):
    """guid_str2binary(str_guid) -> binary_guid

    >>> unhexlify(str2guid('6F9619FF-8B86-D011-B42D-00C04FC964FF'))
    'ff19966f868b11d0b42d00c04fc964ff'
    """
    assert isinstance(s, basestring) and len(s) == 36
    a, b, c, d, e = map(unhexlify, (s[:8],s[9:13],s[14:18],s[19:23],s[24:]))
    reverse = slice(-1, None, -1)
    return buffer(''.join((a[reverse], b[reverse], c[reverse], d, e)))

expr1_re = re.compile(r'''
        ([A-Za-z_]\w*)  # identifier (group 1)
    |   ([(])           # open parenthesis (group 2)
    ''', re.VERBOSE)

expr2_re = re.compile(r'''
     \s*(?:
            (;)                 # semicolon (group 1)
        |   (\.\s*[A-Za-z_]\w*) # dot + identifier (group 2)
        |   ([([])              # open parenthesis or braces (group 3)
        )
    ''', re.VERBOSE)

expr3_re = re.compile(r"""
        [()[\]]                   # parenthesis or braces (group 1)
    |   '''(?:[^\\]|\\.)*?'''     # '''triple-quoted string'''
    |   \"""(?:[^\\]|\\.)*?\"""   # \"""triple-quoted string\"""
    |   '(?:[^'\\]|\\.)*?'        # 'string'
    |   "(?:[^"\\]|\\.)*?"        # "string"
    """, re.VERBOSE)

def parse_expr(s, pos=0):
    z = 0
    match = expr1_re.match(s, pos)
    if match is None: raise ValueError
    start = pos
    i = match.lastindex
    if i == 1: pos = match.end()  # identifier
    elif i == 2: z = 2  # "("
    else: assert False
    while True:
        match = expr2_re.match(s, pos)
        if match is None: return s[start:pos], z==1
        pos = match.end()
        i = match.lastindex
        if i == 1: return s[start:pos], False  # ";" - explicit end of expression
        elif i == 2: z = 2  # .identifier
        elif i == 3:  # "(" or "["
            pos = match.end()
            counter = 1
            open = match.group(i)
            if open == '(': close = ')'
            elif open == '[': close = ']'; z = 2
            else: assert False
            while True:
                match = expr3_re.search(s, pos)
                if match is None: raise ValueError
                pos = match.end()
                x = match.group()
                if x == open: counter += 1
                elif x == close:
                    counter -= 1
                    if not counter: z += 1; break
        else: assert False

def tostring(x):
    if isinstance(x, basestring): return x
    if hasattr(x, '__unicode__'):
        try: return unicode(x)
        except: pass
    if etree is not None and hasattr(x, 'makeelement'): return etree.tostring(x)
    try: return str(x)
    except: pass
    try: return repr(x)
    except: pass
    if type(x) == types.InstanceType: return '<%s instance at 0x%X>' % (x.__class__.__name__)
    return '<%s object at 0x%X>' % (x.__class__.__name__)

def make_offsets(s):
    offsets = [ 0 ]
    si = -1
    try:
        while True:
            si = s.index('\n', si + 1)
            offsets.append(si + 1)
    except ValueError: pass
    offsets.append(len(s))
    return offsets

def pos2lineno(pos, offsets):
    line = bisect(offsets, pos, 0, len(offsets)-1)
    if line == 1: offset = pos
    else: offset = pos - offsets[line - 1]
    return line, offset

def getline(text, offsets, lineno):
    return text[offsets[lineno-1]:offsets[lineno]]

def getlines(text, offsets, lineno, context=1):
    if context <= 0: return [], None
    start = max(0, lineno - 1 - context//2)
    end = min(len(offsets)-1, start + context)
    start = max(0, end - context)
    lines = []
    for i in range(start, end): lines.append(text[offsets[i]:offsets[i+1]])
    index = lineno - 1 - start
    return lines, index

def getlines2(filename, lineno, context=1):
    if context <= 0: return [], None
    lines = linecache.getlines(filename)
    if not lines: return [], None
    start = max(0, lineno - 1 - context//2)
    end = min(len(lines), start + context)
    start = max(0, end - context)
    lines = lines[start:start+context]
    index = lineno - 1 - start
    return lines, index

def reraise(exc_class, exceptions):
    try:
        cls, exc, tb = exceptions[0]
        msg = '%s: %s' % (cls.__name__, " ".join(tostring(arg) for arg in exc.args))
        raise exc_class, exc_class(msg, exceptions), tb
    finally: del tb

def avg(iter):
    count = 0
    sum = 0.0
    for elem in iter:
        if elem is None: continue
        sum += elem
        count += 1
    if not count: return None
    return sum / count

def is_utf8(encoding):
    return encoding.upper().replace('_', '').replace('-', '') in ('UTF8', 'UTF', 'U8')
