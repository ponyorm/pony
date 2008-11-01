import re, os, os.path, sys, time, datetime

from itertools import imap, ifilter
from operator import itemgetter
from inspect import isfunction
from time import strptime
from os import urandom
from codecs import BOM_UTF8, BOM_LE, BOM_BE
from locale import getpreferredencoding
from linecache import getlines

import pony
from pony import options

class ValidationError(ValueError):
    def __init__(self, err_msg=None):
        ValueError.__init__(self, err_msg)
        self.err_msg = err_msg

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
    mod = __import__(name)
    components = name.split('.')
    for comp in components[1:]: mod = getattr(mod, comp)
    return mod

def absolutize_path(filename, frame_depth=2):
    code_filename = sys._getframe(frame_depth).f_code.co_filename
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
    for i, line in enumerate(getlines(filename)):
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
        console_encoding = ( getattr(sys.stderr, 'encoding', None)
                             or getattr(sys.stdout, 'encoding', None)
                             or options.CONSOLE_ENCODING
                             or getpreferredencoding() )
    try: s = s.decode(source_encoding).encode(console_encoding)
    except UnicodeDecodeError, UnicodeEncodeError: pass
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
    result = datetime.datetime.now().isoformat(' ')
    if len(result) == 19: return result + '.000000'
    return result

def datetime2timestamp(d):
    result = d.isoformat(' ')
    if len(result) == 19: return result + '.000000'
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
        return text.decode(encoding or getpreferredencoding())

def compress(s):
    zipped = s.encode('zip')
    if len(zipped) < len(s): return 'Z' + zipped
    return 'N' + s

def decompress(s):
    first = s[0]
    if first == 'N': return s[1:]
    elif first == 'Z': return s[1:].decode('zip')
    raise ValueError('Incorrect data')

def markdown(s, escape_html=True):
    from pony.templating import Html, StrHtml, quote
    from pony.thirdparty.markdown import markdown
    if escape_html: s = quote(s)
    # if isinstance(s, str): s = str.__str__(s)
    # elif isinstance(s, unicode): s = unicode(s)
    return Html(markdown(s))

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
    match = expr1_re.match(s, pos)
    if match is None: raise ValueError
    start = pos
    i = match.lastindex
    if i == 1: pos = match.end()  # identifier
    elif i == 2: pass  # "("
    else: assert False
    while True:
        match = expr2_re.match(s, pos)
        if match is None: return s[start:pos]
        pos = match.end()
        i = match.lastindex
        if i == 1: return s[start:pos]  # ";" - explicit end of expression
        elif i == 2: pass  # .identifier
        elif i == 3:  # "(" or "["
            pos = match.end()
            counter = 1
            open = match.group(i)
            if open == '(': close = ')'
            elif open == '[': close = ']'
            else: assert False
            while True:
                match = expr3_re.search(s, pos)
                if match is None: raise ValueError
                pos = match.end()
                x = match.group()
                if x == open: counter += 1
                elif x == close:
                    counter -= 1
                    if not counter: break
        else: assert False

def tostring(x):
    if isinstance(x, basestring): return x
    if hasattr(x, '__unicode__'): return unicode(x)
    return str(x)
        