from __future__ import absolute_import, print_function
from pony.py23compat import PY2, imap, basestring, unicode

import re, os.path, sys, inspect, types, warnings

from datetime import datetime
from itertools import count as _count
from inspect import isfunction
from time import strptime
from collections import defaultdict
from functools import update_wrapper
from xml.etree import cElementTree

import pony
from pony import options

from pony.thirdparty.compiler import ast
from pony.thirdparty.decorator import decorator as _decorator

if pony.MODE.startswith('GAE-'): localbase = object
else: from threading import local as localbase


class PonyDeprecationWarning(DeprecationWarning):
    pass

def deprecated(stacklevel, message):
    warnings.warn(message, PonyDeprecationWarning, stacklevel)

warnings.simplefilter('once', PonyDeprecationWarning)

def _improved_decorator(caller, func):
    if isfunction(func):
        return _decorator(caller, func)
    def pony_wrapper(*args, **kwargs):
        return caller(func, *args, **kwargs)
    return pony_wrapper

def decorator(caller, func=None):
    if func is not None:
        return _improved_decorator(caller, func)
    def new_decorator(func):
        return _improved_decorator(caller, func)
    if isfunction(caller):
        update_wrapper(new_decorator, caller)
    return new_decorator

def decorator_with_params(dec):
    def parameterized_decorator(*args, **kwargs):
        if len(args) == 1 and isfunction(args[0]) and not kwargs:
            return decorator(dec(), args[0])
        return decorator(dec(*args, **kwargs))
    return parameterized_decorator

@decorator
def cut_traceback(func, *args, **kwargs):
    if not (pony.MODE == 'INTERACTIVE' and options.CUT_TRACEBACK):
        return func(*args, **kwargs)

    try: return func(*args, **kwargs)
    except AssertionError: raise
    except Exception:
        exc_type, exc, tb = sys.exc_info()
        last_pony_tb = None
        try:
            while tb.tb_next:
                module_name = tb.tb_frame.f_globals['__name__']
                if module_name == 'pony' or (module_name is not None  # may be None during import
                                             and module_name.startswith('pony.')):
                    last_pony_tb = tb
                tb = tb.tb_next
            if last_pony_tb is None: raise
            if tb.tb_frame.f_globals.get('__name__') == 'pony.utils' and tb.tb_frame.f_code.co_name == 'throw':
                reraise(exc_type, exc, last_pony_tb)
            raise exc  # Set "pony.options.CUT_TRACEBACK = False" to see full traceback
        finally:
            del exc, tb, last_pony_tb

if PY2:
    exec('''def reraise(exc_type, exc, tb):
    try: raise exc_type, exc, tb
    finally: del tb''')
else:
    def reraise(exc_type, exc, tb):
        try: raise exc.with_traceback(tb)
        finally: del exc, tb

def throw(exc_type, *args, **kwargs):
    if isinstance(exc_type, Exception):
        assert not args and not kwargs
        exc = exc_type
    else: exc = exc_type(*args, **kwargs)
    exc.__cause__ = None
    try:
        if not (pony.MODE == 'INTERACTIVE' and options.CUT_TRACEBACK):
            raise exc
        else:
            raise exc  # Set "pony.options.CUT_TRACEBACK = False" to see full traceback
    finally: del exc

def truncate_repr(s, max_len=100):
    s = repr(s)
    return s if len(s) <= max_len else s[:max_len-3] + '...'

lambda_args_cache = {}

def get_lambda_args(func):
    names = lambda_args_cache.get(func)
    if names is not None: return names
    if type(func) is types.FunctionType:
        if hasattr(inspect, 'signature'):
            names, argsname, kwname, defaults = [], None, None, None
            for p in inspect.signature(func).parameters.values():
                if p.default is not p.empty:
                    defaults.append(p.default)

                if p.kind == p.POSITIONAL_OR_KEYWORD:
                    names.append(p.name)
                elif p.kind == p.VAR_POSITIONAL:
                    argsname = p.name
                elif p.kind == p.VAR_KEYWORD:
                    kwname = p.name
                elif p.kind == p.POSITIONAL_ONLY:
                    throw(TypeError, 'Positional-only arguments like %s are not supported' % p.name)
                elif p.kind == p.KEYWORD_ONLY:
                    throw(TypeError, 'Keyword-only arguments like %s are not supported' % p.name)
                else: assert False
        else:
            names, argsname, kwname, defaults = inspect.getargspec(func)
    elif isinstance(func, ast.Lambda):
        names = func.argnames
        if func.kwargs: names, kwname = names[:-1], names[-1]
        else: kwname = None
        if func.varargs: names, argsname = names[:-1], names[-1]
        else: argsname = None
        defaults = func.defaults
    else: assert False  # pragma: no cover
    if argsname: throw(TypeError, '*%s is not supported' % argsname)
    if kwname: throw(TypeError, '**%s is not supported' % kwname)
    if defaults: throw(TypeError, 'Defaults are not supported')
    lambda_args_cache[func] = names
    return names

def error_method(*args, **kwargs):
    raise TypeError()

_ident_re = re.compile(r'^[A-Za-z_]\w*\Z')

# is_ident = ident_re.match
def is_ident(string):
    'is_ident(string) -> bool'
    return bool(_ident_re.match(string))

_name_parts_re = re.compile(r'''
            [A-Z][A-Z0-9]+(?![a-z]) # ACRONYM
        |   [A-Z][a-z]*             # Capitalized or single capital
        |   [a-z]+                  # all-lowercase
        |   [0-9]+                  # numbers
        |   _+                      # underscores
        ''', re.VERBOSE)

def split_name(name):
    "split_name('Some_FUNNYName') -> ['Some', 'FUNNY', 'Name']"
    if not _ident_re.match(name):
        raise ValueError('Name is not correct Python identifier')
    list = _name_parts_re.findall(name)
    if not (list[0].strip('_') and list[-1].strip('_')):
        raise ValueError('Name must not starting or ending with underscores')
    return [ s for s in list if s.strip('_') ]

def uppercase_name(name):
    "uppercase_name('Some_FUNNYName') -> 'SOME_FUNNY_NAME'"
    return '_'.join(s.upper() for s in split_name(name))

def lowercase_name(name):
    "uppercase_name('Some_FUNNYName') -> 'some_funny_name'"
    return '_'.join(s.lower() for s in split_name(name))

def camelcase_name(name):
    "uppercase_name('Some_FUNNYName') -> 'SomeFunnyName'"
    return ''.join(s.capitalize() for s in split_name(name))

def mixedcase_name(name):
    "mixedcase_name('Some_FUNNYName') -> 'someFunnyName'"
    list = split_name(name)
    return list[0].lower() + ''.join(s.capitalize() for s in list[1:])

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

def absolutize_path(filename, frame_depth):
    if is_absolute_path(filename): return filename
    code_filename = sys._getframe(frame_depth+1).f_code.co_filename
    if not is_absolute_path(code_filename):
        if code_filename.startswith('<') and code_filename.endswith('>'):
            if pony.MODE == 'INTERACTIVE': raise ValueError(
                'When in interactive mode, please provide absolute file path. Got: %r' % filename)
            raise EnvironmentError('Unexpected module filename, which is not absolute file path: %r' % code_filename)
    code_path = os.path.dirname(code_filename)
    return os.path.join(code_path, filename)

def current_timestamp():
    return datetime2timestamp(datetime.now())

def datetime2timestamp(d):
    result = d.isoformat(' ')
    if len(result) == 19: return result + '.000000'
    return result

def timestamp2datetime(t):
    time_tuple = strptime(t[:19], '%Y-%m-%d %H:%M:%S')
    microseconds = int((t[20:26] + '000000')[:6])
    return datetime(*(time_tuple[:6] + (microseconds,)))

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
    if match is None: raise ValueError()
    start = pos
    i = match.lastindex
    if i == 1: pos = match.end()  # identifier
    elif i == 2: z = 2  # "("
    else: assert False  # pragma: no cover
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
            else: assert False  # pragma: no cover
            while True:
                match = expr3_re.search(s, pos)
                if match is None: raise ValueError()
                pos = match.end()
                x = match.group()
                if x == open: counter += 1
                elif x == close:
                    counter -= 1
                    if not counter: z += 1; break
        else: assert False  # pragma: no cover

def tostring(x):
    if isinstance(x, basestring): return x
    if hasattr(x, '__unicode__'):
        try: return unicode(x)
        except: pass
    if hasattr(x, 'makeelement'): return cElementTree.tostring(x)
    try: return str(x)
    except: pass
    try: return repr(x)
    except: pass
    if type(x) == types.InstanceType: return '<%s instance at 0x%X>' % (x.__class__.__name__)
    return '<%s object at 0x%X>' % (x.__class__.__name__)

def strjoin(sep, strings, source_encoding='ascii', dest_encoding=None):
    "Can join mix of unicode and byte strings in different encodings"
    strings = list(strings)
    try: return sep.join(strings)
    except UnicodeDecodeError: pass
    for i, s in enumerate(strings):
        if isinstance(s, str):
            strings[i] = s.decode(source_encoding, 'replace').replace(u'\ufffd', '?')
    result = sep.join(strings)
    if dest_encoding is None: return result
    return result.encode(dest_encoding, 'replace')

def count(*args, **kwargs):
    if kwargs: return _count(*args, **kwargs)
    if len(args) != 1: return _count(*args)
    arg = args[0]
    if hasattr(arg, 'count'): return arg.count()
    try: it = iter(arg)
    except TypeError: return _count(arg)
    return len(set(it))

def avg(iter):
    count = 0
    sum = 0.0
    for elem in iter:
        if elem is None: continue
        sum += elem
        count += 1
    if not count: return None
    return sum / count

def distinct(iter):
    d = defaultdict(int)
    for item in iter:
        d[item] = d[item] + 1
    return d

def concat(*args):
    return ''.join(tostring(arg) for arg in args)

def is_utf8(encoding):
    return encoding.upper().replace('_', '').replace('-', '') in ('UTF8', 'UTF', 'U8')
