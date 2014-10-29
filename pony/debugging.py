from __future__ import absolute_import, print_function
from pony.py23compat import iteritems

import re, sys, os.path, threading, cStringIO, weakref, inspect, keyword, linecache, traceback

from repr import Repr
from itertools import count
from urllib import unquote_plus

import pony
from pony import options, utils, httputils
from pony.utils import detect_source_encoding, is_ident, tostring, pos2lineno, getlines, getlines2, decorator
from pony.templating import html, cycle, htmljoin, Html, StrHtml, ParseError

def restore_escapes(s):
    s = tostring(s)
    if not options.DEBUGGING_RESTORE_ESCAPES: return s
    return utils.restore_escapes(s, 'utf-8').decode('utf-8')

addr_re = re.compile(r' at 0x[0-9a-fA-F]{8}(?:[0-9a-fA-F]{8})?>')

class Repr1(Repr):
    def __init__(self):
        Repr.__init__(self)
        self.maxstring = 100
        self.maxother = 100

aRepr1 = Repr1()
repr1 = aRepr1.repr

class Repr2(Repr):
    def __init__(self):
        Repr.__init__(self)
        self.maxstring = 76
        self.maxother = 76
    def repr_str(self, x, level):
        s = repr(x[:self.maxstring])
        s = restore_escapes(s)
        if len(s) > self.maxstring:
            i = max(0, (self.maxstring-3)//2)
            j = max(0, self.maxstring-3-i)
            s = repr(x[:i] + x[len(x)-j:])
            s = restore_escapes(s)
            s = s[:i] + '...' + s[len(s)-j:]
        return s
    repr_unicode = repr_str
    def repr1(self, x, level):
        typename = type(x).__name__
        if ' ' in typename: typename = '_'.join(typename.split())
        method = getattr(self, 'repr_' + typename, None)
        if method is not None: return method(x, level)
        try: s = repr(x)  # Bugs in x.__repr__() can cause arbitrary exceptions
        except: s = '<%s object at 0x%X>' % (x.__class__.__name__, id(x))
        s = restore_escapes(s)
        if options.DEBUGGING_REMOVE_ADDR: s = addr_re.sub('>', s)
        return truncate(s, self.maxother)
    def repr_instance(self, x, level):
        try: s = repr(x)  # Bugs in x.__repr__() can cause arbitrary exceptions
        except: s = '<%s instance at 0x%X>' % (x.__class__.__name__, id(x))
        s = restore_escapes(s)
        if options.DEBUGGING_REMOVE_ADDR: s = addr_re.sub('>', s)
        return truncate(s, self.maxstring)

aRepr2 = Repr2()
repr2 = aRepr2.repr

def truncate(s, maxlen):
    if len(s) > maxlen:
        i = max(0, (maxlen-3)//2)
        j = max(0, maxlen-3-i)
        s = s[:i] + '...' + s[len(s)-j:]
    return s

class Record(object):
    def __init__(record, module, filename, lineno, lines, index, func=None):
        record.func = None

        record.module = module
        if module == '<template>': record.moduletype = 'template'
        elif module == 'pony' or module.startswith('pony.'): record.moduletype = 'module-system'
        else: record.moduletype = 'module-user'

        record.filename = filename
        if filename and filename != '<?>': record.fname = os.path.split(filename)[1]
        else: record.fname = None
        record.lineno = lineno
        record.lines = lines
        record.index = index
        record.func = func
    @staticmethod
    def from_frame(frame, context=5):
        module = frame.f_globals.get('__name__') or '?'
        filename, lineno, func, lines, index = inspect.getframeinfo(frame, context)
        if lines is None: lines = []  # if lines is None then index also is None
        source_encoding = detect_source_encoding(filename)
        formatted_lines = [ format_line(frame, line.decode(source_encoding, 'replace')) for line in lines ]
        return Record(module, filename, lineno, formatted_lines, index, func)

def format_exc(info=None, context=5):
    if info: exc_type, exc_value, tb = info
    else: exc_type, exc_value, tb = sys.exc_info()
    try:
        exc_msg = restore_escapes(exc_value)
        while tb.tb_next is not None:
            module = tb.tb_frame.f_globals.get('__name__') or '?'
            if module == 'pony' or module.startswith('pony.'): tb = tb.tb_next
            else: break
        records = []
        if issubclass(exc_type, SyntaxError) and exc_value.filename and exc_value.filename != '<?>':
            lines, index = getlines2(exc_value.filename, exc_value.lineno, context=5)
            source_encoding = detect_source_encoding(exc_value.filename)
            formatted_lines = []
            for i, line in enumerate(lines):
                syntax_error_offset = None
                if i == index: syntax_error_offset = exc_value.offset - 1
                formatted_lines.append(format_line(None, line.decode(source_encoding, 'replace'), syntax_error_offset))
            record = Record(module='<?>', filename=exc_value.filename, lineno=exc_value.lineno,
                            lines=formatted_lines, index=index)
            records = [ record ]
        else:
            frames = inspect.getinnerframes(tb, context)
            prev_frame = None
            for frame, filename, lineno, func, lines, index in frames:
                if index is None: continue
                module = frame.f_globals.get('__name__') or '?'
                source_encoding = detect_source_encoding(filename)
                formatted_lines = [ format_line(frame, line.decode(source_encoding, 'replace')) for line in lines ]
                record = Record(module=module, filename=filename, lineno=lineno,
                                lines=formatted_lines, index=index, func=func)
                records.append(record)
                if module != 'pony.templating': pass
                elif func in ('_eval', '_compile'):
                    element = prev_frame.f_locals['elem']  # instance of SyntaxElement subclass
                    text, offsets, filename = (element.source + (None,))[:3]
                    lineno, offset = pos2lineno(element.start, offsets)
                    lines, index = getlines(text, offsets, lineno, context=5)
                    record = Record(module='<template>', filename=filename, lineno=lineno,
                                    lines=lines, index=index)
                    records.append(record)
                prev_frame = frame
            if issubclass(exc_type, ParseError):
                text, offsets = exc_value.source[:2]
                lines, index = getlines(text, offsets, exc_value.lineno, context=5)
                record = Record(module='<template>', filename='<?>', lineno=exc_value.lineno,
                                lines=lines, index=index)
                records.append(record)
        return html()
    finally: del tb

python_re = re.compile(r"""
        (                                        # string (group 1)
        (?:[Uu][Rr]?|[Rr][Uu]?)?                 #     string prefix
        (?:
            '''(?:[^\\]|\\.)*?(?:'''|\Z)         #     '''triple-quoted string'''
        |   \"""(?:[^\\]|\\.)*?(?:\"""|\Z)       #     \"""triple-quoted string\"""
        |   '(?:[^'\\]|\\.)*?(?:'|$)             #     'string'
        |   "(?:[^"\\]|\\.)*?(?:"|$)             #     "string"
        ))
    |   ([(,]\s*[A-Za-z_]\w*\s*=)                # named argument (group 2)
    |   ([A-Za-z_]\w*(?:\s*\.\s*[A-Za-z_]\w*)*)  # identifier chain (group 3)
    |   (\#.*$)                                  # comment (group 4)
    """, re.VERBOSE)


ident_re = re.compile(r'[A-Za-z_]\w*')
end1_re = re.compile(r"(?:[^\\]|\\.)*?'''")
end2_re = re.compile(r'(?:[^\\]|\\.)*?"""')

ident_html = Html('<span class="ident" title="%s">%s</span>')
keyword_html = Html('<strong>%s</strong>')
comment_html = Html('<span class="comment">%s</span>')
str_html = Html('<span class="string">%s</span>')
syntax_error_html = Html('<span class="syntax-error">%s</span>')

def parse_line(line):
    pos = 0
    stop = len(line)
    while pos < stop:
        match = python_re.search(line, pos)
        if match is None: break
        start, end = match.span()
        yield 'other', pos, start, line[pos:start]
        i = match.lastindex
        if i == 1: yield 'string', start, end, match.group()
        elif i == 2: yield 'other', start, end, match.group()
        elif i == 3:
            pos = start
            for x in re.split('(\W+)', match.group()):
                next = pos + len(x)
                if x in keyword.kwlist: yield 'keyword', pos, next, x
                elif is_ident(x):
                    if pos == start: yield 'identifier', pos, next, x
                    else: yield 'attribute', pos, next, x
                else: yield 'other', pos, next, x
                pos = next
        elif i == 4: yield 'comment', start, end, match.group()
        else: assert False  # pragma: no cover
        pos = end
    yield 'other', pos, stop, line[pos:]

__undefined__ = object()

def format_line(frame, line, syntax_error_offset=None):
    if frame is not None:
        f_locals = frame.f_locals
        f_globals = frame.f_globals
    else: f_locals = f_globals = {}
    result = []
    prev = __undefined__
    for kind, start, end, x in parse_line(line):
        if syntax_error_offset is not None and start <= syntax_error_offset < end:
            i = syntax_error_offset - start
            if x[i] != '\n': y = x[:i] + syntax_error_html % x[i] + x[i+1:]
            else: y = x[:i] + syntax_error_html % ' ' + x[i:]
        else: y = x
        if kind == 'string': result.append(str_html % y)
        elif kind == 'comment': result.append(comment_html % y)
        elif kind == 'other': result.append(y)
        elif kind == 'keyword': result.append(keyword_html % y); prev = __undefined__
        elif frame is None: result.append(y)
        else:
            if kind == 'identifier':
                obj = f_locals.get(x, __undefined__)
                if obj is __undefined__: obj = f_globals.get(x, __undefined__)
                if obj is __undefined__:
                    builtins = f_globals.get('__builtins__')
                    if isinstance(builtins, dict): obj = builtins.get(x, __undefined__)
                    else: obj = getattr(builtins, x, __undefined__)
            elif kind == 'attribute':
                if prev is __undefined__: obj = __undefined__
                else: obj = getattr(prev, x, __undefined__)
            else: assert False  # pragma: no cover
            if obj is __undefined__: title = 'undefined'
            else: title = repr2(obj)
            result.append(ident_html % (title, y))
            prev = obj
    return htmljoin(result)

def format_record(record):
    return html()

if pony.MODE.startswith('GAE-'):

    def debugging_decorator(func):
        return func

    def debugging_middleware(app):
        return app

else:

    import bdb

    @decorator
    def debugging_decorator(func, *args, **kwargs):
        if options.DEBUG:
            web = sys.modules.get('pony.web')
            if web is not None:
                debugger = web.local.request.environ.get('debugger')
                if debugger is not None: debugger().set_trace()
        return func(*args, **kwargs)

    class Local(threading.local):
        def __init__(local):
            local.lock = threading.Lock()
            local.lock.acquire()

    local = Local()

    debug_re = re.compile(r'(?:(?<=\?|&)|^)debug(?:=([^&]*))?&?')
    expr_re = re.compile(r'(?:(?<=\?|&)|^)expr(?:=([^&]*))?&?')

    @decorator
    def debugging_middleware(app, environ):
        if not options.DEBUG: return app(environ)
        query = environ.get('QUERY_STRING', '')
        if not debug_re.search(query): return app(environ)

        env = dict((key, value) for key, value in iteritems(environ)
                                if isinstance(key, basestring) and isinstance(value, basestring))
        env['wsgi.version'] = environ['wsgi.version']
        env['wsgi.url_scheme'] = environ['wsgi.url_scheme']
        env['wsgi.multithread'] = environ['wsgi.multithread']
        env['wsgi.multiprocess'] = environ['wsgi.multiprocess']
        env['wsgi.run_once'] = environ['wsgi.run_once']
        content_length = int(environ.get('CONTENT_LENGTH', '0'))
        input_data = environ['wsgi.input'].read(content_length)
        env['wsgi.input'] = cStringIO.StringIO(input_data)
        env['wsgi.errors'] = cStringIO.StringIO()
        file_wrapper = environ.get('wsgi.file_wrapper')
        if file_wrapper is not None: env['wsgi.file_wrapper'] = file_wrapper

        url = httputils.reconstruct_url(environ)

        command = debug_re.search(url).group(1)
        url = debug_re.sub('', url)
        if url.endswith('&'): url = url[:-1]

        expr_match = expr_re.search(url)
        if expr_match is not None:
            expr = unquote_plus(expr_match.group(1))
            url = expr_re.sub('', url)
            if url.endswith('&'): url = url[:-1]
        else: expr = None

        result_holder = []
        queue.put((local.lock, app, env, result_holder, url, command, expr))
        local.lock.acquire()
        return result_holder[0]

    from Queue import Queue
    queue = Queue()

    last = None

    class DebugThread(threading.Thread):
        def __init__(thread):
            threading.Thread.__init__(thread, name="DebugThread")
            thread.setDaemon(True)
        def run(thread):
            global last
            last = queue.get()
            while last is not None:
                lock, app, environ, result_holder, url, command, expr = last
                debugger = Debugger(url)
                environ['debugger'] = weakref.ref(debugger)
                result = debugger.runcall(app, environ)
                if result is not None:
                    status, headers, content = result
                    lock, app, environ, result_holder, url, command, expr = last
                    headers.append(('X-Debug', 'Result'))
                    result_holder.append((status, headers, content))
                    lock.release()
                    last = queue.get()

    class Debugger(bdb.Bdb):
        def __init__(debugger, url):
            debugger.url = url
            bdb.Bdb.__init__(debugger)
            debugger.__state = 0
            debugger.__top_user_frame = None
        def process_queue(debugger, frame):
            from pony.webutils import button
            result = None
            if debugger.__state == 0:
                debugger.__state = 1
                debugger.set_continue()
                return
            global last
            if last is None: debugger.set_quit(); return
            lock, app, environ, result_holder, url, command, expr = last
            if url != debugger.url: debugger.set_quit(); return
            if debugger.__state == 1:
                module = frame.f_globals.get('__name__') or '?'
                if module == 'pony' or module.startswith('pony.'): debugger.set_step(); return
                debugger.__top_user_frame = frame
                debugger.__state = 2
            headers = [('Content-Type', 'text/html; charset=UTF-8'), ('X-Debug', 'Step')]
            if not url.endswith('?'): url += '&'
            record = Record.from_frame(frame, context=9)
            if record.index is None: debugger.set_step(); return
            result_holder.append(('200 OK', headers, html().encode('utf8')))
            lock.release()
            while True:
                last = queue.get()
                lock, app, environ, result_holder, url, command, expr = last
                environ['debugger'] = weakref.ref(debugger)
                if command == 'step': debugger.set_step()
                elif command == 'next': debugger.set_next(frame)
                elif command == 'return': debugger.set_return(frame)
                elif command == 'cont': debugger.set_continue()
                elif expr:
                    try:
                        result = repr1(eval(expr, frame.f_globals, frame.f_locals))
                    except: result = traceback.format_exc()
                    result_holder.append(('200 OK', headers, html().encode('utf8')))
                    lock.release()
                    continue
                break
        def user_line(debugger, frame):
           debugger.process_queue(frame)
        def user_return(debugger, frame, value):
            if frame is debugger.__top_user_frame:
                debugger.__top_user_frame = None
                debugger.set_continue()
        # def user_exception(debugger, frame, exception):
        #   name = frame.f_code.co_name or "<unknown>"
        #   debugger.process_queue('exception in %s %s' % (name, exception), frame)

    @pony.on_shutdown
    def do_shutdown():
        queue.put(None)
        debug_thread.join()

    debug_thread = DebugThread()
    debug_thread.start()
