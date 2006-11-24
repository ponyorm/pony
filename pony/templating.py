import sys, threading, inspect

class Html(unicode):
    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, unicode.__repr__(self))
    def __add__(self, x):  return Html(unicode.__add__(self, quote(x)))
    def __radd__(self, x): return Html(unicode.__add__(quote(x), self))
    def __mul__(self, x):  return Html(unicode.__mul__(self, x))
    def __rmul__(self, x): return Html(unicode.__mul__(self, x))
    def __mod__(self, x):
        if isinstance(x, tuple):
            x = tuple(_wrap(item) for item in x)
        else: x = _wrap(x)
        return Html(unicode.__mod__(self, x))
    def __rmod__(self, x):
        return quote(x) % self
    def join(self, items):
        return Html(unicode.join(self,(unicode(quote(item)) for item in items)))

join = Html('').join

def quote(x):
    if isinstance(x, (int, long, float, Html)): return x
    if not isinstance(x, basestring): x = unicode(x)
    return Html(x.replace('&', '&amp;').replace('<', '&lt;')
                  .replace('>', '&gt;').replace('"', '&quot;'))

def _wrap(x):
    if isinstance(x, basestring):
        if isinstance(x, Html): result = UnicodeWrapper(x)
        else:
            quoted = (x.replace('&', '&amp;').replace('<', '&lt;')
                       .replace('>', '&gt;').replace('"', '&quot;'))
            if isinstance(x, str): result = StrWrapper(quoted)
            else: result = UnicodeWrapper(quoted)
        result.original_value = x
        return result
    if isinstance(x, (int, long, float, Html)): return x
    return Wrapper(x)

class Wrapper(object):
    __slots__ = [ 'value' ]
    def __init__(self, value):
        self.value = value
    def __unicode__(self):
        if isinstance(self.value, Html): return self.value
        return quote(self.value)
    __str__ = __unicode__
    def __repr__(self):
        return quote(`self.value`)
    def __getitem__(self, key):
        return _wrap(self.value[key])

class StrWrapper(str):
    def __repr__(self):
        return quote(`self.original_value`)

class UnicodeWrapper(unicode):
    __slots__ = [ 'original_value' ]
    def __repr__(self):
        return quote(`self.original_value`)

################################################################################

def push_writer(writer):
    local.writers.append(writer)

def pop_writer():
    return local.writers.pop()

def grab_stdout(f):
    def new_function(*args, **keyargs):
        data = []
        push_writer(data.append)
        try: result = f(*args, **keyargs)
        finally: assert pop_writer() == data.append
        if result is not None: return (result,)
        return data
    new_function.__name__ = f.__name__
    new_function.__doc__ = f.__doc__
    return new_function

class Local(threading.local):
    def __init__(self):
        self.writers = []

local = Local()

class ThreadedStdout(object):
    @staticmethod
    def write(s):
        try: w = local.writers[-1]
        except IndexError: w = sys.__stdout__.write
        w(s)

if not isinstance(sys.stdout, ThreadedStdout):
    sys.stdout = ThreadedStdout()

def string2html(f, source_encoding):
    co = f.func_code
    consts = list(co.co_consts)
    for i, c in enumerate(consts):
        if not isinstance(c, basestring): continue
        if isinstance(c, str): c = c.decode(source_encoding)
        consts[i] = Html(c)
    new_code = type(co)(co.co_argcount, co.co_nlocals, co.co_stacksize,
                        co.co_flags, co.co_code, tuple(consts), co.co_names,
                        co.co_varnames, co.co_filename, co.co_name,
                        co.co_firstlineno, co.co_lnotab, co.co_freevars,
                        co.co_cellvars)
    new_function = type(f)(new_code, f.func_globals, f.func_name,
                                     f.func_defaults, f.func_closure)
    return new_function

def create_html_decorator(source_encoding='ascii'):
    def html_decorator(f):
        inner = string2html(f, source_encoding)
        f2 = grab_stdout(inner)
        def new_function(*args, **keyargs):
            return join(f2(*args, **keyargs))
        new_function.inner = inner
        new_function.__name__ = f.__name__
        new_function.__doc__ = f.__doc__
        return new_function
    return html_decorator

def html(*args, **keyargs):
    if len(args) == 1 and inspect.isfunction(args[0]):
        return create_html_decorator()(args[0])
    elif not args:
        source_encoding = keyargs.pop('source_encoding', 'ascii')
        assert not keyargs
        return create_html_decorator(source_encoding)
    else: assert False

if __name__ == '__main__':
    @html
    def header(name):
        print '<html>'
        print '<head><title>Greeting to %s</title></head>' % name
        print '<body>'
    @html
    def footer(): return '</body></html>'
    @html
    def f1(name, i):
        print header(name)
        print '<h1>Hello, %s!</h1>' % name
        print '<ul>'
        for i in range(1, i+1):
            print '<li> Item %d' % i
        print '</ul>'
        print footer()
    x = f1('<John>', 5)
    print '-' * 40
    print x
    raw_input()