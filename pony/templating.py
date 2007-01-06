import sys, threading, inspect, re

try: real_stdout
except: real_stdout = sys.stdout

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

class Local(threading.local):
    def __init__(self):
        self.writers = []

local = Local()

class ThreadedStdout(object):
    @staticmethod
    def write(s):
        try: f = local.writers[-1]
        except IndexError: f = real_stdout.write
        f(s)

threaded_stdout = ThreadedStdout()

def grab_stdout(f):
    def new_function(*args, **keyargs):
        data = []
        local.writers.append(data.append)
        sys.stdout = threaded_stdout
        try: result = f(*args, **keyargs)
        finally: assert local.writers.pop() == data.append
        if result is not None: return (result,)
        return data
    new_function.__name__ = f.__name__
    new_function.__doc__ = f.__doc__
    return new_function

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

################################################################################

def err(text, end, length=30):
    start = end - length
    if start > 0: return text[start:end]
    return text[:end]

class ParseError(Exception):
    def __init__(self, message, text, pos):
        Exception.__init__(self, '%s: %s' % (message, err(text, pos)))
        self.message = message
        self.text = text
        self.pos = pos

def strjoin(list):
    result = []
    temp = []
    for x in list:
        if isinstance(x, basestring): temp.append(x)
        elif temp:
            result.append(''.join(temp).replace('\\\\', '\\'))
            temp = []
            result.append(x)
        else: result.append(x)
    if temp: result.append(''.join(temp))
    return result

main_re = re.compile(r"""

        ([{])                            # open brace (group 1)
    |   ([}])                            # close brace (group 2)
    |   &(?:
            (&)                          # double ampersand (group 3)
        |   (                            # comments (group 4)
                //.*?(?:\n|\Z)           #     &// comment
            |   /\*.*?(?:\*/|\Z)         #     &/* comment */
            )
        |   ( [A-Za-z_]\w*\s*            # statement multi-part name (group 5)
              (?:\.\s*[A-Za-z_]\w*\s*)*
            )?
            [({]                         # start of statement content
        )

    """, re.VERBOSE)

def parse_markup(text, pos=0, nested=False):
    result = []
    brace_counter = 0
    while True:
        match = main_re.search(text, pos)
        if not match:
            if nested or brace_counter:
                raise ParseError('Unexpected end of text', text, len(text))
            result.append(text[pos:])
            end = len(text)
            return strjoin(result), end
        start, end = match.span()
        i = match.lastindex
        if start != pos: result.append(text[pos:start])
        if i == 1:  # {
            brace_counter += 1
            result.append('{')
        elif i == 2:  # }
            if not brace_counter:
                if nested: return strjoin(result), end
                raise ParseError("Unexpected symbol '}'", text, end)
            brace_counter -= 1
            result.append('}')
        elif i == 3: # &&
            result.append('&')
        elif i == 4: # &/* comment */ or &// comment
            pass
        else: # &command(
            assert i in (5, None)
            command, end = parse_command(text, end-1, match.group(5))
            result.append(command)
        pos = end

command_re = re.compile(r"""

    \s*                         # optional whitespace
    (?:                         
        (;)                     # end of command (group 1)
    |   ([{])                   # start of markup block (group 2)
    |   (                       # keyword argument (group 3)
            &
            ([A-Za-z_]\w*)      # keyword name (group 4)
            \s*=\s*[{]
        )
    )?

""", re.VERBOSE)
    
def parse_command(text, pos, name):
    exprlist = None
    if text[pos] == '(':
        exprlist, pos = parse_exprlist(text, pos+1)
    markup_args, markup_keyargs = [], []
    while True:
        match = command_re.match(text, pos)
        assert match
        end = match.end()
        i = match.lastindex
        if i is None:
            return (name, exprlist, markup_args, markup_keyargs), pos
        elif i == 1:
            return (name, exprlist, markup_args, markup_keyargs), end
        elif i == 2:
            arg, end = parse_markup(text, end, True)
            markup_args.append(arg)
        elif i == 3:
            keyname = match.group(4)
            assert keyname is not None
            keyarg, end = parse_markup(text, end, True)
            markup_keyargs.append((keyname, keyarg))
        else: assert False
        pos = end
   
exprlist_re = re.compile(r"""

        ([(])                     # open parenthesis (group 1)
    |   ([)])                     # close parenthesis (group 2)
    |   (                         # comments (group 3):
            \#.*?(?:\n|\Z)        # - Python-style comment inside expressions
        |   &//.*?(?:\n|\Z)       # - &// comment
        |   &/\*.*?(?:\*/|\Z)     # - &/* comment */
        )
    |   '(?:[^'\\]|\\.)*?'        # 'string'
    |   "(?:[^"\\]|\\.)*?"        # "string"
    |   '''(?:[^\\]|\\.)*?'''     # '''triple-quoted string'''
    |   \"""(?:[^\\]|\\.)*?\"""   # \"""triple-quoted string\"""

    """, re.VERBOSE)

def parse_exprlist(text, pos):
    result = []
    counter = 1
    while True:
        match = exprlist_re.search(text, pos)
        if not match:
            raise ParseError('Unexpected end of text', text, len(text))
        start, end = match.span()
        i = match.lastindex
        if i != 3: result.append(text[pos:end])
        if i == 1: counter += 1  # (
        elif i == 2:             # )
            counter -= 1
            if counter == 0: return ''.join(result)[:-1], end
        else: assert i == 3 or i is None
        pos = end
    