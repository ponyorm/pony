import sys, os.path, threading, inspect, re, weakref

from utils import is_ident, decorator

try: real_stdout
except: real_stdout = sys.stdout

class Html(unicode):
    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, unicode.__repr__(self))
##  # Part of correct markup may not be correct markup itself
##  # Also commented because of possible performance issues
##  def __getitem__(self, key):
##      return self.__class__(unicode.__getitem__(self, key))
##  def __getslice__(self, i, j):
##      return self.__class__(unicode.__getslice__(self, i, j))
    def __add__(self, x):
        return self.__class__(unicode.__add__(self, quote(x)))
    def __radd__(self, x):
        return self.__class__(unicode.__add__(quote(x), self))
    def __mul__(self, x):
        return self.__class__(unicode.__mul__(self, x))
    def __rmul__(self, x):
        return self.__class__(unicode.__mul__(self, x))
    def __mod__(self, x):
        if isinstance(x, tuple):
            x = tuple(_wrap(item) for item in x)
        else: x = _wrap(x)
        return self.__class__(unicode.__mod__(self, x))
    def __rmod__(self, x):
        return quote(x) % self
    def join(self, items):
        return self.__class__(unicode.join(self, (unicode(quote(item))
                                                  for item in items)))

htmljoin = Html('').join

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
        # The next line required for PythonWin interactive window
        # (PythonWin resets stdout all the time)
        sys.stdout = threaded_stdout 
        try: result = f(*args, **keyargs)
        finally: assert local.writers.pop() == data.append
        if result is not None: return (result,)
        return data
    new_function.__name__ = f.__name__
    new_function.__doc__ = f.__doc__
    return new_function

def string_consts_to_html(f, source_encoding='ascii'):
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

@decorator
def printtext(old_func):
    func = grab_stdout(old_func)
    def new_func(*args, **keyargs):
        return u''.join(func(*args, **keyargs))
    return new_func

@decorator
def printhtml(old_func):
    func = grab_stdout(string_consts_to_html(old_func))
    def new_func(*args, **keyargs):
        return htmljoin(func(*args, **keyargs))
    return new_func

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

def joinstrings(list):
    result = []
    temp = []
    for x in list:
        if isinstance(x, basestring): temp.append(x)
        elif temp:
            s = ''.join(temp)
            if s: result.append(s)
            temp = []
            result.append(x)
        else: result.append(x)
    if temp:
        s = ''.join(temp)
        if s: result.append(s)
    return result

main_re = re.compile(r"""

        ([{])                            # open brace (group 1)
    |   ([}])                            # close brace (group 2)
    |   &(?:
            (&)                          # double & (group 3)
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
    result = [pos, None]
    brace_counter = 0
    while True:
        match = main_re.search(text, pos)
        if not match:
            if nested or brace_counter:
                raise ParseError('Unexpected end of text', text, len(text))
            result.append(text[pos:])
            end = len(text)
            result[1] = end-1
            return joinstrings(result), end
        start, end = match.span()
        i = match.lastindex
        if start != pos: result.append(text[pos:start])
        if i == 1:  # {
            brace_counter += 1
            result.append('{')
        elif i == 2:  # }
            if not brace_counter:
                if nested:
                    result[1] = end-1
                    return joinstrings(result), end
                raise ParseError("Unexpected symbol '}'", text, end)
            brace_counter -= 1
            result.append('}')
        elif i == 3: # &&
            result.append('&')
        elif i == 4: # &/* comment */ or &// comment
            pass
        else: # &command(
            assert i in (5, None)
            cmd_name = match.group(5)
            command, end = parse_command(text,match.start(),end-1,cmd_name)
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
    
def parse_command(text, start, pos, name):
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
            return (start,pos,name,exprlist,markup_args,markup_keyargs), pos
        elif i == 1:
            return (start,end,name,exprlist,markup_args,markup_keyargs), end
        elif i == 2:
            if markup_keyargs: raise ParseError(
                'Positional arguments cannot go after keyword arguments',
                text, end)
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

class SyntaxElement(object):
    def _raise_unexpected_statement(self, item):
        pos = item[0]
        name = item[2]
        errpos = self.text.index(name, pos) + len(name)
        raise ParseError("Unexpected '%s' statement" % name, self.text, errpos)
    def _check_statement(self, item):
        cmd_name, expr, markup_args, markup_keyargs = item[2:]
        end_of_expr = (markup_args and markup_args[0][0]
                       or markup_keyargs and markup_keyargs[0][0]
                       or self.end)
        if cmd_name in ('if', 'elif', 'for'):
            if not expr: raise ParseError(
                "'%s' statement must contain expression" % cmd_name,
                self.text, end_of_expr)
        elif cmd_name in ('else', 'separator'):
            if expr is not None: raise ParseError(
                "'%s' statement must not contains expression" % cmd_name,
                self.text, end_of_expr)
        if markup_keyargs: raise ParseError(
                "'%s' statement must not have keyword arguments" % cmd_name,
                self.text, markup_keyargs[0][1][0])
        if not markup_args: raise ParseError(
                "'%s' statement must contain markup block" % cmd_name,
                self.text, self.end)
        if len(markup_args) > 1: raise ParseError(
            "'%s' statement must contain exactly one markup block" % cmd_name,
            self.text, markup_args[1][0])

class Markup(SyntaxElement):
    def __init__(self, text, tree):
        assert isinstance(tree, list)
        self.text = text
        self.empty = text.__class__()
        self.start, self.end = tree[:2]
        self.content = []
        for item in tree[2:]:
            if isinstance(item, basestring):
                self.content.append(text.__class__(item))
            elif isinstance(item, tuple):
                prev = self.content and self.content[-1] or None
                if self.content: prev = self.content[-1]
                cmd_name = item[2]
                if cmd_name in ('elif', 'else', 'separator'):
                    if isinstance(prev, basestring) and prev.isspace():
                        self.content.pop()
                        prev = self.content and self.content[-1] or None
                if cmd_name is None:
                    if item[3]: self.content.append(ExprElement(text, item))
                    else: self.content.append(I18nElement(text, item))
                elif cmd_name == 'if':
                    self.content.append(IfElement(text, item))
                elif cmd_name == 'elif':
                    if not isinstance(prev, IfElement):
                        self._raise_unexpected_statement(item)
                    prev.append_elif(item)
                elif cmd_name == 'else':
                    if not isinstance(prev, (IfElement, ForElement)):
                        self._raise_unexpected_statement(item)
                    prev.append_else(item)
                elif cmd_name == 'for':
                    self.content.append(ForElement(text, item))
                elif cmd_name == 'separator':
                    if not isinstance(prev, ForElement):
                        self._raise_unexpected_statement(item)
                    prev.append_separator(item)
                else: self.content.append(FunctionElement(text, item))
            else: assert False
    def eval(self, globals, locals=None):
        result = []
        for element in self.content:
            if isinstance(element, basestring): result.append(element)
            else: result.append(element.eval(globals, locals))
        return self.empty.join(result)

class IfElement(SyntaxElement):
    def __init__(self, text, item):
        self.text = text
        self.start, self.end = item[:2]
        self.chain = []
        self._append(item)
    def append_elif(self, item):
        if self.chain[-1][0] is None: self._raise_unexpected_statement(item)
        self._append(item)
    def append_else(self, item):
        if self.chain[-1][0] is None: self._raise_unexpected_statement(item)
        self._append(item)
    def _append(self, item):
        self._check_statement(item)
        end, expr, markup_args = item[1], item[3], item[4]
        self.end = end
        expr_code = expr is not None and compile(expr, '<?>', 'eval') or None
        self.chain.append((expr, expr_code, Markup(self.text, markup_args[0])))
    def eval(self, globals, locals=None):
        for expr, expr_code, markup in self.chain:
            if expr is None or eval(expr_code, globals, locals):
                return markup.eval(globals, locals)

var_list_re_1 = re.compile(r"""

    \s*
    (?:
        ([A-Za-z_]\w*)    # group 1
    |   ([(])             # group 2
    )

    """, re.VERBOSE)

var_list_re_2 = re.compile(r"""

    \s*
    (?:
        (?: ,\s* )?     # trailing comma
        (?:
            ([)])       # group 1
        |   \b(in)\b    # group 2
        )
    |   (,)             # group 3
    )

    """, re.VERBOSE)

def parse_var_list(text, pos, end, nested=False):
    result = []
    errmsg = "Incorrect Python expression inside 'for' statement"
    while True:
        match = var_list_re_1.match(text, pos, end)
        if not match:
            if result and not nested and text[pos:end].isspace():
                return result
            raise ParseError(errmsg, text, end)
        pos = match.end()
        i = match.lastindex
        if i == 1:
            ident = match.group(1)
            result.append(match.group(1))
        elif i == 2:
            vars, pos = parse_var_list(text, pos, end, True)
            result.extend(vars)
        else: assert False
        match = var_list_re_2.match(text, pos, end)
        if not match: raise ParseError(errmsg, text, end)
        pos = match.end()
        i = match.lastindex
        if i == 1:
            if not nested: raise ParseError("Unexpected ')'", text, pos)
            return result, pos
        elif i == 2:
            if nested: raise ParseError("')' expected", text, pos)
            return result
        elif i == 3: pass
        else: assert False

class ForElement(SyntaxElement):
    def __init__(self, text, item):
        self.text = text
        self.empty = text.__class__()
        self.start, self.end = item[:2]
        self._check_statement(item)
        self.expr = item[3]
        self.markup = Markup(self.text, item[4][0])
        self.var_names = parse_var_list(self.expr, 0, len(self.expr))
        self.separator = self.else_ = None
        var_list = ', '.join(self.var_names)
        list_expr = '[ (%s,) for %s ]' % (var_list, self.expr)
        self.code = compile(list_expr, '<?>', 'eval')
    def append_separator(self, item):
        if self.separator or self.else_: self._raise_unexpected_statement(item)
        self._check_statement(item)
        self.end = item[1]
        self.separator = Markup(self.text, item[4][0])
    def append_else(self, item):
        if self.else_: self._raise_unexpected_statement(item)
        self._check_statement(item)
        self.end = item[1]
        self.else_ = Markup(self.text, item[4][0])
    def eval(self, globals, locals=None):
        NOT_EXISTS = object()
        old_values = []
        for name in self.var_names:
            old_values.append(locals.get(name, NOT_EXISTS))
        result = []
        list = eval(self.code, globals, locals)
        if not list:
            if self.else_: return self.else_.eval(globals, locals)
            return ''
        for i, item in enumerate(list):
            for name, value in zip(self.var_names, item):
                if i and self.separator:
                    result.append(self.separator.eval(globals, locals))
                locals[name] = value
                result.append(self.markup.eval(globals, locals))
        for name, old_value in zip(self.var_names, old_values):
            if old_value is NOT_EXISTS: del locals[name]
            else: locals[name] = old_value
        return self.empty.join(result)

class ExprElement(SyntaxElement):
    def __init__(self, text, item):
        self.text = text
        self.start,self.end,cmd_name,self.expr,markup_args,markup_keyargs = item
        assert cmd_name is None
        if markup_args:
            raise ParseError('Unexpected markup block', text, markup_args[0][0])
        if markup_keyargs:
            raise ParseError('Unexpected keyword argument',
                             text, markup_keyrgs[0][1][0])
        self.expr_code = compile(self.expr, '<?>', 'eval')
    def eval(self, globals, locals=None):
        result = eval(self.expr_code, globals, locals)
        if isinstance(result, basestring): return result
        return unicode(result)

space_re = re.compile(r'\s+')

class I18nElement(SyntaxElement):
    def __init__(self, text, item):
        self.text = text
        self.start, self.end, cmd_name, expr, markup_args, markup_keyargs = item
        assert cmd_name is None and expr is None
        self.markup = Markup(text, markup_args[0])
        if len(markup_args) > 1:
            raise ParseError('Unexpected markup block', text, markup_args[1][0])
        if markup_keyargs:
            raise ParseError('Unexpected keyword argument',
                             text, markup_keyrgs[0][1][0])
        list = []
        i = 0
        for item in self.markup.content:
            if isinstance(item, basestring):
                list.append(item.replace('$', '$$'))
            else:
                i += 1
                list.append('$%d' % i)
        self.base_string = space_re.sub(' ', ''.join(list).strip())
    def eval(self, globals, locals=None):
        return ''

class FunctionElement(SyntaxElement):
    def __init__(self, text, item):
        self.text = text
        (self.start, self.end, self.expr, self.params,
                            markup_args, markup_keyargs) = item
        self.markup_args = [ Markup(text, item) for item in markup_args ]
        self.markup_keyargs = [ (key, Markup(text, item))
                                for (key, item) in markup_keyargs ]
        self.func_code = compile(self.expr, '<?>', 'eval')
        s = '(lambda *args, **keyargs: (list(args), keyargs))(%s)' % self.params
        self.params_code = compile(s, '<?>', 'eval')
    def eval(self, globals, locals=None):
        func = eval(self.func_code, globals, locals)
        args, keyargs = eval(self.params_code, globals, locals)
        if getattr(func, 'lazy', False):
            args.extend([BoundMarkup(m, globals, locals)
                         for m in self.markup_args])
            for key, markup in self.markup_keyargs:
                keyargs[key] = BoundMarkup(markup, globals, locals)
        else:
            for arg in self.markup_args:
                args.append(arg.eval(globals, locals))
            for key, arg in self.markup_keyargs:
                keyargs[key] = arg.eval(globals, locals)
        result = func(*args, **keyargs)
        if isinstance(result, basestring): return result
        return unicode(result)
        
class BoundMarkup(object):
    def __init__(self, markup, globals, locals=None):
        self.markup = markup
        self.globals = globals
        self.locals = locals
    def __call__(self):
        return self.markup.eval(globals, locals)

def compile_text_template(source):
    tree = parse_markup(source)[0]
    return Markup(source, tree)

def compile_html_template(source):
    tree = parse_markup(source)[0]
    return Markup(Html(source), tree)

codename_cache = {}

def get_class_name(frame):
    try:
        code = frame.f_code
        if code.co_argcount:
            first_name = code.co_varnames[0]
            first_argument = frame.f_locals[first_name]
            if isinstance(first_argument, type): # probably classmethod
                cls = first_argument
            else:
                cls = first_argument.__class__
            classes = inspect.getmro(cls)
            for cls in classes:
                for member in cls.__dict__.values():
                    if inspect.isfunction(member):
                        if member.func_code is code: return cls.__name__
                    elif member.__class__ is classmethod:
                        if member.__get__(1, int).im_func.func_code is code:
                            return cls.__name__
    finally:
        frame = None

template_name_cache = {} # weakref.WeakKeyDictionary()

def get_template_name(frame):
    try:
        code = frame.f_code
        result = template_name_cache.get(code)
        if result: return result
        file_name = code.co_filename
        if file_name is None: return None

        code_name = code.co_name
        if not code_name or not is_ident(code_name): return None

        cls_name = get_class_name(frame)
        if cls_name is None:
            root, ext = os.path.splitext(file_name)
            result = '%s.%s' % (root, code_name)
        else:            
            head, tail = os.path.split(file_name)
            tail = '%s.%s' % (cls_name, code_name)
            result = os.path.join(head, tail)
        template_name_cache[code] = result
        return result
    finally:
        frame = None

template_cache = {}

def _template(str_cls,
              text=None, filename=None, globals=None, locals=None,
              source_encoding='ascii'):
    if text and filename:
        raise TypeError("template() function cannot accept both "
                        "'text' and 'filename' parameters at the same time")
    if not text:
        if not filename:
            filename = get_template_name(sys._getframe(2)) + '.template'
        f = file(filename)
        text = f.read()
        f.close()
    markup = template_cache.get(text)
    if not markup:
        tree = parse_markup(text)[0]
        markup = Markup(str_cls(text, source_encoding), tree)
        template_cache[text] = markup
    if globals is None and locals is None:
        globals = sys._getframe(2).f_globals
        locals = sys._getframe(2).f_locals
    return markup.eval(globals, locals)

def template(text=None, filename=None, globals=None, locals=None,
              source_encoding='ascii'):
    return _template(unicode, text, filename, globals, locals, source_encoding)

def html(text=None, filename=None, globals=None, locals=None,
             source_encoding='ascii'):
    return _template(Html, text, filename, globals, locals, source_encoding)