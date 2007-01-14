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
            return strjoin(result), end
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
                    return strjoin(result), end
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
        self.start, self.end = tree[:2]
        self.content = []
        for item in tree[2:]:
            if isinstance(item, basestring): self.content.append(item)
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
    def eval(self, globals, locals):
        result = []
        for element in self.content:
            if isinstance(element, basestring): result.append(element)
            else: result.append(element.eval(globals, locals))
        return ''.join(result)

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
    def eval(self, globals, locals):
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
    def eval(self, globals, locals):
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
        return ''.join(result)

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
    def eval(self, globals, locals):
        return str(eval(self.expr_code, globals, locals))


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
    def eval(self, globals, locals):
        return ''

def collector(*args, **keyargs):
    return list(args), keyargs

class FunctionElement(SyntaxElement):
    def __init__(self, text, item):
        self.text = text
        (self.start, self.end, self.expr, self.params,
                            markup_args, markup_keyargs) = item
        self.markup_args = [ Markup(text, item) for item in markup_args ]
        self.markup_keyargs = [ (key, Markup(text, item))
                                for (key, item) in markup_keyargs ]
        self.func_code = compile(self.expr, '<?>', 'eval')
        self.params_code = compile('__pony_internal_func__(%s)' %self.params,
                                   '<?>', 'eval')
    def eval(self, globals, locals):
        func = eval(self.func_code, globals, locals)
        globals['__pony_internal_func__'] = collector
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
        return str(func(*args, **keyargs))
        
class BoundMarkup(object):
    def __init__(self, markup, globals, locals):
        self.markup = markup
        self.globals = globals
        self.locals = locals
    def __call__(self):
        return self.markup.eval(globals, locals)