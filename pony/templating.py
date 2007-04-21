import sys, os.path, threading, inspect, re, weakref, textwrap

from utils import (read_text_file, is_ident,
                   decorator, decorator_with_params)

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
                  .replace('>', '&gt;').replace('"', '&quot;')
                  .replace("'", '&#39;'))

def _wrap(x):
    if isinstance(x, basestring):
        if isinstance(x, Html): result = UnicodeWrapper(x)
        else:
            quoted = (x.replace('&', '&amp;').replace('<', '&lt;')
                       .replace('>', '&gt;').replace('"', '&quot;')
                       .replace("'", '&#39;'))
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

def htmltag(_name_, _attrs_=None, **_attrs2_):
    attrs = {}
    for d in _attrs_, _attrs2_:
        if not d: continue
        for name, value in d.items():
            name = name.lower().strip('_').replace('_', '-')
            attrs[name] = value
    cls2 = attrs.pop('additional-class', None)
    if cls2 is not None:
        cls = attrs.get('class')
        if cls: attrs['class'] = cls + ' ' + cls2
        else: attrs['class'] = cls2
    attrlist = []
    make_attr = Html('%s="%s"').__mod__
    for name, value in attrs.items():
        if value is True: attrlist.append(name)
        elif value is not False and value is not None:
            attrlist.append(make_attr((name, value)))
    return Html("<%s %s>") % (_name_, Html(' ').join(attrlist))

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

@decorator
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

@decorator_with_params
def printhtml(source_encoding='ascii'):
    def new_decorator(old_func):
        func = grab_stdout(string_consts_to_html(old_func, source_encoding))
        def new_func(*args, **keyargs):
            return htmljoin(func(*args, **keyargs))
        return new_func
    return new_decorator

################################################################################

@decorator
def lazy(old_func):
    old_func.lazy = True
    return old_func

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
    |   [$](?:
            ([$])                        # double $ (group 3)
        |   (                            # comments (group 4)
                //.*?(?:\n|\Z)           #     $// comment
            |   /\*.*?(?:\*/|\Z)         #     $/* comment */
            )
        |   ([(])                        # start of $(expression) (group 5)
        |   ([{])                        # start of ${markup} (group 6)
        |   ( [A-Za-z_]\w*               # multi-part name (group 7)
              (?:\s*\.\s*[A-Za-z_]\w*)*
            )
            (\s*[({])?                   # start of statement content (group 8)
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
        elif i == 3: # $$
            result.append('$')
        elif i == 4: # $/* comment */ or $// comment
            pass
        elif i in (5, 6): # $(expression) or ${i18n markup}
            command, end = parse_command(text, start, end-1, None)
            result.append(command)
        elif i == 7: # $expression.path
            result.append((start, end, None, match.group(7), None, None))
        elif i == 8: # $function.call(...)
            cmd_name = match.group(7)
            command, end = parse_command(text, match.start(), end-1, cmd_name)
            result.append(command)
        pos = end

command_re = re.compile(r"""

    \s*                         # optional whitespace
    (?:                         
        (;)                     # end of command (group 1)
    |   ([{])                   # start of markup block (group 2)
    |   (                       # keyword argument (group 3)
            [$]
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
        |   [$]//.*?(?:\n|\Z)     # - $// comment
        |   [$]/\*.*?(?:\*/|\Z)   # - $/* comment */
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
        elif cmd_name in ('else', 'sep', 'separator'):
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
                if cmd_name in ('elif', 'else', 'sep', 'separator'):
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
                elif cmd_name in ('sep', 'separator'):
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
        if expr is None: expr_code = None
        else: expr_code = compile(expr.lstrip(), '<?>', 'eval')
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
        if locals is None: locals = {}
        not_found = object()
        old_values = []
        var_names = self.var_names + [ 'for' ]
        for name in var_names:
            old_values.append(locals.get(name, not_found))
        result = []
        list = eval(self.code, globals, locals)
        if not list:
            if self.else_: return self.else_.eval(globals, locals)
            return ''
        for i, item in enumerate(list):
            if i and self.separator:
                result.append(self.separator.eval(globals, locals))
            for name, value in zip(self.var_names, item): locals[name] = value
            locals['for'] = i, len(list)
            result.append(self.markup.eval(globals, locals))
        for name, old_value in zip(self.var_names, old_values):
            if old_value is not_found: del locals[name]
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
        if inspect.isroutine(result): result = result()
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
        self.params = self.params or ''
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
        if inspect.isroutine(result): result = result()
        if isinstance(result, basestring): return result
        return unicode(result)
        
class BoundMarkup(object):
    def __init__(self, markup, globals, locals=None):
        self.markup = markup
        self.globals = globals
        self.locals = locals
    def __call__(self):
        return self.markup.eval(self.globals, self.locals)

def _cycle_check(args):
    for arg in args:
        if not isinstance(arg, BoundMarkup):
            raise TypeError('Incorrect type of argument: %s' % str(type(arg)))
    locals = args[-1].locals
    if locals is None or 'for' not in locals: raise TypeError(
        '$cycle() function may only be called inside a $for loop')
    return locals['for']

@lazy
def cycle(*args):
    if len(args) < 2: raise TypeError(
        '$cycle() function takes at least 2 arguments (%d given)' % len(args))
    if isinstance(args[0], BoundMarkup):
        current, total = _cycle_check(args[1:])
        return args[current % len(args)]()
    elif isinstance(args[0], basestring):
        if len(args) != 2: raise TypeError(
            'When first argument is string, $cycle() function '
            'takes exactly 2 arguments (%d given)' % len(args))
        current, total = _cycle_check(args[1:])
        s, markup = args
        if s == 'odd': flag = not (current % 2) # rows treated as 1-based!
        elif s == 'even': flag = (current % 2) # rows treated as 1-based!
        elif s == 'first': flag = not current
        elif s == 'not first': flag = current
        elif s == 'last': flag = (current == total - 1)
        elif s == 'not last': flag = (current != total - 1)
        return flag and markup() or ''
    elif not isinstance(args[0], (int, long)):
        raise TypeError('First argument of $cycle() function must be int, '
                        'string or markup. Got: %s' % str(type(args[0])))
    elif isinstance(args[1], (int, long)):
        if len(args) == 2: raise TypeError('Markup expected')
        if len(args) > 3:
            raise TypeError('$cycle() function got too many arguments')
        current, total = _cycle_check(args[2:])
        i, j, markup = args
        return (current % j == i - 1) and markup() or ''
    else:
        if len(args) > 2:
            raise TypeError('$cycle() function got too many arguments')
        current, total = _cycle_check(args[1:])
        i, markup = args
        if i > 0: return (current == i - 1) and markup() or ''
        elif i < 0: return (i == current - total) and markup() or ''
        else: raise TypeError('$cycle first argument cannot be 0')
    
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

def compile_text_template(source):
    tree = parse_markup(source)[0]
    return Markup(source, tree)

def compile_html_template(source):
    tree = parse_markup(source)[0]
    return Markup(Html(source), tree)

template_cache = {}

def _template(str_cls, default_ext,
              text=None, filename=None,
              globals=None, locals=None,
              encoding=None, keep_indent=False):
    if text and filename:
        raise TypeError("template function cannot accept both "
                        "'text' and 'filename' parameters at the same time")
    if not text:
        if not filename:
            filename = get_template_name(sys._getframe(2)) + default_ext
        text = read_text_file(filename, encoding=encoding)
    else:
        if not isinstance(text, unicode):
            text = unicode(text, encoding or 'ascii')
        if not keep_indent: text = textwrap.dedent(text)
    markup = template_cache.get(text)
    if not markup:
        tree = parse_markup(text)[0]
        markup = Markup(str_cls(text), tree)
        template_cache[text] = markup
    if globals is None and locals is None:
        globals = sys._getframe(2).f_globals
        locals = sys._getframe(2).f_locals
    return markup.eval(globals, locals)

def template(*args, **keyargs):
    return _template(unicode, '.txt', *args, **keyargs)

def html(*args, **keyargs):
    return _template(Html, '.html', *args, **keyargs)
