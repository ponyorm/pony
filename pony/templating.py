import sys, os.path, threading, inspect, re, weakref, textwrap, copy_reg

import pony
from pony import grab_stdout
from pony import options, i18n
from pony.utils import read_text_file, is_ident, decorator, decorator_with_params, get_mtime

try: from pony import _templating
except ImportError: pass

def plainstr(x):
    if not isinstance(x, basestring):
        if hasattr(x, '__unicode__'): x = unicode(x)
        else: x = str(x)
    # if isinstance(x, Html): return unicode(x)
    # if isinstance(x, StrHtml): return str.__str__(x)
    return x[:]

class Html(unicode):
    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, unicode.__repr__(self))
    def __add__(self, x):
        return Html(unicode.__add__(self, quote(x, True)))
    def __radd__(self, x):
        y = quote(x, True)
        if y is not x: return y + self
        raise TypeError("unsupported operand type(s) for +: %r and %r"
                        % (x.__class__.__name__, self.__class__.__name__))
    def __mul__(self, x):
        return Html(unicode.__mul__(self, x))
    def __rmul__(self, x):
        return Html(unicode.__mul__(self, x))
    def __mod__(self, x):
        if not isinstance(x, tuple): x = _wrap(x, True)
        else: x = tuple(_wrap(item, True) for item in x)
        return Html(unicode.__mod__(self, x))
    def join(self, items):
        return Html(unicode.join(self, (quote(item, True) for item in items)))
    #  Part of correct markup may not be correct markup itself
    #  Also commented because of possible performance issues
    #  def __getitem__(self, key):
    #      return Html(unicode.__getitem__(self, key))
    #  def __getslice__(self, i, j):
    #      return Html(unicode.__getslice__(self, i, j))

class StrHtml(str):
    def __str__(self):        # Because of bug in Python 2.4 print statement.
        s = str.__str__(self)
        return StrHtml2(s) 
    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, str.__repr__(self))
    def __add__(self, x):
        try: result = str.__add__(self, quote(x))
        except UnicodeDecodeError:
            return Html(unicode.__add__(unicode(self, errors='replace'), quote(x, True)))
        if isinstance(result, str): return StrHtml(result)
        return Html(result)
    def __radd__(self, x):
        return quote(x) + self
    def __mul__(self, x):
        result = str.__mul__(self, x)
        if isinstance(result, str): return StrHtml(result)
        return Html(result)
    def __rmul__(self, x):
        result = str.__mul__(self, x)
        if isinstance(result, str): return StrHtml(result)
        return Html(result)
    def __mod__(self, x):
        if not isinstance(x, tuple): y = _wrap(x)
        else: y = tuple(_wrap(item) for item in x)
        try: result = str.__mod__(self, y)
        except UnicodeDecodeError:
            if not isinstance(x, tuple): y = _wrap(x, True)
            else: y = tuple(_wrap(item, True) for item in x)
            return Html(unicode.__mod__(unicode(self, errors='replace'), y))
        if isinstance(result, str): return StrHtml(result)
        return Html(result)
    def join(self, items):
        items = list(items)
        try: result = str.join(self, (quote(item) for item in items))
        except UnicodeDecodeError:
            return Html(unicode(self, errors='replace')).join(items)
        if isinstance(result, str): return StrHtml(result)
        return Html(result)
    # Part of correct markup may not be correct markup itself
    # Also commented because of possible performance issues
    # def __getitem__(self, key):
    #     return StrHtml(str.__getitem__(self, key))
    # def __getslice__(self, i, j):
    #     return StrHtml(str.__getslice__(self, i, j))

class StrHtml2(StrHtml):
    def __str__(self):
        return str.__str__(self)

def quote(x, unicode_replace=False):
    if isinstance(x, (Html, int, long, float)): return x

    if not isinstance(x, basestring):
        if hasattr(x, '__unicode__'): x = unicode(x)
        else: x = str(x)
        if isinstance(x, Html): return x
    if isinstance(x, StrHtml):
        if not unicode_replace: return x
        return Html(unicode(x, errors='replace'))

    x = (x.replace('&', '&amp;').replace('<', '&lt;')
          .replace('>', '&gt;').replace('"', '&quot;')
          .replace("'", '&#39;'))
    if isinstance(x, unicode): return Html(x)
    if not unicode_replace: return StrHtml(x)
    return Html(unicode(x, errors='replace'))

def _wrap(x, unicode_replace=False):
    if isinstance(x, (int, long, float)): return x
    if not isinstance(x, basestring): return Wrapper(x, unicode_replace)

    if not isinstance(x, (Html, StrHtml)):
        x = (x.replace('&', '&amp;').replace('<', '&lt;')
              .replace('>', '&gt;').replace('"', '&quot;')
              .replace("'", '&#39;'))
    if isinstance(x, str):
        if not unicode_replace: result = StrWrapper(x)
        else: result = UnicodeWrapper(unicode(x, errors='replace'))
    else: result = UnicodeWrapper(x)
    result.original_value = x
    return result

class Wrapper(object):
    __slots__ = [ 'value', 'unicode_replace' ]
    def __init__(self, value, unicode_replace):
        self.value = value
        self.unicode_replace = unicode_replace
    def __str__(self):
        return quote(self.value, self.unicode_replace)
    def __unicode__(self):
        return quote(self.value, self.unicode_replace)
    def __repr__(self):
        return quote(`self.value`)
    def __getitem__(self, key):
        return _wrap(self.value[key], self.unicode_replace)

class StrWrapper(str):
    def __repr__(self):
        return quote(`self.original_value`)

class UnicodeWrapper(unicode):
    __slots__ = [ 'original_value' ]
    def __repr__(self):
        return quote(`self.original_value`)

try: _templating
except NameError: pass
else:
    Html = _templating.Html
    StrHtml = _templating.StrHtml
    StrHtml2 = _templating.StrHtml2
    quote = _templating.quote
    del _wrap, Wrapper, UnicodeWrapper

start_offset = options.PICKLE_START_OFFSET
if options.PICKLE_HTML_AS_PLAIN_STR:
    copy_reg.pickle(Html, lambda x: (unicode, (unicode(x),)))
    copy_reg.pickle(StrHtml, lambda x: (str, (str.__str__(x),)))
    copy_reg.pickle(StrHtml2, lambda x: (str, (str.__str__(x),)))
    copy_reg.add_extension('__builtin__', 'unicode', start_offset)
    copy_reg.add_extension('__builtin__', 'str', start_offset+1)
else:
    copy_reg.pickle(Html, lambda x: (Html, (unicode(x),)))
    copy_reg.pickle(StrHtml, lambda x: (StrHtml, (str.__str__(x),)))
    copy_reg.pickle(StrHtml2, lambda x: (StrHtml, (str.__str__(x),)))
    copy_reg.add_extension('pony.templating', 'Html', start_offset)
    copy_reg.add_extension('pony.templating', 'StrHtml', start_offset+1)
    
htmljoin = Html('').join

def htmltag(_name_, _attrs_=None, **_attrs2_):
    attrs = {}
    for d in _attrs_, _attrs2_:
        if not d: continue
        for name, value in d.items():
            name = name.lower().strip('_').replace('_', '-')
            if name == 'class':
                value = (' '.join((attrs.get(name, ''), value))).strip()
                if not value: continue
            attrs[name] = value
    attrlist = []
    make_attr = Html('%s="%s"').__mod__
    for name, value in attrs.items():
        if value is True: attrlist.append(name)
        elif value is not False and value is not None:
            if isinstance(value, StrHtml): value = str.__str__(value)
            elif isinstance(value, Html): value = unicode(value)
            attrlist.append(make_attr((name, value)))
    return Html("<%s %s>") % (_name_, Html(' ').join(attrlist))

################################################################################

def string_consts_to_html(f):
    co = f.func_code
    consts = list(co.co_consts)
    for i, x in enumerate(consts):
        if isinstance(x, str): consts[i] = StrHtml(x)
        elif isinstance(x, unicode): consts[i] = Html(x)
    new_code = type(co)(co.co_argcount, co.co_nlocals, co.co_stacksize,
                        co.co_flags, co.co_code, tuple(consts), co.co_names,
                        co.co_varnames, co.co_filename, co.co_name,
                        co.co_firstlineno, co.co_lnotab, co.co_freevars,
                        co.co_cellvars)
    if f.func_defaults:
        defaults = list(f.func_defaults)
        for i, x in enumerate(defaults):
            if isinstance(x, str): defaults[i] = StrHtml(x)
            elif isinstance(x, unicode): defaults[i] = Html(x)
        defaults = tuple(defaults)
    else: defaults = None
    new_function = type(f)(new_code, f.func_globals, f.func_name,
                                     defaults, f.func_closure)
    return new_function

@decorator
def printtext(old_func):
    func = grab_stdout(old_func)
    def new_func(*args, **keyargs):
        return u''.join(func(*args, **keyargs))
    return new_func

@decorator
def printhtml(old_func):
    if pony.MODE.startswith('GAE-'):
        raise EnvironmentError('@printhtml decorator does not work inside Google AppEngine.\n'
                               'Use @http decorator and html() function instead.')
    decorators = getattr(old_func, 'decorators', set())
    if 'printhtml' in decorators: return old_func
    if decorators: raise TypeError(
        'Incorrect decorator order: @printhtml must be first decorator to apply\n'
        '(that is, @printhtml must be last decorator in program text).')
    func = grab_stdout(string_consts_to_html(old_func))
    def new_func(*args, **keyargs):
        return htmljoin(func(*args, **keyargs))
    return new_func

################################################################################

@decorator
def lazy(old_func):
    old_func.__lazy__ = True
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
        |   ( \.?[A-Za-z_]\w*               # multi-part name (group 7)
              (?:\s*\.\s*[A-Za-z_]\w*)*
            )
            (\s*(?:[(]|\\?[{]))?                   # start of statement content (group 8)
        )

    """, re.VERBOSE | re.DOTALL)

newline_re = re.compile(r'^ *\n')

def parse_markup(text, start_pos=0, nested=False):
    def remove_spaces(tree):
        for i in range(3, len(tree)):
            item = tree[i]
            prev = tree[i-1]
            if isinstance(item, basestring) and isinstance(prev, tuple):
                prev_markups = prev[4]
                if prev_markups: tree[i] = newline_re.sub('', item)
        if not nested: return tree
        assert text[start_pos-1] == '{'
        start = tree[2]
        if text[start_pos-2] != '\\':
            tree[2] = newline_re.sub('', start)
            return tree
        if isinstance(start, basestring): tree[2] = start.lstrip()
        end = tree[-1]
        if isinstance(end, basestring): tree[-1] = end.rstrip()
        return tree
    pos = start_pos
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
            return remove_spaces(joinstrings(result)), end
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
                    return remove_spaces(joinstrings(result)), end
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
        elif i >= 7:
            cmd_name = match.group(7)
            if cmd_name is not None and cmd_name.startswith('.'):
                if not is_ident(cmd_name[1:]): raise ParseError('Invalid method call', text, start)
            if i == 7: # $expression.path
                result.append((start, end, None, cmd_name, None))
            elif i == 8: # $function.call(...)
                command, end = parse_command(text, match.start(), end-1, cmd_name)
                result.append(command)
        pos = end

command_re = re.compile(r"""

    \s*                         # optional whitespace
    (?:                         
        (;)                     # end of command (group 1)
    |   (\\?[{])                   # start of markup block (group 2)
    )?

""", re.VERBOSE)
    
def parse_command(text, start, pos, name):
    exprlist = None
    if text[pos] == '(':
        exprlist, pos = parse_exprlist(text, pos+1)
    markup_args = []
    while True:
        match = command_re.match(text, pos)
        assert match
        end = match.end()
        i = match.lastindex
        if i is None:
            return (start, pos, name, exprlist, markup_args), pos
        elif i == 1:
            return (start, end, name, exprlist, markup_args), end
        elif i == 2:
            arg, end = parse_markup(text, end, True)
            markup_args.append(arg)
        else: assert False
        pos = end
   
exprlist_re = re.compile(r"""

        (                         # comments (group 1):
            \#.*?(?:\n|\Z)        # - Python-style comment inside expressions
        |   [$]?//.*?(?:\n|\Z)     # - $// comment
        |   [$]?/\*.*?(?:\*/|\Z)   # - $/* comment */
        )
    |   ([(])                     # open parenthesis (group 2)
    |   ([)])                     # close parenthesis (group 3)
    |   ([$])?[A-Za-z_]\w*        # Python identifier with optional $ ($ is group 4)
    |   '''(?:[^\\]|\\.)*?'''     # '''triple-quoted string'''
    |   \"""(?:[^\\]|\\.)*?\"""   # \"""triple-quoted string\"""
    |   '(?:[^'\\]|\\.)*?'        # 'string'
    |   "(?:[^"\\]|\\.)*?"        # "string"

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
        result.append(text[pos:start])
        if i == 1:  # comment
            result.append(' ')
        elif i == 2:
            counter += 1
            result.append('(')
        elif i == 3:               
            counter -= 1
            result.append(')')
            if counter == 0: return ''.join(result)[:-1], end
        elif i == 4: result.append(text[start+1:end])
        else: result.append(text[start:end])
        pos = end

class SyntaxElement(object):
    def _raise_unexpected_statement(self, item):
        pos = item[0]
        name = item[2]
        errpos = self.text.index(name, pos) + len(name)
        raise ParseError("Unexpected '%s' statement" % name, self.text, errpos)
    def _check_statement(self, item):
        cmd_name, expr, markup_args = item[2:]
        end_of_expr = markup_args and markup_args[0][0] or self.end
        if cmd_name in ('if', 'elif', 'for'):
            if not expr: raise ParseError("'%s' statement must contain expression" % cmd_name, self.text, end_of_expr)
        elif cmd_name in ('else', 'sep', 'separator', 'try'):
            if expr is not None: raise ParseError(
                "'%s' statement must not contains expression" % cmd_name, self.text, end_of_expr)
        if not markup_args: raise ParseError(
            "'%s' statement must contain markup block" % cmd_name, self.text, self.end)
        if len(markup_args) > 1: raise ParseError(
            "'%s' statement must contain exactly one markup block" % cmd_name, self.text, markup_args[1][0])

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
                if self.content: prev = self.content[-1]
                else: prev = None
                cmd_name = item[2]
                if cmd_name is not None and cmd_name.startswith('.') \
                   or cmd_name in ('elif', 'else', 'sep', 'separator', 'except'):
                    if isinstance(prev, basestring) and (not prev or prev.isspace()):
                        self.content.pop()
                        if self.content: prev = self.content[-1]
                        else: prev = None
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
                elif cmd_name == 'try':
                    self.content.append(TryElement(text, item))
                elif cmd_name == 'except':
                    if not isinstance(prev, TryElement):
                        self._raise_unexpected_statement(item)
                    prev.append_except(item)
                elif cmd_name.startswith('.'):
                    if not isinstance(prev, FunctionElement):
                        self._raise_unexpected_statement(item)
                    prev.append_method(item)
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
        return ''

var_list_re_1 = re.compile(r"""

    \s*
    (?:
        ([A-Za-z_]\w*)    # group 1
    |   ([[(])            # group 2
    )

    """, re.VERBOSE)

var_list_re_2 = re.compile(r"""

    \s*
    (?:
        (?: ,\s* )?     # trailing comma
        (?:
            ([])])      # group 1
        |   \b(in)\b    # group 2
        )
    |   (,)             # group 3
    )

    """, re.VERBOSE)

def parse_var_list(expr, pos, nested=False):
    result = []
    errmsg = "Incorrect Python expression inside 'for' statement"
    while True:
        match = var_list_re_1.match(expr, pos)
        if not match:
            if result and not nested and expr[pos:].isspace():
                return result
            raise ParseError(errmsg, expr, len(expr))
        pos = match.end()
        i = match.lastindex
        if i == 1:
            ident = match.group(1)
            result.append(match.group(1))
        elif i == 2:
            vars, pos = parse_var_list(expr, pos, True)
            result.extend(vars)
        else: assert False
        match = var_list_re_2.match(expr, pos)
        if not match: raise ParseError(errmsg, expr, len(expr))
        pos = match.end()
        i = match.lastindex
        if i == 1:
            if not nested: raise ParseError("Unexpected ')'", expr, pos)
            return result, pos
        elif i == 2:
            if nested: raise ParseError("')' expected", expr, pos)
            return result
        elif i == 3: pass
        else: assert False

for_re = re.compile(r"""
        '''(?:[^\\]|\\.)*?'''     # '''triple-quoted string'''
    |   \"""(?:[^\\]|\\.)*?\"""   # \"""triple-quoted string\"""
    |   '(?:[^'\\]|\\.)*?'        # 'string'
    |   "(?:[^"\\]|\\.)*?"        # "string"
    |   ;                         
    """, re.VERBOSE)

ident_re = re.compile(r'[A-Za-z_]\w*')

def parse_for(expr):
    elements = []
    while True:
        for match in for_re.finditer(expr):
            if match.group() == ';':
                elements.append(expr[:match.start()])
                expr = expr[match.end():]
                break
        else: elements.append(expr); break
    expr = elements[0]
    assignments = []
    for s in elements[1:]:
        if '=' not in s: raise ParseError('Invalid assignment', s, 0)
        a, b = s.split('=', 1)
        var_names = ident_re.findall(a)
        compile(b.lstrip(), '<?>', 'eval') # check; can raise exception
        code = compile(s.lstrip(), '<?>', 'single')
        assignments.append((var_names, code))
    return expr, assignments

class ForElement(SyntaxElement):
    def __init__(self, text, item):
        self.text = text
        self.empty = text.__class__()
        self.start, self.end = item[:2]
        self._check_statement(item)
        self.expr, self.assignments = parse_for(item[3])
        self.markup = Markup(text, item[4][0])
        self.var_names = parse_var_list(self.expr, 0)
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
        all_var_names = self.var_names + [ 'for' ]
        for ass_names, _ in self.assignments: all_var_names.extend(ass_names)
        for name in all_var_names:
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
            for _, code in self.assignments: exec code in globals, locals
            result.append(self.markup.eval(globals, locals))
        for name, old_value in zip(all_var_names, old_values):
            if old_value is not_found: del locals[name]
            else: locals[name] = old_value
        return self.empty.join(result)

class TryElement(SyntaxElement):
    def __init__(self, text, item):
        self.text = text
        self.start, self.end = item[:2]
        self._check_statement(item)
        self.markup = Markup(text, item[4][0])
        self.except_list = []
        self.else_ = None
    def append_except(self, item):
        self._check_statement(item)
        self.end = item[1]
        expr, markup_args = item[3:5]
        if expr is None: code = None
        else: code = compile(expr.lstrip(), '<?>', 'eval')
        self.except_list.append((code, Markup(self.text, markup_args[0])))
    def eval(self, globals, locals=None):
        try: return self.markup.eval(globals, locals)
        except Exception:
            exc_type, exc_value, traceback = sys.exc_info()
            try:
                for code, markup in self.except_list:
                    if code is None: return markup.eval(globals, locals)
                    exc = eval(code, globals, locals)
                    if issubclass(exc_type, exc): return markup.eval(globals, locals)
                raise exc_type, exc_value, traceback
            finally: del traceback
                
class ExprElement(SyntaxElement):
    def __init__(self, text, item):
        self.text = text
        self.start, self.end, cmd_name, self.expr, markup_args = item
        assert cmd_name is None
        self.expr_code = compile(self.expr, '<?>', 'eval')
        if not markup_args: self.markup = None
        elif len(markup_args) > 1: raise ParseError('Unexpected markup block', text, markup_args[0][0])
        else: self.markup = Markup(text, markup_args[0])
    def eval(self, globals, locals=None):
        try: result = eval(self.expr_code, globals, locals)
        except:
            if not self.markup: raise
            return self.markup.eval(globals, locals)
        if inspect.isroutine(result): result = result()
        if isinstance(result, basestring): return result
        return unicode(result)

space_re = re.compile(r'\s+')

class I18nElement(SyntaxElement):
    def __init__(self, text, item):
        self.text = text
        self.start, self.end, cmd_name, expr, markup_args = item
        assert cmd_name is None and expr is None
        self.markup = Markup(text, markup_args[0])
        if len(markup_args) > 1: raise ParseError('Unexpected markup block', text, markup_args[1][0])
        self.items = [ item for item in self.markup.content if not isinstance(item, basestring) ]
        key_list = [ not isinstance(item, basestring) and '$#' or item.replace('$', '$$')
                     for item in self.markup.content ]
        self.key = ' '.join(''.join(key_list).split())
    def eval(self, globals, locals=None):
        params = [ item.eval(globals, locals) for item in self.items ]
        if 'pony.web' in sys.modules:
            from pony.web import http
            languages = http.request.languages
        else: languages = []
        result = i18n.translate(self.key, params, languages)
        if result is not None: return result
        params.reverse()
        result = []
        for element in self.markup.content:
            if isinstance(element, basestring): result.append(element)
            else: result.append(params.pop())
        return self.markup.empty.join(result)

_func_code = compile('__pony_func__(*__pony_args__, **__pony_keyargs__)', '<?>', 'eval')

class FunctionElement(SyntaxElement):
    def __init__(self, text, item):
        self.text = text
        self.start, self.end, self.expr, self.params, markup_args = item
        self.params = self.params or ''
        self.markup_args = [ Markup(text, item) for item in markup_args ]
        self.func_code = compile(self.expr, '<?>', 'eval')
        s = '(lambda *args, **keyargs: (list(args), keyargs))(%s)' % self.params
        self.params_code = compile(s, '<?>', 'eval')
        self.methods = []
    def append_method(self, item):
        start, end, expr, params, markup_args = item
        assert expr.startswith('.')
        method_name = expr[1:]
        params = params or ''
        markup_args = [ Markup(self.text, item) for item in markup_args ]
        s = '(lambda *args, **keyargs: (list(args), keyargs))(%s)' % self.params
        params_code = compile(s, '<?>', 'eval')
        self.methods.append((method_name, params_code, markup_args))
    def eval(self, globals, locals=None):
        func = eval(self.func_code, globals, locals)
        args, keyargs = eval(self.params_code, globals, locals)
        if getattr(func, '__lazy__', False):
            args.extend([BoundMarkup(m, globals, locals) for m in self.markup_args])
        else:
            for arg in self.markup_args: args.append(arg.eval(globals, locals))
        if locals is None: locals = {}
        locals['__pony_func__'] = func
        locals['__pony_args__'] = args
        locals['__pony_keyargs__'] = keyargs
        result = eval(_func_code, globals, locals)
        for method_name, params_code, markup_args in self.methods:
            method = getattr(result, method_name)
            args, keyargs = eval(params_code, globals, locals)
            if getattr(method, '__lazy__', False):
                args.extend([BoundMarkup(m, globals, locals) for m in markup_args])
            else:
                for arg in markup_args: args.append(arg.eval(globals, locals))
            method(*args, **keyargs)
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

template_string_cache = {}

def markup_from_string(str_cls, s,
                         encoding=None, keep_indent=False, caching=True):
    if caching:
        try: return template_string_cache[s]
        except KeyError: pass
    if isinstance(s, unicode): text = s
    else: text = unicode(s, encoding or 'ascii', errors='replace')
    if not keep_indent: text = textwrap.dedent(text)
    tree = parse_markup(text)[0]
    markup = Markup(str_cls(text), tree)
    if caching: template_string_cache[s] = markup
    return markup

template_file_cache = {}

redirect_prefix = 'see template: '
translation_prefix = 'see translation for: '

def markup_from_file(str_cls, filename, encoding=None):
    key = filename, str_cls, encoding
    mtime = get_mtime(filename)
    old_mtime, markup = template_file_cache.get(key, (None, None))
    if markup and mtime == old_mtime: return markup
    text = read_text_file(filename, encoding)

    if text.startswith(redirect_prefix):
        new_filename = text[len(redirect_prefix):].strip()
        return markup_from_file(str_cls, new_filename, encoding)

    if text.startswith(translation_prefix):
        lang = text[len(redirect_prefix):].strip().lower()
        root, ext = os.path.splitext(filename)
        root, _ = root.split('-', 1)
        new_filename = '%s-%s%s' % (root, lang, ext)
        return markup_from_file(str_cls, new_filename, encoding)

    markup = markup_from_string(str_cls, text, encoding, True, False)
    template_file_cache[filename] = mtime, markup
    return markup

def markup_from_file_i18n(str_cls, filename, encoding=None):
    if 'pony.web' in sys.modules:
        from pony.web import http
        root, ext = os.path.splitext(filename)
        for lang in http.request.languages:
            i18n_filename = '%s-%s%s' % (root, lang, ext)
            try: return markup_from_file(str_cls, i18n_filename, encoding)
            except OSError, IOError: pass
    return markup_from_file(str_cls, filename, encoding)

def _template(str_cls, default_ext,
              text=None, filename=None, globals=None, locals=None, encoding=None, keep_indent=None):
    if text is not None and filename is not None: raise TypeError(
        "template function cannot accept both 'text' and 'filename' parameters at the same time")
    if text is None:
        if filename is None: filename = get_template_name(sys._getframe(2)) + default_ext
        if keep_indent is not None: raise TypeError(
            "'keep_indent' argument cannot be used for file-based templates")
        markup = markup_from_file_i18n(str_cls, filename, encoding)
    else: markup = markup_from_string(str_cls, text, encoding, keep_indent)
    if globals is None and locals is None:
        globals = sys._getframe(2).f_globals
        locals = sys._getframe(2).f_locals
    return markup.eval(globals, locals)

def template(*args, **keyargs):
    return _template(unicode, '.txt', *args, **keyargs)

def html(*args, **keyargs):
    return _template(Html, '.html', *args, **keyargs)
