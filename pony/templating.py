from __future__ import absolute_import, print_function
from pony.py23compat import izip

import itertools, sys, os.path, threading, inspect, re, weakref, textwrap, copy_reg

import pony
from pony import options, i18n, utils
from pony.utils import read_text_file, is_ident, get_mtime, make_offsets, pos2lineno, getline

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
        return quote(repr(self.value))
    def __getitem__(self, key):
        return _wrap(self.value[key], self.unicode_replace)

class StrWrapper(str):
    def __repr__(self):
        return quote(repr(self.original_value))

class UnicodeWrapper(unicode):
    __slots__ = [ 'original_value' ]
    def __repr__(self):
        return quote(repr(self.original_value))

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
            attrlist.append(make_attr((name, value)))
    return Html("<%s%s%s>") % (_name_, attrlist and ' ' or '', Html(' ').join(attrlist))

################################################################################

def lazy(func):
    func.__lazy__ = True
    return func

def _compile(s):
    return compile(s, '<?>', 'eval')

def _eval(code, globals, locals):
    return eval(code, globals, locals)

class ParseError(Exception):
    def __init__(self, message, source, pos):
        self.message = message
        self.source = source
        self.pos = pos
        text, offsets = source[:2]
        self.lineno, self.offset = pos2lineno(pos, offsets)
        self.line = getline(text, offsets, self.lineno)
        Exception.__init__(self, '%s: %s' % (message, self.line))

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
    |   @(?:
            (@)                        # double @ (group 3)
        |   (                            # comments (group 4)
                //.*?(?:\n|\Z)           #     @// comment
            |   /\*.*?(?:\*/|\Z)         #     @/* comment */
            )
        |   ([(])                        # start of @(expression) (group 5)
        |   ([{])                        # start of @{markup} (group 6)
        |   ( \+?[A-Za-z_]\w*               # multi-part name (group 7)
              (?:\s*\.\s*[A-Za-z_]\w*)*
            )
            (\s*(?:[(]|\\?[{]))?                   # start of statement content (group 8)
        )

    """, re.VERBOSE | re.DOTALL)

newline_re = re.compile(r'^ *\n')

def parse_markup(source, start_pos=0, nested=False):
    text = source[0]
    def remove_spaces(tree):
        for i in range(3, len(tree)):
            item = tree[i]
            prev = tree[i-1]
            if isinstance(item, basestring) and isinstance(prev, tuple):
                prev_markups = prev[4]
                if prev_markups: tree[i] = newline_re.sub('', item)
        if not nested or len(tree) == 2: return tree
        assert text[start_pos-1] == '{'
        start = tree[2]
        if text[start_pos-2] != '\\':
            if isinstance(start, basestring): tree[2] = newline_re.sub('', start)
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
                raise ParseError('Unexpected end of text', source, len(text))
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
                raise ParseError("Unexpected symbol '}'", source, end)
            brace_counter -= 1
            result.append('}')
        elif i == 3: # @@
            result.append('@')
        elif i == 4: # @/* comment */ or @// comment
            pass
        elif i in (5, 6): # @(expression) or @{i18n markup}
            command, end = parse_command(source, start, end-1, None)
            result.append(command)
        elif i >= 7:
            cmd_name = match.group(7)
            if cmd_name is not None and cmd_name.startswith('+'):
                if not is_ident(cmd_name[1:]): raise ParseError('Invalid method call', source, start)
            if i == 7: # @expression
                try: expr, _ = utils.parse_expr(text, start+1)
                except ValueError: raise ParseError('Invalid Python expression', source, start+1)
                end = start+1 + len(expr)
                if expr.endswith(';'): expr = expr[:-1]
                result.append((start, end, None, expr, None))
            elif i == 8: # @function.call(...)
                command, end = parse_command(source, match.start(), end-1, cmd_name)
                result.append(command)
        pos = end

command_re = re.compile(r"""

    \s*                         # optional whitespace
    (?:
        (;)                     # end of command (group 1)
    |   (\\?[{])                   # start of markup block (group 2)
    )?

""", re.VERBOSE)

def parse_command(source, start, pos, name):
    text = source[0]
    exprlist = None
    if text[pos] == '(': exprlist, pos = parse_exprlist(source, pos+1)
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
            arg, end = parse_markup(source, end, True)
            markup_args.append(arg)
        else: assert False  # pragma: no cover
        pos = end

exprlist_re = re.compile(r"""

        (                         # comments (group 1):
            \#.*?(?:\n|\Z)        # - Python-style comment inside expressions
        |   @?//.*?(?:\n|\Z)     # - @// comment
        |   @?/\*.*?(?:\*/|\Z)   # - @/* comment */
        )
    |   ([(])                     # open parenthesis (group 2)
    |   ([)])                     # close parenthesis (group 3)
    |   (@)?[A-Za-z_]\w*        # Python identifier with optional @ (@ is group 4)
    |   '''(?:[^\\]|\\.)*?'''     # '''triple-quoted string'''
    |   \"""(?:[^\\]|\\.)*?\"""   # \"""triple-quoted string\"""
    |   '(?:[^'\\]|\\.)*?'        # 'string'
    |   "(?:[^"\\]|\\.)*?"        # "string"

    """, re.VERBOSE)

def parse_exprlist(source, pos):
    text = source[0]
    result = []
    counter = 1
    while True:
        match = exprlist_re.search(text, pos)
        if not match: raise ParseError('Unexpected end of text', source, len(text))
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
    def _raise_unexpected_statement(elem, item):
        pos = item[0]
        name = item[2]
        errpos = elem.source[0].index(name, pos) + len(name)
        raise ParseError("Unexpected '%s' statement" % name, elem.source, errpos)
    def _check_statement(elem, item):
        cmd_name, expr, markup_args = item[2:]
        end_of_expr = markup_args[0][0] if markup_args else elem.end
        if cmd_name in ('if', 'elif', 'for'):
            if not expr: raise ParseError("'%s' statement must contain expression" % cmd_name, elem.source, end_of_expr)
        elif cmd_name in ('else', 'sep', 'separator', 'try'):
            if expr is not None: raise ParseError(
                "'%s' statement must not contains expression" % cmd_name, elem.source, end_of_expr)
        if not markup_args: raise ParseError(
            "'%s' statement must contain markup block" % cmd_name, elem.source, elem.end)
        if len(markup_args) > 1: raise ParseError(
            "'%s' statement must contain exactly one markup block" % cmd_name, elem.source, markup_args[1][0])

class Markup(SyntaxElement):
    def __init__(elem, source, tree):
        # do not rename 'elem' to 'markup'! It is important for pony.debugging.format_exc() function
        assert isinstance(tree, list)
        text = source[0]
        elem.source = source
        elem.empty = text.__class__()
        elem.start, elem.end = tree[:2]
        elem.content = []
        for item in tree[2:]:
            if isinstance(item, basestring):
                elem.content.append(text.__class__(item))
            elif isinstance(item, tuple):
                if elem.content: prev = elem.content[-1]
                else: prev = None
                cmd_name = item[2]
                if cmd_name is not None and cmd_name.startswith('+') \
                   or cmd_name in ('elif', 'else', 'sep', 'separator', 'except'):
                    if isinstance(prev, basestring) and (not prev or prev.isspace()):
                        elem.content.pop()
                        if elem.content: prev = elem.content[-1]
                        else: prev = None
                if cmd_name is None:
                    if item[3]: elem.content.append(ExprElement(source, item))
                    else: elem.content.append(I18nElement(source, item))
                elif cmd_name == 'if':
                    elem.content.append(IfElement(source, item))
                elif cmd_name == 'elif':
                    if not isinstance(prev, IfElement):
                        elem._raise_unexpected_statement(item)
                    prev.append_elif(item)
                elif cmd_name == 'else':
                    if not isinstance(prev, (IfElement, ForElement)):
                        elem._raise_unexpected_statement(item)
                    prev.append_else(item)
                elif cmd_name == 'for':
                    elem.content.append(ForElement(source, item))
                elif cmd_name in ('sep', 'separator'):
                    if not isinstance(prev, ForElement):
                        elem._raise_unexpected_statement(item)
                    prev.append_separator(item)
                elif cmd_name == 'try':
                    elem.content.append(TryElement(source, item))
                elif cmd_name == 'except':
                    if not isinstance(prev, TryElement):
                        elem._raise_unexpected_statement(item)
                    prev.append_except(item)
                elif cmd_name.startswith('+'):
                    if not isinstance(prev, FunctionElement):
                        elem._raise_unexpected_statement(item)
                    prev.append_method(item)
                else: elem.content.append(FunctionElement(source, item))
            else: assert False  # pragma: no cover
    def eval(elem, globals, locals=None):
        result = []
        for element in elem.content:
            if isinstance(element, basestring): result.append(element)
            else: result.append(element.eval(globals, locals))
        return elem.empty.join(result)

class IfElement(SyntaxElement):
    def __init__(elem, source, item):
        elem.source = source
        elem.start, elem.end = item[:2]
        elem.chain = []
        elem._append(item)
    def append_elif(elem, item):
        if elem.chain[-1][0] is None: elem._raise_unexpected_statement(item)
        elem._append(item)
    def append_else(elem, item):
        if elem.chain[-1][0] is None: elem._raise_unexpected_statement(item)
        elem._append(item)
    def _append(elem, item):
        elem._check_statement(item)
        end, expr, markup_args = item[1], item[3], item[4]
        elem.end = end
        if expr is None: expr_code = None
        else: expr_code = _compile(expr.lstrip())
        elem.chain.append((expr, expr_code, Markup(elem.source, markup_args[0])))
    def eval(elem, globals, locals=None):
        for expr, expr_code, markup in elem.chain:
            if expr is None or _eval(expr_code, globals, locals):
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

def parse_var_list(source, start, expr, pos, nested=False):
    result = []
    errmsg = "Incorrect Python expression inside 'for' statement"
    while True:
        match = var_list_re_1.match(expr, pos)
        if not match:
            if result and not nested and expr[pos:].isspace():
                return result
            raise ParseError(errmsg, source, start)
        pos = match.end()
        i = match.lastindex
        if i == 1:
            ident = match.group(1)
            result.append(match.group(1))
        elif i == 2:
            vars, pos = parse_var_list(source, start, expr, pos, True)
            result.extend(vars)
        else: assert False  # pragma: no cover
        match = var_list_re_2.match(expr, pos)
        if not match: raise ParseError(errmsg, source, start)
        pos = match.end()
        i = match.lastindex
        if i == 1:
            if not nested: raise ParseError("Unexpected ')'", source, start)
            return result, pos
        elif i == 2:
            if nested: raise ParseError("')' expected. Got: 'in'", source, start)
            return result
        elif i == 3: pass
        else: assert False  # pragma: no cover

for_re = re.compile(r"""
        '''(?:[^\\]|\\.)*?'''     # '''triple-quoted string'''
    |   \"""(?:[^\\]|\\.)*?\"""   # \"""triple-quoted string\"""
    |   '(?:[^'\\]|\\.)*?'        # 'string'
    |   "(?:[^"\\]|\\.)*?"        # "string"
    |   ;
    """, re.VERBOSE)

ident_re = re.compile(r'[A-Za-z_]\w*')

class ForElement(SyntaxElement):
    def __init__(elem, source, item):
        text = source[0]
        elem.source = source
        elem.empty = text.__class__()
        elem.start, elem.end = item[:2]
        elem._check_statement(item)
        elem.expr, elem.assignments = elem._parse_for(item[3])
        elem.markup = Markup(source, item[4][0])
        elem.var_names = parse_var_list(elem.source, elem.start, elem.expr, 0)
        elem.separator = elem.else_ = None
        var_list = ', '.join(elem.var_names)
        list_expr = '[ (%s,) for %s ]' % (var_list, elem.expr)
        elem.code = _compile(list_expr)
    def _parse_for(elem, expr):
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
            if '=' not in s: raise ParseError('Invalid assignment: %r' % s, elem.source, elem.start)
            a, b = s.split('=', 1)
            var_names = ident_re.findall(a)
            _compile(b.lstrip()) # check; can raise exception
            code = compile(s.lstrip(), '<?>', 'single')
            assignments.append((var_names, code))
        return expr, assignments
    def append_separator(elem, item):
        if elem.separator or elem.else_: elem._raise_unexpected_statement(item)
        elem._check_statement(item)
        elem.end = item[1]
        elem.separator = Markup(elem.source, item[4][0])
    def append_else(elem, item):
        if elem.else_: elem._raise_unexpected_statement(item)
        elem._check_statement(item)
        elem.end = item[1]
        elem.else_ = Markup(elem.source, item[4][0])
    def eval(elem, globals, locals=None):
        if locals is None: locals = {}
        not_found = object()
        old_values = []
        all_var_names = elem.var_names + [ 'for' ]
        for ass_names, _ in elem.assignments: all_var_names.extend(ass_names)
        for name in all_var_names:
            old_values.append(locals.get(name, not_found))
        result = []
        list = _eval(elem.code, globals, locals)
        if not list:
            if elem.else_: return elem.else_.eval(globals, locals)
            return ''
        for i, item in enumerate(list):
            if i and elem.separator:
                result.append(elem.separator.eval(globals, locals))
            for name, value in izip(elem.var_names, item): locals[name] = value
            locals['for'] = i, len(list)
            for _, code in elem.assignments: exec code in globals, locals
            result.append(elem.markup.eval(globals, locals))
        for name, old_value in izip(all_var_names, old_values):
            if old_value is not_found: del locals[name]
            else: locals[name] = old_value
        return elem.empty.join(result)

class TryElement(SyntaxElement):
    def __init__(elem, source, item):
        elem.source = source
        elem.start, elem.end = item[:2]
        elem._check_statement(item)
        elem.markup = Markup(source, item[4][0])
        elem.except_list = []
        elem.else_ = None
    def append_except(elem, item):
        elem._check_statement(item)
        elem.end = item[1]
        expr, markup_args = item[3:5]
        if expr is None: code = None
        else: code = _compile(expr.lstrip())
        elem.except_list.append((code, Markup(elem.source, markup_args[0])))
    def eval(elem, globals, locals=None):
        try: return elem.markup.eval(globals, locals)
        except Exception:
            exc_type, exc_value, traceback = sys.exc_info()
            try:
                for code, markup in elem.except_list:
                    if code is None: return markup.eval(globals, locals)
                    exc = _eval(code, globals, locals)
                    if issubclass(exc_type, exc): return markup.eval(globals, locals)
                raise exc_type, exc_value, traceback
            finally: del traceback

class ExprElement(SyntaxElement):
    def __init__(elem, source, item):
        elem.source = source
        elem.start, elem.end, cmd_name, elem.expr, markup_args = item
        assert cmd_name is None
        elem.expr_code = _compile(elem.expr)
        if not markup_args: elem.markup = None
        elif len(markup_args) > 1: raise ParseError('Unexpected markup block', source, markup_args[0][0])
        else: elem.markup = Markup(source, markup_args[0])
    def eval(elem, globals, locals=None):
        try: result = _eval(elem.expr_code, globals, locals)
        except:
            if not elem.markup: raise
            return elem.markup.eval(globals, locals)
        if inspect.isroutine(result): result = result()
        if isinstance(result, basestring): return result
        return unicode(result)

space_re = re.compile(r'\s+')

class I18nElement(SyntaxElement):
    def __init__(elem, source, item):
        elem.source = source
        elem.start, elem.end, cmd_name, expr, markup_args = item
        assert cmd_name is None and expr is None
        elem.markup = Markup(source, markup_args[0])
        if len(markup_args) > 1: raise ParseError('Unexpected markup block', source, markup_args[1][0])
        elem.items = [ item for item in elem.markup.content if not isinstance(item, basestring) ]
        key_list = [ '$#' if not isinstance(item, basestring) else item.replace('$', '$$')
                     for item in elem.markup.content ]
        elem.key = ' '.join(''.join(key_list).split())
    def eval(elem, globals, locals=None):
        params = [ item.eval(globals, locals) for item in elem.items ]
        if 'pony.web' in sys.modules:
            from pony.web import http
            languages = http.request.languages
        else: languages = []
        result = i18n.translate(elem.key, params, languages)
        if result is not None: return result
        params.reverse()
        result = []
        for element in elem.markup.content:
            if isinstance(element, basestring): result.append(element)
            else: result.append(params.pop())
        return elem.markup.empty.join(result)

_func_code = compile('__pony_func__(*__pony_args__, **__pony_keyargs__)', '<?>', 'eval')

class FunctionElement(SyntaxElement):
    def __init__(elem, source, item):
        elem.source = source
        elem.start, elem.end, elem.expr, elem.params, markup_args = item
        elem.params = elem.params or ''
        elem.markup_args = [ Markup(source, item) for item in markup_args ]
        elem.func_code = _compile(elem.expr)
        s = '(lambda *args, **kwargs: (list(args), kwargs))(%s)' % elem.params
        elem.params_code = _compile(s)
        elem.methods = []
    def append_method(elem, item):
        start, end, expr, params, markup_args = item
        assert expr.startswith('+')
        method_name = expr[1:]
        params = params or ''
        markup_args = [ Markup(elem.source, item) for item in markup_args ]
        s = '(lambda *args, **kwargs: (list(args), kwargs))(%s)' % params
        params_code = _compile(s)
        elem.methods.append((method_name, params_code, markup_args))
    def eval(elem, globals, locals=None):
        func = _eval(elem.func_code, globals, locals)
        args, kwargs = _eval(elem.params_code, globals, locals)
        if getattr(func, '__lazy__', False):
            args.extend([BoundMarkup(m, globals, locals) for m in elem.markup_args])
        else:
            for arg in elem.markup_args: args.append(arg.eval(globals, locals))
        if locals is None: locals = {}
        locals['__pony_func__'] = func
        locals['__pony_args__'] = args
        locals['__pony_keyargs__'] = kwargs
        result = _eval(_func_code, globals, locals)
        for method_name, params_code, markup_args in elem.methods:
            method = getattr(result, method_name)
            args, kwargs = _eval(params_code, globals, locals)
            if getattr(method, '__lazy__', False):
                  args.extend([BoundMarkup(m, globals, locals) for m in markup_args])
            else: args.extend([ m.eval(globals, locals) for m in markup_args ])
            method(*args, **kwargs)
        if inspect.isroutine(result): result = result()
        if isinstance(result, basestring): return result
        return unicode(result)

class BoundMarkup(object):
    def __init__(elem, markup, globals, locals=None):
        elem.markup = markup
        elem.globals = globals
        elem.locals = locals
    def __call__(elem):
        return elem.markup.eval(elem.globals, elem.locals)

def _cycle_check(args):
    for arg in args:
        if not isinstance(arg, BoundMarkup):
            raise TypeError('Incorrect type of argument: %s' % str(type(arg)))
    locals = args[-1].locals
    if locals is None or 'for' not in locals: raise TypeError(
        '@cycle() function may only be called inside a @for loop')
    return locals['for']

@lazy
def cycle(*args):
    if len(args) < 2: return itertools.cycle(*args)
    if isinstance(args[0], BoundMarkup):
        current, total = _cycle_check(args[1:])
        return args[current % len(args)]()
    elif isinstance(args[0], basestring):
        if len(args) != 2: raise TypeError(
            'When first argument is string, @cycle() function '
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
        raise TypeError('First argument of @cycle() function must be int, '
                        'string or markup. Got: %s' % str(type(args[0])))
    elif isinstance(args[1], (int, long)):
        if len(args) == 2: raise TypeError('Markup expected')
        if len(args) > 3: raise TypeError('@cycle() function got too many arguments')
        current, total = _cycle_check(args[2:])
        i, j, markup = args
        return markup() if current % j == i - 1 else ''
    else:
        if len(args) > 2: raise TypeError('@cycle() function got too many arguments')
        current, total = _cycle_check(args[1:])
        i, markup = args
        if i > 0: return markup() if current == i - 1 else ''
        elif i < 0: return markup() if i == current - total else ''
        else: raise TypeError('@cycle first argument cannot be 0')

try: __builtins__['cycle'] = cycle
except:  # Just in case... I'm not sure is it needed
    try: __builtins__.cycle = cycle
    except: pass

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

template_string_cache = {}

def markup_from_string(str_cls, s, encoding=None, keep_indent=False, caching=True, filename=None):
    if caching:
        try: return template_string_cache[s]
        except KeyError: pass
    if not isinstance(s, unicode): s = unicode(s, encoding or 'ascii', errors='replace')
    if not keep_indent: s = textwrap.dedent(s)
    source = str_cls(s), make_offsets(s), filename
    tree = parse_markup(source)[0]
    markup = Markup(source, tree)
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
    s = read_text_file(filename, encoding)

    if s.startswith(redirect_prefix):
        new_filename = s[len(redirect_prefix):].strip()
        return markup_from_file(str_cls, new_filename, encoding)

    if s.startswith(translation_prefix):
        lang = s[len(redirect_prefix):].strip().lower()
        root, ext = os.path.splitext(filename)
        root, _ = root.split('-', 1)
        new_filename = '%s-%s%s' % (root, lang, ext)
        return markup_from_file(str_cls, new_filename, encoding)

    markup = markup_from_string(str_cls, s, encoding, True, False, filename)
    template_file_cache[filename] = mtime, markup
    return markup

def markup_from_file_i18n(str_cls, filename, encoding=None):
    if 'pony.web' in sys.modules:
        from pony.web import http
        root, ext = os.path.splitext(filename)
        for lang in http.request.languages:
            i18n_filename = '%s-%s%s' % (root, lang, ext)
            try: return markup_from_file(str_cls, i18n_filename, encoding)
            except (OSError, IOError): pass
    return markup_from_file(str_cls, filename, encoding)

def _template(str_cls, default_ext, text=None, filename=None, globals=None, locals=None, encoding=None, keep_indent=None):
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

def template(*args, **kwargs):
    return _template(unicode, '.txt', *args, **kwargs)

def html(*args, **kwargs):
    return _template(Html, '.html', *args, **kwargs)
