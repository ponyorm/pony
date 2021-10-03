from __future__ import absolute_import, print_function, division
from pony.py23compat import PY2, izip, imap, itervalues, basestring, unicode, buffer, int_types

from operator import attrgetter
from decimal import Decimal
from datetime import date, datetime, timedelta
from binascii import hexlify

from pony import options
from pony.utils import datetime2timestamp, throw, is_ident
from pony.converting import timedelta2str
from pony.orm.ormtypes import RawSQL, Json

class AstError(Exception): pass

class Param(object):
    __slots__ = 'style', 'id', 'paramkey', 'converter', 'optimistic'
    def __init__(param, paramstyle, paramkey, converter=None, optimistic=False):
        param.style = paramstyle
        param.id = None
        param.paramkey = paramkey
        param.converter = converter
        param.optimistic = optimistic
    def eval(param, values):
        varkey, i, j = param.paramkey
        value = values[varkey]
        if i is not None:
            t = type(value)
            if t is tuple: value = value[i]
            elif t is RawSQL: value = value.values[i]
            elif hasattr(value, '_get_items'): value = value._get_items()[i]
            else: assert False, t
        if j is not None:
            assert type(type(value)).__name__ == 'EntityMeta'
            value = value._get_raw_pkval_()[j]
        converter = param.converter
        if value is not None and converter is not None:
            if converter.attr is None:
                value = converter.val2dbval(value)
            value = converter.py2sql(value)
        return value
    def __unicode__(param):
        paramstyle = param.style
        if paramstyle == 'qmark': return u'?'
        elif paramstyle == 'format': return u'%s'
        elif paramstyle == 'numeric': return u':%d' % param.id
        elif paramstyle == 'named': return u':p%d' % param.id
        elif paramstyle == 'pyformat': return u'%%(p%d)s' % param.id
        else: throw(NotImplementedError)
    if not PY2: __str__ = __unicode__
    def __repr__(param):
        return '%s(%r)' % (param.__class__.__name__, param.paramkey)

class CompositeParam(Param):
    __slots__ = 'items', 'func'
    def __init__(param, paramstyle, paramkey, items, func):
        for item in items: assert isinstance(item, (Param, Value)), item
        Param.__init__(param, paramstyle, paramkey)
        param.items = items
        param.func = func
    def eval(param, values):
        args = [ item.eval(values) if isinstance(item, Param) else item.value for item in param.items ]
        return param.func(args)

class Value(object):
    __slots__ = 'paramstyle', 'value'
    def __init__(self, paramstyle, value):
        self.paramstyle = paramstyle
        self.value = value
    def __unicode__(self):
        value = self.value
        if value is None:
            return 'null'
        if isinstance(value, bool):
            return value and '1' or '0'
        if isinstance(value, basestring):
            return self.quote_str(value)
        if isinstance(value, datetime):
            return 'TIMESTAMP ' + self.quote_str(datetime2timestamp(value))
        if isinstance(value, date):
            return 'DATE ' + self.quote_str(str(value))
        if isinstance(value, timedelta):
            return "INTERVAL '%s' HOUR TO SECOND" % timedelta2str(value)
        if PY2:
            if isinstance(value, (int, long, float, Decimal)):
                return str(value)
            if isinstance(value, buffer):
                return "X'%s'" % hexlify(value)
        else:
            if isinstance(value, (int, float, Decimal)):
                return str(value)
            if isinstance(value, bytes):
                return "X'%s'" % hexlify(value).decode('ascii')
        assert False, repr(value)  # pragma: no cover
    if not PY2:
        __str__ = __unicode__
    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__, self.value)
    def quote_str(self, s):
        if self.paramstyle in ('format', 'pyformat'): s = s.replace('%', '%%')
        return "'%s'" % s.replace("'", "''")

def flat(tree):
    stack = [ tree ]
    result = []
    stack_pop = stack.pop
    stack_extend = stack.extend
    result_append = result.append
    while stack:
        x = stack_pop()
        if isinstance(x, basestring): result_append(x)
        else:
            try: stack_extend(x)
            except TypeError: result_append(x)
    return result[::-1]

def flat_conditions(conditions):
    result = []
    for condition in conditions:
        if condition[0] == 'AND':
            result.extend(flat_conditions(condition[1:]))
        else: result.append(condition)
    return result

def join(delimiter, items):
    items = iter(items)
    try: result = [ next(items) ]
    except StopIteration: return []
    for item in items:
        result.append(delimiter)
        result.append(item)
    return result

def move_conditions_from_inner_join_to_where(sections):
    new_sections = list(sections)
    for i, section in enumerate(sections):
        if section[0] == 'FROM':
            new_from_list = [ 'FROM' ] + [ list(item) for item in section[1:] ]
            new_sections[i] = new_from_list
            if len(sections) > i+1 and sections[i+1][0] == 'WHERE':
                new_where_list = list(sections[i+1])
                new_sections[i+1] = new_where_list
            else:
                new_where_list = [ 'WHERE' ]
                new_sections.insert(i+1, new_where_list)
            break
    else: return sections
    for join in new_from_list[2:]:
        if join[1] in ('TABLE', 'SELECT') and len(join) == 4:
            new_where_list.append(join.pop())
    return new_sections

def make_binary_op(symbol, default_parentheses=False):
    def binary_op(builder, expr1, expr2, parentheses=None):
        if parentheses is None: parentheses = default_parentheses
        if parentheses: return '(', builder(expr1), symbol, builder(expr2), ')'
        return builder(expr1), symbol, builder(expr2)
    return binary_op

def make_unary_func(symbol):
    def unary_func(builder, expr):
        return '%s(' % symbol, builder(expr), ')'
    return unary_func

def indentable(method):
    def new_method(builder, *args, **kwargs):
        result = method(builder, *args, **kwargs)
        if builder.indent <= 1: return result
        return builder.indent_spaces * (builder.indent-1), result
    new_method.__name__ = method.__name__
    return new_method

class SQLBuilder(object):
    dialect = None
    param_class = Param
    composite_param_class = CompositeParam
    value_class = Value
    indent_spaces = " " * 4
    least_func_name = 'least'
    greatest_func_name = 'greatest'
    def __init__(builder, provider, ast):
        builder.provider = provider
        builder.quote_name = provider.quote_name
        builder.paramstyle = paramstyle = provider.paramstyle
        builder.ast = ast
        builder.indent = 0
        builder.keys = {}
        builder.inner_join_syntax = options.INNER_JOIN_SYNTAX
        builder.suppress_aliases = False
        builder.result = flat(builder(ast))
        params = tuple(x for x in builder.result if isinstance(x, Param))
        layout = []
        for i, param in enumerate(params):
            if param.id is None: param.id = i + 1
            layout.append(param.paramkey)
        builder.layout = layout
        builder.sql = u''.join(imap(unicode, builder.result)).rstrip('\n')
        if paramstyle in ('qmark', 'format'):
            def adapter(values):
                return tuple(param.eval(values) for param in params)
        elif paramstyle == 'numeric':
            def adapter(values):
                return tuple(param.eval(values) for param in params)
        elif paramstyle in ('named', 'pyformat'):
            def adapter(values):
                return {'p%d' % param.id: param.eval(values) for param in params}
        else: throw(NotImplementedError, paramstyle)
        builder.params = params
        builder.adapter = adapter
    def __call__(builder, ast):
        if isinstance(ast, basestring):
            throw(AstError, 'An SQL AST list was expected. Got string: %r' % ast)
        symbol = ast[0]
        if not isinstance(symbol, basestring):
            throw(AstError, 'Invalid node name in AST: %r' % ast)
        method = getattr(builder, symbol, None)
        if method is None: throw(AstError, 'Method not found: %s' % symbol)
        try:
            return method(*ast[1:])
        except TypeError:
            raise
##            traceback = sys.exc_info()[2]
##            if traceback.tb_next is None:
##                del traceback
##                throw(AstError, 'Invalid data for method %s: %r'
##                               % (symbol, ast[1:]))
##            else:
##                del traceback
##                raise
    def INSERT(builder, table_name, columns, values, returning=None):
        return [ 'INSERT INTO ', builder.quote_name(table_name), ' (',
                 join(', ', [builder.quote_name(column) for column in columns ]),
                 ') VALUES (', join(', ', [builder(value) for value in values]), ')' ]
    def DEFAULT(builder):
        return 'DEFAULT'
    def UPDATE(builder, table_name, pairs, where=None):
        return [ 'UPDATE ', builder.quote_name(table_name), '\nSET ',
                 join(', ', [ (builder.quote_name(name), ' = ', builder(param)) for name, param in pairs]),
                 where and [ '\n', builder(where) ] or [] ]
    def DELETE(builder, alias, from_ast, where=None):
        builder.indent += 1
        if alias is not None:
            assert isinstance(alias, basestring)
            if not where: return 'DELETE ', alias, ' ', builder(from_ast)
            return 'DELETE ', alias, ' ', builder(from_ast), builder(where)
        else:
            assert from_ast[0] == 'FROM' and len(from_ast) == 2 and from_ast[1][1] == 'TABLE'
            alias = from_ast[1][0]
            if alias is not None: builder.suppress_aliases = True
            if not where: return 'DELETE ', builder(from_ast)
            return 'DELETE ', builder(from_ast), builder(where)
    def _subquery(builder, *sections):
        builder.indent += 1
        if not builder.inner_join_syntax:
            sections = move_conditions_from_inner_join_to_where(sections)
        result = [ builder(s) for s in sections ]
        builder.indent -= 1
        return result
    def SELECT(builder, *sections):
        prev_suppress_aliases = builder.suppress_aliases
        builder.suppress_aliases = False
        try:
            result = builder._subquery(*sections)
            if builder.indent:
                indent = builder.indent_spaces * builder.indent
                return '(\n', result, indent + ')'
            return result
        finally:
            builder.suppress_aliases = prev_suppress_aliases
    def SELECT_FOR_UPDATE(builder, nowait, skip_locked, *sections):
        assert not builder.indent
        result = builder.SELECT(*sections)
        nowait = ' NOWAIT' if nowait else ''
        skip_locked = ' SKIP LOCKED' if skip_locked else ''
        return result, 'FOR UPDATE', nowait, skip_locked, '\n'
    def EXISTS(builder, *sections):
        result = builder._subquery(*sections)
        indent = builder.indent_spaces * builder.indent
        return 'EXISTS (\n', indent, 'SELECT 1\n', result, indent, ')'
    def NOT_EXISTS(builder, *sections):
        return 'NOT ', builder.EXISTS(*sections)
    @indentable
    def ALL(builder, *expr_list):
        exprs = [ builder(e) for e in expr_list ]
        return 'SELECT ', join(', ', exprs), '\n'
    @indentable
    def DISTINCT(builder, *expr_list):
        exprs = [ builder(e) for e in expr_list ]
        return 'SELECT DISTINCT ', join(', ', exprs), '\n'
    @indentable
    def AGGREGATES(builder, *expr_list):
        exprs = [ builder(e) for e in expr_list ]
        return 'SELECT ', join(', ', exprs), '\n'
    def AS(builder, expr, alias):
        return builder(expr), ' AS ', builder.quote_name(alias)
    def compound_name(builder, name_parts):
        return '.'.join(p and builder.quote_name(p) or '' for p in name_parts)
    def sql_join(builder, join_type, sources):
        indent = builder.indent_spaces * (builder.indent-1)
        indent2 = indent + builder.indent_spaces
        indent3 = indent2 + builder.indent_spaces
        result = [ indent, 'FROM ']
        for i, source in enumerate(sources):
            if len(source) == 3:
                alias, kind, x = source
                join_cond = None
            elif len(source) == 4:
                alias, kind, x, join_cond = source
            else: throw(AstError, 'Invalid source in FROM section: %r' % source)
            if i > 0:
                if join_cond is None: result.append(', ')
                else: result += [ '\n', indent, '  %s JOIN ' % join_type ]
            if builder.suppress_aliases: alias = None
            elif alias is not None: alias = builder.quote_name(alias)
            if kind == 'TABLE':
                if isinstance(x, basestring): result.append(builder.quote_name(x))
                else: result.append(builder.compound_name(x))
                if alias is not None: result += ' ', alias  # Oracle does not support 'AS' here
            elif kind == 'SELECT':
                if alias is None: throw(AstError, 'Subquery in FROM section must have an alias')
                result += builder.SELECT(*x), ' ', alias  # Oracle does not support 'AS' here
            else: throw(AstError, 'Invalid source kind in FROM section: %r' % kind)
            if join_cond is not None: result += [ '\n', indent2, 'ON ', builder(join_cond) ]
        result.append('\n')
        return result
    def FROM(builder, *sources):
        return builder.sql_join('INNER', sources)
    def INNER_JOIN(builder, *sources):
        builder.inner_join_syntax = True
        return builder.sql_join('INNER', sources)
    @indentable
    def LEFT_JOIN(builder, *sources):
        return builder.sql_join('LEFT', sources)
    def WHERE(builder, *conditions):
        if not conditions: return ''
        conditions = flat_conditions(conditions)
        indent = builder.indent_spaces * (builder.indent-1)
        result = [ indent, 'WHERE ' ]
        extend = result.extend
        extend((builder(conditions[0]), '\n'))
        for condition in conditions[1:]:
            extend((indent, '  AND ', builder(condition), '\n'))
        return result
    def HAVING(builder, *conditions):
        if not conditions: return ''
        conditions = flat_conditions(conditions)
        indent = builder.indent_spaces * (builder.indent-1)
        result = [ indent, 'HAVING ' ]
        extend = result.extend
        extend((builder(conditions[0]), '\n'))
        for condition in conditions[1:]:
            extend((indent, '  AND ', builder(condition), '\n'))
        return result
    @indentable
    def GROUP_BY(builder, *expr_list):
        exprs = [ builder(e) for e in expr_list ]
        return 'GROUP BY ', join(', ', exprs), '\n'
    @indentable
    def UNION(builder, kind, *sections):
        return 'UNION ', kind, '\n', builder.SELECT(*sections)
    @indentable
    def INTERSECT(builder, *sections):
        return 'INTERSECT\n', builder.SELECT(*sections)
    @indentable
    def EXCEPT(builder, *sections):
        return 'EXCEPT\n', builder.SELECT(*sections)
    @indentable
    def ORDER_BY(builder, *order_list):
        result = [ 'ORDER BY ' ]
        result.extend(join(', ', [ builder(expr) for expr in order_list ]))
        result.append('\n')
        return result
    def DESC(builder, expr):
        return builder(expr), ' DESC'
    @indentable
    def LIMIT(builder, limit, offset=None):
        if limit is None:
            limit = 'null'
        else:
            assert isinstance(limit, int_types)
        assert offset is None or isinstance(offset, int)
        if offset:
            return 'LIMIT %s OFFSET %d\n' % (limit, offset)
        else:
            return 'LIMIT %s\n' % limit
    def COLUMN(builder, table_alias, col_name):
        if builder.suppress_aliases or not table_alias:
            return [ '%s' % builder.quote_name(col_name) ]
        return [ '%s.%s' % (builder.quote_name(table_alias), builder.quote_name(col_name)) ]
    def PARAM(builder, paramkey, converter=None, optimistic=False):
        return builder.make_param(builder.param_class, paramkey, converter, optimistic)
    def make_param(builder, param_class, paramkey, *args):
        keys = builder.keys
        param = keys.get(paramkey)
        if param is None:
            param = param_class(builder.paramstyle, paramkey, *args)
            keys[paramkey] = param
        return param
    def make_composite_param(builder, paramkey, items, func):
        return builder.make_param(builder.composite_param_class, paramkey, items, func)
    def STAR(builder, table_alias):
        return builder.quote_name(table_alias), '.*'
    def ROW(builder, *items):
        return '(', join(', ', imap(builder, items)), ')'
    def VALUE(builder, value):
        return builder.value_class(builder.paramstyle, value)
    def AND(builder, *cond_list):
        cond_list = [ builder(condition) for condition in cond_list ]
        return join(' AND ', cond_list)
    def OR(builder, *cond_list):
        cond_list = [ builder(condition) for condition in cond_list ]
        return '(', join(' OR ', cond_list), ')'
    def NOT(builder, condition):
        return 'NOT (', builder(condition), ')'
    def POW(builder, expr1, expr2):
        return 'power(', builder(expr1), ', ', builder(expr2), ')'

    EQ  = make_binary_op(' = ')
    NE  = make_binary_op(' <> ')
    LT  = make_binary_op(' < ')
    LE  = make_binary_op(' <= ')
    GT  = make_binary_op(' > ')
    GE  = make_binary_op(' >= ')
    ADD = make_binary_op(' + ', True)
    SUB = make_binary_op(' - ', True)
    MUL = make_binary_op(' * ', True)
    DIV = make_binary_op(' / ', True)
    FLOORDIV = make_binary_op(' / ', True)

    def MOD(builder, a, b):
        symbol = ' %% ' if builder.paramstyle in ('format', 'pyformat') else ' % '
        return '(', builder(a), symbol, builder(b), ')'
    def FLOAT_EQ(builder, a, b):
        a, b = builder(a), builder(b)
        return 'abs(', a, ' - ', b, ') / coalesce(nullif(greatest(abs(', a, '), abs(', b, ')), 0), 1) <= 1e-14'
    def FLOAT_NE(builder, a, b):
        a, b = builder(a), builder(b)
        return 'abs(', a, ' - ', b, ') / coalesce(nullif(greatest(abs(', a, '), abs(', b, ')), 0), 1) > 1e-14'
    def CONCAT(builder, *args):
        return '(',  join(' || ', imap(builder, args)), ')'
    def NEG(builder, expr):
        return '-(', builder(expr), ')'
    def IS_NULL(builder, expr):
        return builder(expr), ' IS NULL'
    def IS_NOT_NULL(builder, expr):
        return builder(expr), ' IS NOT NULL'
    def LIKE(builder, expr, template, escape=None):
        result = builder(expr), ' LIKE ', builder(template)
        if escape: result = result + (' ESCAPE ', builder(escape))
        return result
    def NOT_LIKE(builder, expr, template, escape=None):
        result = builder(expr), ' NOT LIKE ', builder(template)
        if escape: result = result + (' ESCAPE ', builder(escape))
        return result
    def BETWEEN(builder, expr1, expr2, expr3):
        return builder(expr1), ' BETWEEN ', builder(expr2), ' AND ', builder(expr3)
    def NOT_BETWEEN(builder, expr1, expr2, expr3):
        return builder(expr1), ' NOT BETWEEN ', builder(expr2), ' AND ', builder(expr3)
    def IN(builder, expr1, x):
        if not x: return '0 = 1'
        if len(x) >= 1 and x[0] == 'SELECT':
            return builder(expr1), ' IN ', builder(x)
        expr_list = [ builder(expr) for expr in x ]
        return builder(expr1), ' IN (', join(', ', expr_list), ')'
    def NOT_IN(builder, expr1, x):
        if not x: return '1 = 1'
        if len(x) >= 1 and x[0] == 'SELECT':
            return builder(expr1), ' NOT IN ', builder(x)
        expr_list = [ builder(expr) for expr in x ]
        return builder(expr1), ' NOT IN (', join(', ', expr_list), ')'
    def COUNT(builder, distinct, *expr_list):
        assert distinct in (None, True, False)
        if not distinct:
            if not expr_list: return ['COUNT(*)']
            if builder.dialect == 'PostgreSQL':
                return 'COUNT(', builder.ROW(*expr_list), ')'
            else:
                return 'COUNT(', join(', ', imap(builder, expr_list)), ')'
        if not expr_list: throw(AstError, 'COUNT(DISTINCT) without argument')
        if len(expr_list) == 1:
            return 'COUNT(DISTINCT ', builder(expr_list[0]), ')'

        if builder.dialect == 'PostgreSQL':
            return 'COUNT(DISTINCT ', builder.ROW(*expr_list), ')'
        elif builder.dialect == 'MySQL':
            return 'COUNT(DISTINCT ', join(', ', imap(builder, expr_list)), ')'
        # Oracle and SQLite queries translated to completely different subquery syntax
        else: throw(NotImplementedError)  # This line must not be executed
    def SUM(builder, distinct, expr):
        assert distinct in (None, True, False)
        return distinct and 'coalesce(SUM(DISTINCT ' or 'coalesce(SUM(', builder(expr), '), 0)'
    def AVG(builder, distinct, expr):
        assert distinct in (None, True, False)
        return distinct and 'AVG(DISTINCT ' or 'AVG(', builder(expr), ')'
    def GROUP_CONCAT(builder, distinct, expr, sep=None):
        assert distinct in (None, True, False)
        result = distinct and 'GROUP_CONCAT(DISTINCT ' or 'GROUP_CONCAT(', builder(expr)
        if sep is not None:
            if builder.provider.dialect == 'MySQL':
                result = result, ' SEPARATOR ', builder(sep)
            else:
                result = result, ', ', builder(sep)
        return result, ')'
    UPPER = make_unary_func('upper')
    LOWER = make_unary_func('lower')
    LENGTH = make_unary_func('length')
    ABS = make_unary_func('abs')
    def COALESCE(builder, *args):
        if len(args) < 2: assert False  # pragma: no cover
        return 'coalesce(', join(', ', imap(builder, args)), ')'
    def MIN(builder, distinct, *args):
        assert not distinct, distinct
        if len(args) == 0: assert False  # pragma: no cover
        elif len(args) == 1: fname = 'MIN'
        else: fname = builder.least_func_name
        return fname, '(',  join(', ', imap(builder, args)), ')'
    def MAX(builder, distinct, *args):
        assert not distinct, distinct
        if len(args) == 0: assert False  # pragma: no cover
        elif len(args) == 1: fname = 'MAX'
        else: fname = builder.greatest_func_name
        return fname, '(',  join(', ', imap(builder, args)), ')'
    def SUBSTR(builder, expr, start, len=None):
        if len is None: return 'substr(', builder(expr), ', ', builder(start), ')'
        return 'substr(', builder(expr), ', ', builder(start), ', ', builder(len), ')'
    def STRING_SLICE(builder, expr, start, stop):
        if start is None:
            start = [ 'VALUE', 0 ]

        if start[0] == 'VALUE':
            start_value = start[1]
            if builder.dialect == 'PostgreSQL' and start_value < 0:
                index_sql = [ 'LENGTH', expr ]
                if start_value < -1:
                    index_sql = [ 'SUB', index_sql, [ 'VALUE', -(start_value + 1) ] ]
            else:
                if start_value >= 0: start_value += 1
                index_sql = [ 'VALUE', start_value ]
        else:
            inner_sql = start
            then = [ 'ADD', inner_sql, [ 'VALUE', 1 ] ]
            else_ = [ 'ADD', [ 'LENGTH', expr ], then ] if builder.dialect == 'PostgreSQL' else inner_sql
            index_sql = [ 'IF', [ 'GE', inner_sql, [ 'VALUE', 0 ] ], then, else_ ]

        if stop is None:
            len_sql = None
        elif stop[0] == 'VALUE':
            stop_value = stop[1]
            if start[0] == 'VALUE':
                start_value = start[1]
                if start_value >= 0 and stop_value >= 0:
                    len_sql = [ 'VALUE', stop_value - start_value ]
                elif start_value < 0 and stop_value < 0:
                    len_sql = [ 'VALUE', stop_value - start_value ]
                elif start_value >= 0 and stop_value < 0:
                    len_sql = [ 'SUB', [ 'LENGTH', expr ], [ 'VALUE', start_value - stop_value ]]
                    len_sql = [ 'MAX', False, len_sql, [ 'VALUE', 0 ] ]
                elif start_value < 0 and stop_value >= 0:
                    len_sql = [ 'SUB', [ 'VALUE', stop_value + 1 ], index_sql ]
                    len_sql = [ 'MAX', False, len_sql, [ 'VALUE', 0 ] ]
                else:
                    assert False  # pragma: nocover1
            else:
                start_sql = [ 'COALESCE', start, [ 'VALUE', 0 ] ]
                if stop_value >= 0:
                    start_positive = [ 'SUB', stop, start_sql ]
                    start_negative = [ 'SUB', [ 'VALUE', stop_value + 1 ], index_sql ]
                else:
                    start_positive = [ 'SUB', [ 'LENGTH', expr ], [ 'ADD', start_sql, [ 'VALUE', -stop_value ] ] ]
                    start_negative = [ 'SUB', stop, start_sql]
                len_sql = [ 'IF', [ 'GE', start_sql, [ 'VALUE', 0 ] ], start_positive, start_negative ]
                len_sql = [ 'MAX', False, len_sql, [ 'VALUE', 0 ] ]
        else:
            stop_sql = [ 'COALESCE', stop, [ 'VALUE', -1 ] ]
            if start[0] == 'VALUE':
                start_value = start[1]
                start_sql = [ 'VALUE', start_value ]
                if start_value >= 0:
                    stop_positive = [ 'SUB', stop_sql, start_sql ]
                    stop_negative = [ 'SUB', [ 'LENGTH', expr ], [ 'SUB', start_sql, stop_sql ] ]
                else:
                    stop_positive = [ 'SUB', [ 'ADD', stop_sql, [ 'VALUE', 1 ] ], index_sql ]
                    stop_negative = [ 'SUB', stop_sql, start_sql]
                len_sql = [ 'IF', [ 'GE', stop_sql, [ 'VALUE', 0 ] ], stop_positive, stop_negative ]
                len_sql = [ 'MAX', False, len_sql, [ 'VALUE', 0 ] ]
            else:
                start_sql = [ 'COALESCE', start, [ 'VALUE', 0 ] ]
                both_positive = [ 'SUB', stop_sql, start_sql ]
                both_negative = both_positive
                start_positive = [ 'SUB', [ 'LENGTH', expr ], [ 'SUB', start_sql, stop_sql ] ]
                stop_positive = [ 'SUB', [ 'ADD', stop_sql, [ 'VALUE', 1 ] ], index_sql ]
                len_sql = [ 'CASE', None, [
                    (
                        [ 'AND', [ 'GE', start_sql, [ 'VALUE', 0 ] ], [ 'GE', stop_sql, [ 'VALUE', 0 ] ] ],
                        both_positive
                    ),
                    (
                        [ 'AND', [ 'LT', start_sql, [ 'VALUE', 0 ] ], [ 'LT', stop_sql, [ 'VALUE', 0 ] ] ],
                        both_negative
                    ),
                    (
                        [ 'AND', [ 'GE', start_sql, [ 'VALUE', 0 ] ], [ 'LT', stop_sql, [ 'VALUE', 0 ] ] ],
                        start_positive
                    ),
                    (
                        [ 'AND', [ 'LT', start_sql, [ 'VALUE', 0 ] ], [ 'GE', stop_sql, [ 'VALUE', 0 ] ] ],
                        stop_positive
                    ),
                ]]
                len_sql = [ 'MAX', False, len_sql, [ 'VALUE', 0 ] ]
        sql = [ 'SUBSTR', expr, index_sql, len_sql ]
        return builder(sql)
    def CASE(builder, expr, cases, default=None):
        if expr is None and default is not None and default[0] == 'CASE' and default[1] is None:
            cases2, default2 = default[2:]
            return builder.CASE(None, tuple(cases) + tuple(cases2), default2)
        result = [ 'case' ]
        if expr is not None:
            result.append(' ')
            result.extend(builder(expr))
        for condition, expr in cases:
            result.extend((' when ', builder(condition), ' then ', builder(expr)))
        if default is not None:
            result.extend((' else ', builder(default)))
        result.append(' end')
        return result
    def IF(builder, cond, then, else_):
        return builder.CASE(None, [(cond, then)], else_)
    def TRIM(builder, expr, chars=None):
        if chars is None: return 'trim(', builder(expr), ')'
        return 'trim(', builder(expr), ', ', builder(chars), ')'
    def LTRIM(builder, expr, chars=None):
        if chars is None: return 'ltrim(', builder(expr), ')'
        return 'ltrim(', builder(expr), ', ', builder(chars), ')'
    def RTRIM(builder, expr, chars=None):
        if chars is None: return 'rtrim(', builder(expr), ')'
        return 'rtrim(', builder(expr), ', ', builder(chars), ')'
    def REPLACE(builder, str, from_, to):
        return 'replace(', builder(str), ', ', builder(from_), ', ', builder(to), ')'
    def TO_INT(builder, expr):
        return 'CAST(', builder(expr), ' AS integer)'
    def TO_STR(builder, expr):
        return 'CAST(', builder(expr), ' AS text)'
    def TO_REAL(builder, expr):
        return 'CAST(', builder(expr), ' AS real)'
    def TODAY(builder):
        return 'CURRENT_DATE'
    def NOW(builder):
        return 'CURRENT_TIMESTAMP'
    def DATE(builder, expr):
        return 'DATE(', builder(expr) ,')'
    def YEAR(builder, expr):
        return 'EXTRACT(YEAR FROM ', builder(expr), ')'
    def MONTH(builder, expr):
        return 'EXTRACT(MONTH FROM ', builder(expr), ')'
    def DAY(builder, expr):
        return 'EXTRACT(DAY FROM ', builder(expr), ')'
    def HOUR(builder, expr):
        return 'EXTRACT(HOUR FROM ', builder(expr), ')'
    def MINUTE(builder, expr):
        return 'EXTRACT(MINUTE FROM ', builder(expr), ')'
    def SECOND(builder, expr):
        return 'EXTRACT(SECOND FROM ', builder(expr), ')'
    def RANDOM(builder):
        return 'RAND()'
    def RAWSQL(builder, sql):
        if isinstance(sql, basestring): return sql
        return [ x if isinstance(x, basestring) else builder(x) for x in sql ]
    def build_json_path(builder, path):
        empty_slice = slice(None, None, None)
        has_params = False
        has_wildcards = False
        items = [ builder(element) for element in path ]
        for item in items:
            if isinstance(item, Param):
                has_params = True
            elif isinstance(item, Value):
                value = item.value
                if value is Ellipsis or value == empty_slice: has_wildcards = True
                else: assert isinstance(value, (int, basestring)), value
            else: assert False, item
        if has_params:
            paramkey = tuple(item.paramkey if isinstance(item, Param) else
                             None if type(item.value) is slice else item.value
                             for item in items)
            path_sql = builder.make_composite_param(paramkey, items, builder.eval_json_path)
        else:
            result_value = builder.eval_json_path(item.value for item in items)
            path_sql = builder.value_class(builder.paramstyle, result_value)
        return path_sql, has_params, has_wildcards
    @classmethod
    def eval_json_path(cls, values):
        result = ['$']
        append = result.append
        empty_slice = slice(None, None, None)
        for value in values:
            if isinstance(value, int): append('[%d]' % value)
            elif isinstance(value, basestring):
                append('.' + value if is_ident(value) else '."%s"' % value.replace('"', '\\"'))
            elif value is Ellipsis: append('.*')
            elif value == empty_slice: append('[*]')
            else: assert False, value
        return ''.join(result)
    def JSON_QUERY(builder, expr, path):
        throw(NotImplementedError)
    def JSON_VALUE(builder, expr, path, type):
        throw(NotImplementedError)
    def JSON_NONZERO(builder, expr):
        throw(NotImplementedError)
    def JSON_CONCAT(builder, left, right):
        throw(NotImplementedError)
    def JSON_CONTAINS(builder, expr, path, key):
        throw(NotImplementedError)
    def JSON_ARRAY_LENGTH(builder, value):
        throw(NotImplementedError)
    def JSON_PARAM(builder, expr):
        return builder(expr)
    def ARRAY_INDEX(builder, col, index):
        throw(NotImplementedError)
    def ARRAY_CONTAINS(builder, key, not_in, col):
        throw(NotImplementedError)
    def ARRAY_SUBSET(builder, array1, not_in, array2):
        throw(NotImplementedError)
    def ARRAY_LENGTH(builder, array):
        throw(NotImplementedError)
    def ARRAY_SLICE(builder, array, start, stop):
        throw(NotImplementedError)
    def MAKE_ARRAY(builder, *items):
        throw(NotImplementedError)
