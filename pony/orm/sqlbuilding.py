from operator import attrgetter
from decimal import Decimal
from datetime import date, datetime
from binascii import hexlify

from pony import options
from pony.utils import datetime2timestamp, throw

class AstError(Exception): pass

class Param(object):
    __slots__ = 'style', 'id', 'key', 'py2sql'
    def __init__(param, paramstyle, id, key, converter=None):
        param.style = paramstyle
        param.id = id
        param.key = key
        param.py2sql = converter and converter.py2sql or (lambda val: val)
    def __unicode__(param):
        paramstyle = param.style
        if paramstyle == 'qmark': return u'?'
        elif paramstyle == 'format': return u'%s'
        elif paramstyle == 'numeric': return u':%d' % param.id
        elif paramstyle == 'named': return u':p%d' % param.id
        elif paramstyle == 'pyformat': return u'%%(p%d)s' % param.id
        else: throw(NotImplementedError)
    def __repr__(param):
        return '%s(%r)' % (param.__class__.__name__, param.key)

class Value(object):
    __slots__ = 'paramstyle', 'value'
    def __init__(self, paramstyle, value):
        self.paramstyle = paramstyle
        self.value = value
    def __unicode__(self):
        value = self.value
        if value is None: return 'null'
        if isinstance(value, bool): return value and '1' or '0'
        if isinstance(value, (int, long, float, Decimal)): return str(value)
        if isinstance(value, basestring): return self.quote_str(value)
        if isinstance(value, datetime): return self.quote_str(datetime2timestamp(value))
        if isinstance(value, date): return self.quote_str(str(value))
        if isinstance(value, buffer): return "X'%s'" % hexlify(value)
        assert False, value
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
            try: stack_extend(reversed(x))
            except TypeError: result_append(x)
    return result

def flat_conditions(conditions):
    result = []
    for condition in conditions:
        if condition[0] == 'AND':
            result.extend(flat_conditions(condition[1:]))
        else: result.append(condition)
    return result

def join(delimiter, items):
    items = iter(items)
    try: result = [ items.next() ]
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

def convert(values, params):
    for param in params:
        key = param.key
        if type(key) is tuple:
            key, i = key
            if type(key) is tuple:
                key, j = key
                tup = values[key]
                obj = tup[j]
                value = obj._get_raw_pkval_()[i]
            else:
                obj_or_tuple = values[key]
                if type(obj_or_tuple) is tuple: value = obj_or_tuple[i]
                else: value = obj_or_tuple._get_raw_pkval_()[i]
        else: value = values[key]
        if value is not None: value = param.py2sql(value)
        yield value

class SQLBuilder(object):
    dialect = None
    make_param = Param
    make_value = Value
    indent_spaces = " " * 4
    def __init__(builder, provider, ast):
        builder.provider = provider
        builder.quote_name = provider.quote_name
        builder.paramstyle = paramstyle = provider.paramstyle
        builder.ast = ast
        builder.indent = 0
        builder.keys = {}
        builder.inner_join_syntax = options.INNER_JOIN_SYNTAX
        builder.result = flat(builder(ast))
        builder.sql = u''.join(map(unicode, builder.result)).rstrip('\n')
        if paramstyle in ('qmark', 'format'):
            params = tuple(x for x in builder.result if isinstance(x, Param))
            def adapter(values):
                return tuple(convert(values, params))
        elif paramstyle == 'numeric':
            params = tuple(param for param in sorted(builder.keys.itervalues(), key=attrgetter('id')))
            def adapter(values):
                return tuple(convert(values, params))
        elif paramstyle in ('named', 'pyformat'):
            params = tuple(param for param in sorted(builder.keys.itervalues(), key=attrgetter('id')))
            def adapter(values):
                return dict(('p%d' % param.id, value) for param, value in zip(params, convert(values, params)))
        else: throw(NotImplementedError, paramstyle)
        builder.params = params
        builder.layout = tuple(param.key for param in params)
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
    def DELETE(builder, table_name, where=None):
        result = [ 'DELETE FROM ', builder.quote_name(table_name) ]
        if where: result += [ '\n', builder(where) ]
        return result
    def subquery(builder, *sections):
        builder.indent += 1
        if not builder.inner_join_syntax:
            sections = move_conditions_from_inner_join_to_where(sections)
        result = [ builder(s) for s in sections ]
        builder.indent -= 1
        return result
    def SELECT(builder, *sections):
        result = builder.subquery(*sections)
        if builder.indent:
            indent = builder.indent_spaces * builder.indent
            return '(\n', result, indent + ')'
        return result
    def SELECT_FOR_UPDATE(builder, nowait, *sections):
        assert not builder.indent
        result = builder.SELECT(*sections)
        return result, 'FOR UPDATE NOWAIT\n' if nowait else 'FOR UPDATE\n'
    def EXISTS(builder, *sections):
        result = builder.subquery(*sections)
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
            if alias is not None: alias = builder.quote_name(alias)
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
        if not offset: return 'LIMIT ', builder(limit), '\n'
        else: return 'LIMIT ', builder(limit), ' OFFSET ', builder(offset), '\n'
    def COLUMN(builder, table_alias, col_name):
        if table_alias: return [ '%s.%s' % (builder.quote_name(table_alias), builder.quote_name(col_name)) ]
        else: return [ '%s' % (builder.quote_name(col_name)) ]
    def PARAM(builder, key, converter=None):
        keys = builder.keys
        param = keys.get(key)
        if param is None:
            param = Param(builder.paramstyle, len(keys) + 1, key, converter)
            keys[key] = param
        return [ param ]
    def ROW(builder, *items):
        return '(', join(', ', map(builder, items)), ')'
    def VALUE(builder, value):
        return [ builder.make_value(builder.paramstyle, value) ]
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

    def CONCAT(builder, *args):
        return '(',  join(' || ', map(builder, args)), ')'
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
        if not x: throw(AstError, 'Empty IN clause')
        if len(x) >= 1 and x[0] == 'SELECT':
            return builder(expr1), ' IN ', builder(x)
        expr_list = [ builder(expr) for expr in x ]
        return builder(expr1), ' IN (', join(', ', expr_list), ')'
    def NOT_IN(builder, expr1, x):
        if not x: throw(AstError, 'Empty IN clause')
        if len(x) >= 1 and x[0] == 'SELECT':
            return builder(expr1), ' NOT IN ', builder(x)
        expr_list = [ builder(expr) for expr in x ]
        return builder(expr1), ' NOT IN (', join(', ', expr_list), ')'
    def COUNT(builder, kind, *expr_list):
        if kind == 'ALL':
            if not expr_list: return ['COUNT(*)']
            return 'COUNT(', join(', ', map(builder, expr_list)), ')'
        elif kind == 'DISTINCT':
            if not expr_list: throw(AstError, 'COUNT(DISTINCT) without argument')
            if len(expr_list) == 1: return 'COUNT(DISTINCT ', builder(expr_list[0]), ')'
            if builder.dialect == 'PostgreSQL':
                return 'COUNT(DISTINCT ', builder.ROW(*expr_list), ')'
            elif builder.dialect == 'MySQL':
                return 'COUNT(DISTINCT ', join(', ', map(builder, expr_list)), ')'
            # Oracle and SQLite queries translated to completely different subquery syntax
            else: throw(NotImplementedError)  # This line must not be executed
        throw(AstError, 'Invalid COUNT kind (must be ALL or DISTINCT)')
    def SUM(builder, expr, distinct=False):
        return distinct and 'coalesce(SUM(DISTINCT ' or 'coalesce(SUM(', builder(expr), '), 0)'
    def AVG(builder, expr, distinct=False):
        return distinct and 'AVG(DISTINCT ' or 'AVG(', builder(expr), ')'
    UPPER = make_unary_func('upper')
    LOWER = make_unary_func('lower')
    LENGTH = make_unary_func('length')
    ABS = make_unary_func('abs')
    def COALESCE(builder, *args):
        if len(args) < 2: assert False
        return 'coalesce(', join(', ', map(builder, args)), ')'
    def MIN(builder, *args):
        if len(args) == 0: assert False
        elif len(args) == 1: fname = 'MIN'
        else: fname = 'least'
        return fname, '(',  join(', ', map(builder, args)), ')'
    def MAX(builder, *args):
        if len(args) == 0: assert False
        elif len(args) == 1: fname = 'MAX'
        else: fname = 'greatest'
        return fname, '(',  join(', ', map(builder, args)), ')'
    def SUBSTR(builder, expr, start, len=None):
        if len is None: return 'substr(', builder(expr), ', ', builder(start), ')'
        return 'substr(', builder(expr), ', ', builder(start), ', ', builder(len), ')'
    def CASE(builder, expr, cases, default=None):
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
    def TRIM(builder, expr, chars=None):
        if chars is None: return 'trim(', builder(expr), ')'
        return 'trim(', builder(expr), ', ', builder(chars), ')'
    def LTRIM(builder, expr, chars=None):
        if chars is None: return 'ltrim(', builder(expr), ')'
        return 'ltrim(', builder(expr), ', ', builder(chars), ')'
    def RTRIM(builder, expr, chars=None):
        if chars is None: return 'rtrim(', builder(expr), ')'
        return 'rtrim(', builder(expr), ', ', builder(chars), ')'
    def TO_INT(builder, expr):
        return 'CAST(', builder(expr), ' AS integer)'
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
