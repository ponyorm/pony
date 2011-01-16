import sys, threading
from operator import attrgetter

from pony.sqlsymbols import *

def quote_name(name, quote_char='"'):
    if isinstance(name, basestring):
        name = name.replace(quote_char, quote_char+quote_char)
        return quote_char + name + quote_char
    return '.'.join(quote_name(item, quote_char) for item in name)

class AstError(Exception): pass

class Param(object):
    __slots__ = 'style', 'id', 'key',
    def __init__(param, paramstyle, id, key):
        param.style = paramstyle
        param.id = id
        param.key = key
    def __unicode__(param):
        paramstyle = param.style
        if paramstyle == 'qmark': return u'?'
        elif paramstyle == 'format': return u'%s'
        elif paramstyle == 'numeric': return u':%d' % param.id
        elif paramstyle == 'named': return u':p%d' % param.id
        elif paramstyle == 'pyformat': return u'%%(p%d)s' % param.id
        else: raise NotImplementedError
    def __repr__(param):
        return '%s(%r)' % (param.__class__.__name__, param.key)

class Value(object):
    __slots__ = 'value',
    def __init__(self, value):
        self.value = value
    def __unicode__(self):
        value = self.value
        if value is None: return 'null'
        if isinstance(value, (int, long)): return str(value)
        if isinstance(value, basestring): return self.quote_str(value)
        assert False
    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__, self.value)
    def quote_str(self, s):
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

def join(delimiter, items):
    items = iter(items)
    try: result = [ items.next() ]
    except StopIteration: return []
    for item in items:
        result.append(delimiter)
        result.append(item)
    return result

def make_binary_op(symbol):
    def binary_op(builder, expr1, expr2):
        return '(', builder(expr1), symbol, builder(expr2), ')'
    return binary_op

def make_unary_func(symbol):
    def unary_func(builder, expr):
        return '%s(' % symbol, builder(expr), ')'
    return unary_func

def indentable(method):
    def new_method(builder, *args, **keyargs):
        result = method(builder, *args, **keyargs)
        if builder.indent <= 1: return result
        return builder.indent_spaces * (builder.indent-1), result
    new_method.__name__ = method.__name__
    return new_method

class SQLBuilder(object):
    make_param = Param
    value = Value
    indent_spaces = " " * 4
    def __init__(builder, ast, paramstyle='qmark', quote_char='"'):
        builder.indent = 0
        builder.ast = ast
        builder.paramstyle = paramstyle
        builder.quote_char = quote_char
        builder.keys = {}
        builder.result = flat(builder(ast))
        builder.sql = u''.join(map(unicode, builder.result))
        if paramstyle in ('qmark', 'format'):
            layout = tuple(x.key for x in builder.result if isinstance(x, Param))
            def adapter(values):
                return tuple(map(values.__getitem__, layout))
        elif paramstyle in ('named', 'pyformat'):
            layout = tuple(param.key for param in sorted(builder.keys.itervalues(), key=attrgetter('id')))
            def adapter(values):
                return dict(('p%d'%(i+1), values[key]) for i, key in enumerate(layout))
        elif paramstyle == 'numeric':
            layout = tuple(param.key for param in sorted(builder.keys.itervalues(), key=attrgetter('id')))
            def adapter(values):
                return tuple(map(values.__getitem__, layout))
        else: raise NotImplementedError
        builder.layout = layout
        builder.adapter = adapter 
    def __call__(builder, ast):
        if isinstance(ast, basestring):
            raise AstError('An SQL AST list was expected. Got string: %r' % ast)
        symbol = ast[0]
        if not isinstance(symbol, basestring):
            raise AstError('Invalid node name in AST: %r' % ast)
        method = getattr(builder, symbol, None)
        if method is None: raise AstError('Method not found: %s' % symbol)
        try:
            return method(*ast[1:])
        except TypeError:
            raise
##            traceback = sys.exc_info()[2]
##            if traceback.tb_next is None:
##                del traceback
##                raise AstError('Invalid data for method %s: %r'
##                               % (symbol, ast[1:]))
##            else:
##                del traceback
##                raise
    def quote_name(builder, name):
        return quote_name(name, builder.quote_char)
    def INSERT(builder, table_name, columns, values):
        return [ 'INSERT INTO ', builder.quote_name(table_name), ' (',
                 join(', ', [builder.quote_name(column) for column in columns ]),
                 ') VALUES (', join(', ', [builder(value) for value in values]), ')' ]
    def UPDATE(builder, table_name, pairs, where=None):
        return [ 'UPDATE ', builder.quote_name(table_name), '\nSET ',
                 join(', ', [ (builder.quote_name(name), '=', builder(param)) for name, param in pairs]),
                 where and [ '\n', builder(where) ] or [] ]
    def DELETE(builder, table_name, where=None):
        result = [ 'DELETE FROM ', builder.quote_name(table_name) ]
        if where: result += [ '\n', builder(where) ]
        return result
    def SELECT(builder, *sections):
        builder.indent += 1
        result = [ builder(s) for s in sections ]
        builder.indent -= 1
        if builder.indent : result = ['(\n', result, ')']
        return result
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
    def compound_name(builder, name_parts):
        return '.'.join(p and builder.quote_name(p) or '' for p in name_parts)
    def sql_join(builder, join_type, sources):
        result = ['FROM ']
        for i, source in enumerate(sources):
            if len(source) == 3:   alias, kind, x = source; join_cond = None
            elif len(source) == 4: alias, kind, x, join_cond = source
            else: raise AstError('Invalid source in FROM section: %r' % source)
            if alias is not None: alias = builder.quote_name(alias)
            if i > 0:
                if join_cond is None: result.append(', ')
                else: result.append(' %s JOIN ' % join_type)
            if kind == TABLE:
                if isinstance(x, basestring): result.append(builder.quote_name(x))
                else: result.append(builder.compound_name(x))
                if alias is not None: result += ' AS ', alias
            elif kind == SELECT:
                if alias is None: raise AstError('Subquery in FROM section must have an alias')
                result += '(', builder.SELECT(*x), ') AS ', alias
            else: raise AstError('Invalid source kind in FROM section: %s',kind)
            if join_cond is not None: result += ' ON ', builder(join_cond)
        result.append('\n')
        return result
    @indentable
    def FROM(builder, *sources):
        return builder.sql_join('INNER', sources)
    @indentable
    def LEFT_JOIN(builder, *sources):
        return builder.sql_join('LEFT', sources)
    def WHERE(builder, condition):
        indent = builder.indent_spaces * (builder.indent-1)
        result = [ indent, 'WHERE ' ]
        if condition[0] != AND:
            result.extend((builder(condition), '\n'))
        else:
            result.extend((builder(condition[1]), '\n'))
            for item in condition[2:]:
                result.extend((indent, '  AND ', builder(item), '\n'))
        return result
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
        result = ['ORDER BY ']
        for i, (expr, dir) in enumerate(order_list):
            if i > 0: result.append(', ')
            result += builder(expr), ' ', dir
        result.append('\n')
        return result
    @indentable
    def LIMIT(builder, limit, offset=None):
        if not offset: return 'LIMIT ', builder(limit), '\n'
        else: return 'LIMIT ', builder(limit), ' OFFSET ', builder(offset), '\n'
    def COLUMN(builder, table_alias, col_name):
        if table_alias: return [ '%s.%s' % (builder.quote_name(table_alias), builder.quote_name(col_name)) ]
        else: return [ '%s' % (builder.quote_name(col_name)) ]
    def PARAM(builder, key):
        keys = builder.keys
        param = keys.get(key)
        if param is None:
            param = Param(builder.paramstyle, len(keys) + 1, key)
            keys[key] = param
        return [ param ]
    def VALUE(builder, value):
        return [ builder.value(value) ]
    def AND(builder, *cond_list):
        cond_list = [ builder(condition) for condition in cond_list ]
        return '(', join(' AND ', cond_list), ')'
    def OR(builder, *cond_list):
        cond_list = [ builder(condition) for condition in cond_list ]
        return '(', join(' OR ', cond_list), ')'
    def NOT(builder, condition):
        return 'NOT (', builder(condition), ')'
    
    EQ  = make_binary_op(' = ')
    NE  = make_binary_op(' <> ')
    LT  = make_binary_op(' < ')
    LE  = make_binary_op(' <= ')
    GT  = make_binary_op(' > ')
    GE  = make_binary_op(' >= ')
    ADD = make_binary_op(' + ')
    SUB = make_binary_op(' - ')
    MUL = make_binary_op(' * ')
    DIV = make_binary_op(' / ')
    POW = make_binary_op(' ** ')

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
        result = builder(expr), ' NOT LIKE ', builder.template
        if escape: result = result + (' ESCAPE ', builder(escape))
        return result
    def BETWEEN(builder, expr1, expr2, expr3):
        return builder(expr1), ' BETWEEN ', builder(expr2), ' AND ', builder(expr3)
    def NOT_BETWEEN(builder, expr1, expr2, expr3):
        return builder(expr1), ' NOT BETWEEN ', builder(expr2), ' AND ', builder(expr3)
    def IN(builder, expr1, x):
        if not x: raise AstError('Empty IN clause')
        if len(x) >= 1 and x[0] == SELECT:
            return builder(expr1), ' IN ', builder(x)
        expr_list = [ builder(expr) for expr in x ]
        return builder(expr1), ' IN (', join(', ', expr_list), ')'
    def NOT_IN(builder, expr1, x):
        if not x: raise AstError('Empty IN clause')
        if len(x) >= 1 and x[0] == SELECT:
            return builder(expr1), ' NOT IN ', builder(x)
        expr_list = [ builder(expr) for expr in x ]
        return builder(expr1), ' NOT IN (', join(', ', expr_list), ')'
    def EXISTS(builder, *sections):
        return 'EXISTS (\nSELECT 1 ', builder.SELECT(*sections), ')'
    def NOT_EXISTS(builder, *sections):
        return 'NOT EXISTS (\nSELECT * ', builder.SELECT(*sections), ')'
    def COUNT(builder, kind, expr=None):
        if kind == ALL:
            if expr is None: return ['COUNT(*)']
            return 'COUNT(', builder(expr), ')'
        elif kind == DISTINCT:
            if expr is None: raise AstError('COUNT(DISTINCT) without argument')
            return 'COUNT(DISTINCT ', builder(expr), ')'
        raise AstError('Invalid COUNT kind (must be ALL or DISTINCT)')
    SUM = make_unary_func('SUM')
    AVG = make_unary_func('AVG')
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
        else: fname = 'min'
        return fname, '(',  join(', ', map(builder, args)), ')'
    def MAX(builder, *args):
        if len(args) == 0: assert False
        elif len(args) == 1: fname = 'MAX'
        else: fname = 'max'
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
