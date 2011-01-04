import __builtin__
from compiler import ast
from types import NoneType
from operator import attrgetter
from itertools import imap

from pony import orm
from pony.decompiling import decompile
from pony.templating import Html, StrHtml
from pony.dbapiprovider import SQLBuilder
from pony.sqlsymbols import *

MAX_ALIAS_LENGTH = 30

class TranslationError(Exception): pass

python_ast_cache = {}
sql_cache = {}

def select(gen):
    tree, external_names = decompile(gen)
    globals = gen.gi_frame.f_globals
    locals = gen.gi_frame.f_locals
    variables = {}
    for name in external_names:
        try: value = locals[name]
        except KeyError:
            try: value = globals[name]
            except KeyError:
                if hasattr(__builtin__, name): continue
                else: raise NameError, name
        variables[name] = value
    vartypes = dict((name, get_normalized_type(value)) for name, value in variables.iteritems())
    return Query(gen, tree, vartypes, variables)

class Query(object):
    def __init__(query, gen, tree, vartypes, variables):
        query._gen = gen
        query._tree = tree
        query._vartypes = vartypes
        query._variables = variables
        query._result = None
        query._python_ast_key = gen.gi_frame.f_code, tuple(sorted(vartypes.iteritems()))
        translator = python_ast_cache.get(query._python_ast_key)
        if translator is None:
            translator = SQLTranslator(tree, vartypes)
            python_ast_cache[query._python_ast_key] = translator
        query._translator = translator
        query._database = translator.entity._diagram_.database
        query._order = None
        query._limit = None
    def __iter__(query):
        translator = query._translator
        sql_key = query._python_ast_key + (query._order, query._limit)
        cache_entry = sql_cache.get(sql_key)
        database = query._database
        if cache_entry is None:
            sql_ast = translator.sql_ast
            if query._order:
                alias = translator.alias
                orderby_section = [ ORDER_BY ]
                for attr in query._order:
                    for column in attr.columns:
                        orderby_section.append(([COLUMN, alias, column], ASC))
                sql_ast = sql_ast + [ orderby_section ]
            if query._limit:
                start, stop = query._limit
                limit = stop - start
                offset = start
                assert limit is not None
                limit_section = [ LIMIT, [ VALUE, limit ]]
                if offset: limit_section.append([ VALUE, offset ])
                sql_ast = sql_ast + [ limit_section ]
            con, provider = database._get_connection()
            sql, adapter = provider.ast2sql(con, sql_ast)
            cache_entry = sql, adapter
            sql_cache[sql_key] = cache_entry
        else: sql, adapter = cache_entry
        param_dict = {}
        for param_name, extractor in translator.extractors.items():
            param_dict[param_name] = extractor(query._variables)
        arguments = adapter(param_dict)
        cursor = database._exec_sql(sql, arguments)
        result = translator.entity._fetch_objects(cursor, translator.attr_offsets)
        if translator.attrname is not None:
            return imap(attrgetter(translator.attrname), result)
        return iter(result)
    def orderby(query, *args):
        if not args: raise TypeError('query.orderby() requires at least one argument')
        entity = query._translator.entity
        for arg in args:
            if not isinstance(arg, orm.Attribute): raise TypeError(
                'query.orderby() arguments must be attributes. Got: %r' % arg)
            if entity._adict_.get(arg.name) is not arg: raise TypeError(
                'Attribute %s does not belong to Entity %s' % (arg, entity.__name__))
        new_query = object.__new__(Query)
        new_query.__dict__.update(query.__dict__)
        new_query._order = args
        return new_query
    def __getitem__(query, key):
        if isinstance(key, slice):
            step = key.step
            if step is not None and step <> 1: raise TypeError("Parameter 'step' of slice object is not allowed here")
            start = key.start
            if start is None: start = 0
            elif start < 0: raise TypeError("Parameter 'start' of slice object cannot be negative")
            stop = key.stop
            if stop is None:
                if start is None: return query
                elif not query._limit: raise TypeError("Parameter 'stop' of slice object should be specified")
                else: stop = query._limit[1]
        else:
            try: i = key.__index__()
            except AttributeError:
                try: i = key.__int__()
                except AttributeError:
                    raise TypeError('Incorrect argument type: %r' % key)
            start = i
            stop = i + 1
        if query._limit is not None:
            prev_start, prev_stop = query._limit
            start = prev_start + start
            stop = min(prev_stop, prev_start + stop)
        if start >= stop: start = stop = 0
        new_query = object.__new__(Query)
        new_query.__dict__.update(query.__dict__)
        new_query._limit = start, stop
        return new_query
    def limit(query, limit, offset=None):
        start = offset or 0
        stop = start + limit
        return query[start:stop]
    def fetch(query):
        return list(query)

primitive_types = set([ int, unicode ])
type_normalization_dict = { long : int, str : unicode, StrHtml : unicode, Html : unicode }

def get_normalized_type(value):
    if isinstance(value, orm.EntityMeta): return value
    value_type = type(value)
    if value_type is orm.EntityIter: return value.entity
    return normalize_type(value_type)

def normalize_type(t):
    if t is NoneType: return t
    t = type_normalization_dict.get(t, t)
    if t not in primitive_types and not isinstance(t, orm.EntityMeta): raise TypeError, t
    return t

def are_comparable_types(op, type1, type2):
    # op: '<' | '>' | '=' | '>=' | '<=' | '<>' | '!=' | '=='
    #         | 'in' | 'not' 'in' | 'is' | 'is' 'not'
    if op in ('is', 'is not'): return type1 is not NoneType and type2 is NoneType
    if op in ('<', '<=', '>', '>='): return type1 is type2 and type1 in primitive_types
    if op in ('==', '<>', '!='):
        if type1 is NoneType and type2 is NoneType: return False
        if type1 is NoneType or type2 is NoneType: return True
        elif type1 in primitive_types: return type1 is type2
        elif isinstance(type1, orm.EntityMeta): return type1._root_ is type2._root_
        else: return False
    else: assert False

def sqland(items):
    if len(items) == 1: return items[0]
    return [ AND ] + items

def sqlor(items):
    if len(items) == 1: return items[0]
    return [ OR ] + items

class ASTTranslator(object):
    def __init__(translator, tree):
        translator.tree = tree
        translator.pre_methods = {}
        translator.post_methods = {}
    def dispatch(translator, node):
        cls = node.__class__

        try: pre_method = translator.pre_methods[cls]
        except KeyError:
            pre_method = getattr(translator, 'pre' + cls.__name__, None)
            translator.pre_methods[cls] = pre_method
        if pre_method is not None:
            # print 'PRE', node.__class__.__name__, '+'
            pre_method(node)
        else:            
            # print 'PRE', node.__class__.__name__, '-'
            translator.default_pre(node)
        
        for child in node.getChildNodes(): translator.dispatch(child)

        try: post_method = translator.post_methods[cls]
        except KeyError:
            post_method = getattr(translator, 'post' + cls.__name__, None)
            translator.post_methods[cls] = post_method
        if post_method is not None:
            # print 'POST', node.__class__.__name__, '+'
            post_method(node)
        else:            
            # print 'POST', node.__class__.__name__, '-'
            translator.default_post(node)
    def default_pre(translator, node):
        pass
    def default_post(translator, node):
        pass

class SQLTranslator(ASTTranslator):
    def __init__(translator, tree, vartypes):
        assert isinstance(tree, ast.GenExprInner)
        ASTTranslator.__init__(translator, tree)
        translator.diagram = None
        translator.vartypes = vartypes
        translator.iterables = iterables = {}
        translator.aliases = aliases = {}
        translator.extractors = {}
        translator.distinct = False
        translator.from_ = [ FROM ]
        conditions = translator.conditions = []
        translator.inside_expr = False
        translator.alias_counters = {}
        for i, qual in enumerate(tree.quals):
            assign = qual.assign
            if not isinstance(assign, ast.AssName): raise TypeError
            if assign.flags != 'OP_ASSIGN': raise TypeError

            name = assign.name
            if name in iterables: raise TranslationError('Duplicate name: %s' % name)
            if name.startswith('__'): raise TranslationError('Illegal name: %s' % name)
            assert name not in aliases

            node = qual.iter
            attr_names = []
            while isinstance(node, ast.Getattr):
                attr_names.append(node.attrname)
                node = node.expr
            if not isinstance(node, ast.Name): raise TypeError

            if not attr_names:
                if i > 0: translator.distinct = True
                iter_name = node.name
                entity = vartypes[iter_name] # can raise KeyError
                if not isinstance(entity, orm.EntityMeta): raise NotImplementedError

                if translator.diagram is None: translator.diagram = entity._diagram_
                elif translator.diagram is not entity._diagram_: raise TranslationError(
                    'All entities in a query must belong to the same diagram')
            else:
                if len(attr_names) > 1: raise NotImplementedError
                attr_name = attr_names[0]
                parent_entity = iterables.get(node.name)
                if parent_entity is None: raise TranslationError("Name %r must be defined in query")
                attr = parent_entity._adict_.get(attr_name)
                if attr is None: raise AttributeError, attr_name
                if not attr.is_collection: raise TypeError
                if not isinstance(attr, orm.Set): raise NotImplementedError
                entity = attr.py_type
                if not isinstance(entity, orm.EntityMeta): raise NotImplementedError
                reverse = attr.reverse
                if not reverse.is_collection:
                    for c1, c2 in zip(parent_entity._pk_columns_, reverse.columns):
                        conditions.append([ EQ, [ COLUMN, node.name, c1 ], [ COLUMN, name, c2 ] ])
                else:
                    if not isinstance(reverse, orm.Set): raise NotImplementedError
                    translator.distinct = True
                    m2m_table = attr.table
                    m2m_alias = '%s--%s' % (node.name, name)
                    aliases[m2m_alias] = m2m_alias
                    translator.from_.append([ m2m_alias, TABLE, m2m_table ])
                    for c1, c2 in zip(parent_entity._pk_columns_, reverse.columns):
                        conditions.append([ EQ, [ COLUMN, node.name, c1 ], [ COLUMN, m2m_alias, c2 ] ])
                    for c1, c2 in zip(attr.columns, entity._pk_columns_):
                        conditions.append([ EQ, [ COLUMN, m2m_alias, c1 ], [ COLUMN, name, c2 ] ])
            iterables[name] = entity
            aliases[name] = name
            translator.from_.append([ name, TABLE, entity._table_ ])
            for if_ in qual.ifs:
                assert isinstance(if_, ast.GenExprIf)
                translator.dispatch(if_)
                translator.conditions.append(if_.monad.getsql())
        translator.inside_expr = True
        translator.dispatch(tree.expr)
        monad = tree.expr.monad
        translator.attrname = None
        if isinstance(monad, (StringAttrMonad, NumericAttrMonad)):
            translator.attrname = monad.attr.name
            monad = monad.parent
        if not isinstance(monad, (ObjectIterMonad, ObjectAttrMonad)):
            raise TranslationError, monad
        alias = translator.alias = monad.alias
        entity = translator.entity = monad.type
        if isinstance(monad, ObjectIterMonad):
            if alias != translator.tree.quals[-1].assign.name:
                translator.distinct = True
        elif isinstance(monad, ObjectAttrMonad):
            translator.distinct = True
            short_alias = aliases.get(alias)
            if short_alias is None:
                short_alias = translator.get_short_alias(alias, entity)
                aliases[alias] = short_alias
                translator.from_.append([ short_alias, TABLE, entity._table_ ])
                assert len(monad.columns) == len(entity._pk_columns_)
                for c1, c2 in zip(monad.columns, entity._pk_columns_):
                    conditions.append([ EQ, [ COLUMN, monad.base_alias, c1 ], [ COLUMN, short_alias, c2 ] ])
            alias = short_alias
        else: assert False
        translator.select, translator.attr_offsets = entity._construct_select_clause_(alias, translator.distinct)
        translator.sql_ast = [ SELECT, translator.select, translator.from_ ]
        if translator.conditions: translator.sql_ast.append([ WHERE, sqland(translator.conditions) ])
    def postGenExprIf(translator, node):
        monad = node.test.monad
        if monad.type is not bool: raise TypeError
        node.monad = monad
    def postCompare(translator, node):
        expr1 = node.expr
        ops = node.ops
        if len(ops) > 1: raise NotImplementedError
        op, expr2 = ops[0]
        # op: '<' | '>' | '=' | '>=' | '<=' | '<>' | '!=' | '=='
        #         | 'in' | 'not in' | 'is' | 'is not'
        if op.endswith('in'):
            node.monad = expr2.monad.contains(expr1.monad, op == 'not in')
        else:
            node.monad = expr1.monad.cmp(op, expr2.monad)
    def postConst(translator, node):
        value = node.value
        if type(value) is not tuple:
            node.monad = ConstMonad(translator, value)
        else:
            node.monad = ListMonad(translator, [ ConstMonad(translator, item) for item in value ])
    def postList(translator, node):
        node.monad = ListMonad(translator, [ item.monad for item in node.nodes ])
    def postTuple(translator, node):
        node.monad = ListMonad(translator, [ item.monad for item in node.nodes ])
    def postName(translator, node):
        name = node.name
        if name in translator.iterables:
            entity = translator.iterables[name]
            node.monad = ObjectIterMonad(translator, name, entity)
        else:
            try: value_type = translator.vartypes[name]
            except KeyError:
                func = getattr(__builtin__, name, None)
                if func is None: raise NameError(name)
                func_monad_class = special_functions.get(func)
                if func_monad_class is None: raise NotImlementedError
                node.monad = func_monad_class(translator)
            else:
                if value_type is NoneType: node.monad = NoneMonad(translator)
                else: node.monad = ParamMonad(translator, value_type, name)
    def postAdd(translator, node):
        node.monad = node.left.monad + node.right.monad
    def postSub(translator, node):
        node.monad = node.left.monad - node.right.monad
    def postMul(translator, node):
        node.monad = node.left.monad * node.right.monad
    def postDiv(translator, node):
        node.monad = node.left.monad / node.right.monad
    def postPower(translator, node):
        node.monad = node.left.monad ** node.right.monad
    def postUnarySub(translator, node):
        node.monad = -node.expr.monad
    def postGetattr(translator, node):
        node.monad = node.expr.monad.getattr(node.attrname)
    def postAnd(translator, node):
        node.monad = AndMonad([ subnode.monad for subnode in node.nodes ])
    def postOr(translator, node):
        node.monad = OrMonad([ subnode.monad for subnode in node.nodes ])
    def postNot(translator, node):
        node.monad = NotMonad(node.expr.monad)
    def postCallFunc(translator, node):
        if node.star_args is not None: raise NotImplementedError
        if node.dstar_args is not None: raise NotImplementedError
        args = []
        keyargs = {}
        for arg in node.args:
            if isinstance(arg, ast.Keyword):
                keyargs[arg.name] = arg.expr.monad
            else: args.append(arg.monad)
        func_monad = node.node.monad
        node.monad = func_monad(*args, **keyargs)
    def postSubscript(translator, node):
        assert node.flags == 'OP_APPLY'
        assert isinstance(node.subs, list) and len(node.subs) == 1
        expr_monad = node.expr.monad
        index_monad = node.subs[0].monad
        node.monad = expr_monad[index_monad]
    def postSlice(translator, node):
        assert node.flags == 'OP_APPLY'
        expr_monad = node.expr.monad
        upper = node.upper
        if upper is not None: upper = upper.monad
        lower = node.lower
        if lower is not None: lower = lower.monad
        node.monad = expr_monad[lower:upper]
    def get_short_alias(translator, alias, entity):
        if len(alias) <= MAX_ALIAS_LENGTH: return alias
        name = entity.__name__[:MAX_ALIAS_LENGTH-3].lower()
        i = translator.alias_counters.setdefault(name, 1)
        short_alias = '%s-%d' % (name, i)
        translator.alias_counters[name] = i + 1
        return short_alias

class Monad(object):
    def __init__(monad, translator, type):
        monad.translator = translator
        monad.type = type
    def cmp(monad, op, monad2):
        return CmpMonad(op, monad, monad2)
    def contains(monad, item, not_in=False): raise TypeError
    def __nonzero__(monad): raise TypeError

    def getattr(monad, attrname): raise TypeError
    def __call__(monad, *args, **keyargs): raise TypeError
    def len(monad): raise TypeError
    def __getitem__(monad, key): raise TypeError
    def __iter__(monad): raise TypeError

    def __add__(monad, monad2): raise TypeError
    def __sub__(monad, monad2): raise TypeError
    def __mul__(monad, monad2): raise TypeError
    def __div__(monad, monad2): raise TypeError
    def __pow__(monad, monad2): raise TypeError

    def __neg__(monad): raise TypeError
    def __abs__(monad): raise TypeError

class ListMonad(Monad):
    def __init__(monad, translator, items):
        Monad.__init__(monad, translator, list)
        monad.items = items
    def contains(monad, x, not_in=False):
        for item in monad.items:
            if not are_comparable_types('==', x.type, item.type): raise TypeError
        left_sql = x.getsql()
        if len(left_sql) == 1:
            if not_in: sql = [ NOT_IN, left_sql[0], [ item.getsql()[0] for item in monad.items ] ]
            else: sql = [ IN, left_sql[0], [ item.getsql()[0] for item in monad.items ] ]
        elif not_in:
            sql = sqland([ sqlor([ [ NE, a, b ]  for a, b in zip(left_sql, item.getsql()) ]) for item in monad.items ])
        else:
            sql = sqlor([ sqland([ [ EQ, a, b ]  for a, b in zip(left_sql, item.getsql()) ]) for item in monad.items ])
        return BoolExprMonad(monad.translator, sql)

def make_numeric_binop(sqlop):
    def numeric_binop(monad, monad2):
        if not isinstance(monad2, NumericMixin): raise TypeError
        left_sql = monad.getsql()
        right_sql = monad2.getsql()
        assert len(left_sql) == len(right_sql) == 1
        return ExprMonad(monad.translator, int, [ sqlop, left_sql[0], right_sql[0] ])
    numeric_binop.__name__ = sqlop
    return numeric_binop

class NumericMixin(object):
    __add__ = make_numeric_binop(ADD)
    __sub__ = make_numeric_binop(SUB)
    __mul__ = make_numeric_binop(MUL)
    __div__ = make_numeric_binop(DIV)
    __pow__ = make_numeric_binop(POW)
    def __neg__(monad):
        sql = monad.getsql()[0]
        return ExprMonad(monad.translator, int, [ NEG, sql ])
    def __abs__(monad):
        sql = monad.getsql()[0]
        return ExprMonad(monad.translator, int, [ ABS, sql ])

def make_string_binop(sqlop):
    def string_binop(monad, monad2):
        if not isinstance(monad2, StringMixin): raise TypeError
        left_sql = monad.getsql()
        right_sql = monad2.getsql()
        assert len(left_sql) == len(right_sql) == 1
        return ExprMonad(monad.translator, unicode, [ sqlop, left_sql[0], right_sql[0] ])
    string_binop.__name__ = sqlop
    return string_binop

class StringMixin(object):
    def getattr(monad, attrname):
        return StringMethodMonad(monad.translator, monad, attrname)
    __add__ = make_string_binop(CONCAT)
    def __getitem__(monad, index):
        if isinstance(index, slice):
            if index.step is not None: raise TypeError("Slice 'step' attribute is not supported")
            start, stop = index.start, index.stop
            if start is None and stop is None: return monad
            if isinstance(monad, StringConstMonad) \
               and (start is None or isinstance(start, NumericConstMonad)) \
               and (stop is None or isinstance(stop, NumericConstMonad)):
                if start is not None: start = start.value
                if stop is not None: stop = stop.value
                return StringConstMonad(monad.translator, monad.value[start:stop])

            if start is not None and start.type is not int: raise TypeError('string indices must be integers')
            if stop is not None and stop.type is not int: raise TypeError('string indices must be integers')
            
            expr_sql = monad.getsql()[0]

            if start is None:
                start_sql = [ VALUE, 1 ]
            elif isinstance(start, NumericConstMonad):
                if start.value < 0: raise NotImplementedError('Negative slice indices not supported')
                start_sql = [ VALUE, start.value + 1 ]
            else:
                start_sql = start.getsql()[0]
                start_sql = [ ADD, start_sql, [ VALUE, 1 ] ]

            if stop is None:
                len_sql = None
            elif isinstance(stop, NumericConstMonad):
                if stop.value < 0: raise NotImplementedError('Negative slice indices not supported')
                if start is None:
                    len_sql = [ VALUE, stop.value ]
                elif isinstance(start, NumericConstMonad):
                    len_sql = [ VALUE, stop.value - start.value ]
                else:
                    len_sql = [ SUB, [ VALUE, stop.value ], start_sql ]
            else:
                stop_sql = stop.getsql()[0]
                len_sql = [ SUB, stop_sql, start_sql ]

            sql = [ SUBSTR, expr_sql, start_sql, len_sql ]
            return ExprMonad(monad.translator, unicode, sql)
        
        if isinstance(monad, StringConstMonad) and isinstance(index, NumericConstMonad):
            return StringConstMonad(monad.translator, monad.value[index.value])
        if index.type is not int: raise TypeError('string indices must be integers')
        expr_sql = monad.getsql()[0]
        if isinstance(index, NumericConstMonad):
            value = index.value
            if value >= 0: value += 1
            index_sql = [ VALUE, value ]
        else:
            inner_sql = index.getsql()[0]
            index_sql = [ ADD, inner_sql, [ CASE, None, [ ([GE, inner_sql, [ VALUE, 0 ]], [ VALUE, 1 ]) ], [ VALUE, 0 ] ] ]
        sql = [ SUBSTR, expr_sql, index_sql, [ VALUE, 1 ] ]
        return ExprMonad(monad.translator, unicode, sql)
    def len(monad):
        sql = monad.getsql()[0]
        return ExprMonad(monad.translator, int, [ LENGTH, sql ])
    def contains(monad, item, not_in=False):
        if item.type is not unicode: raise TypeError
        if isinstance(item, StringConstMonad):
            item_sql = [ VALUE, '%%%s%%' % item.value ]
        else:
            item_sql = [ CONCAT, [ VALUE, '%' ], item.getsql()[0], [ VALUE, '%' ] ]
        sql = [ LIKE, monad.getsql()[0], item_sql ]
        return BoolExprMonad(monad.translator, sql)
        
class MethodMonad(Monad):
    def __init__(monad, translator, parent, attrname):
        Monad.__init__(monad, translator, 'METHOD')
        monad.parent = parent
        monad.attrname = attrname
        try: method = getattr(monad, 'call_' + monad.attrname)
        except AttributeError:
            raise AttributeError('%r object has no attribute %r' % (parent.type.__name__, attrname))
    def __call__(monad, *args, **keyargs):
        method = getattr(monad, 'call_' + monad.attrname)
        return method(*args, **keyargs)

def make_string_func(sqlop):
    def func(monad):
        sql = monad.parent.getsql()
        assert len(sql) == 1
        return ExprMonad(monad.translator, unicode, [ sqlop, sql[0] ])
    func.__name__ = sqlop
    return func

class StringMethodMonad(MethodMonad):
    call_upper = make_string_func(UPPER)
    call_lower = make_string_func(LOWER)
    def call_startswith(monad, arg):
        parent_sql = monad.parent.getsql()[0]
        if arg.type is not unicode:
            raise TypeError("Argument of 'startswith' method must be a string")
        if isinstance(arg, StringConstMonad):
            assert isinstance(arg.value, basestring)
            arg_sql = [ VALUE, arg.value + '%' ]
        else:
            arg_sql = arg.getsql()[0]
            arg_sql = [ CONCAT, arg_sql, [ VALUE, '%' ] ]
        sql = [ LIKE, parent_sql, arg_sql ]
        return BoolExprMonad(monad.translator, sql)
    def call_endswith(monad, arg):
        parent_sql = monad.parent.getsql()[0]
        if arg.type is not unicode:
            raise TypeError("Argument of 'endswith' method must be a string")
        if isinstance(arg, StringConstMonad):
            assert isinstance(arg.value, basestring)
            arg_sql = [ VALUE, '%' + arg.value ]
        else:
            arg_sql = arg.getsql()[0]
            arg_sql = [ CONCAT, [ VALUE, '%' ], arg_sql ]
        sql = [ LIKE, parent_sql, arg_sql ]
        return BoolExprMonad(monad.translator, sql)
    def strip(monad, chars, strip_type):
        parent_sql = monad.parent.getsql()[0]
        if chars is not None and chars.type is not unicode:
            raise TypeError("'chars' argument must be a string")
        if chars is None:
            return ExprMonad(monad.translator, unicode, [ strip_type, parent_sql ])
        else:
            chars_sql = chars.getsql()[0]
            return ExprMonad(monad.translator, unicode, [ strip_type, parent_sql, chars_sql ])
    def call_strip(monad, chars=None):
        return monad.strip(chars, TRIM)
    def call_lstrip(monad, chars=None):
        return monad.strip(chars, LTRIM)
    def call_rstrip(monad, chars=None):
        return monad.strip(chars, RTRIM)
    
class ObjectMixin(object): pass

class ObjectIterMonad(ObjectMixin, Monad):
    def __init__(monad, translator, alias, entity):
        Monad.__init__(monad, translator, entity)
        monad.alias = alias
    def getattr(monad, name):
        entity = monad.type
        attr = getattr(entity, name) # can raise AttributeError
        if monad.translator.inside_expr and attr.is_collection:
            raise TranslationError('Collection attributes cannot be used inside expression part of select')
        return AttrMonad(monad, attr, monad.alias)
    def getsql(monad):
        entity = monad.type
        return [ [ COLUMN, monad.alias, column ] for attr in entity._pk_attrs_ if not attr.is_collection
                                                 for column in attr.columns ]

class AttrMonad(Monad):
    def __new__(cls, translator, attr, *args, **keyargs):
        assert cls is AttrMonad
        type = normalize_type(attr.py_type)
        if type is int: cls = NumericAttrMonad
        elif type is unicode: cls = StringAttrMonad
        elif isinstance(type, orm.EntityMeta): cls = ObjectAttrMonad
        else: assert False
        return object.__new__(cls)
    def __init__(monad, parent, attr, base_alias, columns=None, alias=None):
        type = normalize_type(attr.py_type)
        Monad.__init__(monad, parent.translator, type)
        monad.parent = parent
        monad.attr = attr
        monad.base_alias = base_alias
        monad.columns = columns or attr.columns
        monad.alias = alias or '-'.join((base_alias, attr.name))
    def getsql(monad):
        return [ [ COLUMN, monad.base_alias, column ] for column in monad.columns ]

class ObjectAttrMonad(ObjectMixin, AttrMonad):
    def getattr(monad, name):
        alias = monad.alias
        translator = monad.translator
        entity = monad.type
        attr = getattr(entity, name) # can raise AttributeError
        if translator.inside_expr and attr.is_collection:
            raise TranslationError('Collection attributes cannot be used inside expression part of select')
        if attr.pk_offset is not None:
            base_alias = monad.base_alias
            columns = monad.columns
            if entity._pk_is_composite_:
                i = 0
                for a in entity._pk_attrs_:
                    if a is attr: break
                    i += len(a.columns)
                columns = columns[i:i+len(attr.columns)]
        else:
            short_alias = translator.aliases.get(alias)
            if short_alias is None:
                short_alias = translator.get_short_alias(alias, entity)
                translator.aliases[alias] = short_alias
                translator.from_.append([ short_alias, TABLE, entity._table_ ])
                conditions = translator.conditions
                assert len(monad.columns) == len(entity._pk_columns_)
                for c1, c2 in zip(monad.columns, entity._pk_columns_):
                    conditions.append([ EQ, [ COLUMN, monad.base_alias, c1 ], [ COLUMN, short_alias, c2 ] ])
            base_alias = short_alias
            columns = attr.columns
        attr_alias = '-'.join((alias, name))
        return AttrMonad(monad, attr, base_alias, columns, attr_alias)

class NumericAttrMonad(NumericMixin, AttrMonad): pass
class StringAttrMonad(StringMixin, AttrMonad): pass

class ParamMonad(Monad):
    def __new__(cls, translator, type, name, parent=None):
        assert cls is ParamMonad
        type = normalize_type(type)
        if type is int: cls = NumericParamMonad
        elif type is unicode: cls = StringParamMonad
        elif isinstance(type, orm.EntityMeta): cls = ObjectParamMonad
        else: assert False
        return object.__new__(cls)
    def __init__(monad, translator, type, name, parent=None):
        type = normalize_type(type)
        Monad.__init__(monad, translator, type)
        monad.name = name
        monad.parent = parent
        if parent is None: monad.extractor = lambda variables : variables[name]
        else: monad.extractor = lambda variables : getattr(parent.extractor(variables), name)
    def getsql(monad):
        monad.add_extractors()
        return [ [ PARAM, monad.name ] ]
    def add_extractors(monad):
        name = monad.name
        extractors = monad.translator.extractors
        extractors[name] = monad.extractor

class ObjectParamMonad(ObjectMixin, ParamMonad):
    def __init__(monad, translator, entity, name, parent=None):
        if translator.diagram is not entity._diagram_: raise TranslationError(
            'All entities in a query must belong to the same diagram')
        monad.params = [ '-'.join((name, path)) for path in entity._pk_paths_ ]
        ParamMonad.__init__(monad, translator, entity, name, parent)
    def getattr(monad, name):
        entity = monad.type
        attr = entity._adict_[name]
        return ParamMonad(monad.translator, attr.py_type, name, monad)
    def getsql(monad):
        monad.add_extractors()
        return [ [ PARAM, param ] for param in monad.params ]
    def add_extractors(monad):
        entity = monad.type
        extractors = monad.translator.extractors
        if not entity._raw_pk_is_composite_:
            extractors[monad.params[0]] = lambda variables, extractor=monad.extractor : extractor(variables)._raw_pkval_
        else:
            for i, param in enumerate(monad.params):
                extractors[param] = lambda variables, i=i, extractor=monad.extractor : extractor(variables)._raw_pkval_[i]

class StringParamMonad(StringMixin, ParamMonad): pass
class NumericParamMonad(NumericMixin, ParamMonad): pass

class ExprMonad(Monad):
    def __new__(cls, translator, type, sql):
        assert cls is ExprMonad
        type = normalize_type(type)
        if type is int: cls = NumericExprMonad
        elif type is unicode: cls = StringExprMonad
        else: assert False
        return object.__new__(cls)        
    def __init__(monad, translator, type, sql):
        Monad.__init__(monad, translator, type)
        monad.sql = sql
    def getsql(monad):
        return [ monad.sql ]

class StringExprMonad(StringMixin, ExprMonad): pass
class NumericExprMonad(NumericMixin, ExprMonad): pass

class ConstMonad(Monad):
    def __new__(cls, translator, value):
        assert cls is ConstMonad
        value_type = normalize_type(type(value))
        if value_type is int: cls = NumericConstMonad
        elif value_type is unicode: cls = StringConstMonad
        elif value_type is NoneType: cls = NoneMonad
        else: raise TypeError
        return object.__new__(cls)
    def __init__(monad, translator, value):
        value_type = normalize_type(type(value))
        Monad.__init__(monad, translator, value_type)
        monad.value = value
    def getsql(monad):
        return [ [ VALUE, monad.value ] ]

class NoneMonad(Monad):
    def __init__(monad, translator, value=None):
        assert value is None
        ConstMonad.__init__(monad, translator, value)

class StringConstMonad(StringMixin, ConstMonad):
    def len(monad):
        return ExprMonad(monad.translator, int, [ VALUE, len(monad.value) ])
    
class NumericConstMonad(NumericMixin, ConstMonad): pass

class BoolMonad(Monad):
    def __init__(monad, translator):
        monad.translator = translator
        monad.type = bool

class BoolExprMonad(BoolMonad):
    def __init__(monad, translator, sql):
        monad.translator = translator
        monad.type = bool
        monad.sql = sql
    def getsql(monad):
        return monad.sql

cmpops = { '>=' : GE, '>' : GT, '<=' : LE, '<' : LT }        

class CmpMonad(BoolMonad):
    def __init__(monad, op, left, right):
        if not are_comparable_types(op, left.type, right.type): raise TypeError, [left.type, right.type]
        if op == '<>': op = '!='
        if left.type is NoneType:
            assert right.type is not NoneType
            left, right = right, left
        if right.type is NoneType:
            if op == '==': op = 'is'
            elif op == '!=': op = 'is not'
        elif op == 'is': op = '=='
        elif op == 'is not': op = '!='
        BoolMonad.__init__(monad, left.translator)
        monad.op = op
        monad.left = left
        monad.right = right
    def getsql(monad):
        op = monad.op
        sql = []
        left_sql = monad.left.getsql()
        if op == 'is':
            return sqland([ [ IS_NULL, item ] for item in left_sql ])
        if op == 'is not':
            return sqland([ [ IS_NOT_NULL, item ] for item in left_sql ])
        right_sql = monad.right.getsql()
        assert len(left_sql) == len(right_sql)
        if op in ('<', '<=', '>', '>='):
            assert len(left_sql) == len(right_sql) == 1
            return [ cmpops[op], left_sql[0], right_sql[0] ]
        if op == '==':
            return sqland([ [ EQ, a, b ] for (a, b) in zip(left_sql, right_sql) ])
        if op == '!=':
            return sqlor([ [ NE, a, b ] for (a, b) in zip(left_sql, right_sql) ])
        assert False

class LogicalBinOpMonad(BoolMonad):
    def __init__(monad, operands):
        assert len(operands) >= 2
        for operand in operands:
            if operand.type is not bool: raise TypeError
        BoolMonad.__init__(monad, operands[0].translator)
        monad.operands = operands
    def getsql(monad):
        return [ monad.binop ] + [ operand.getsql() for operand in monad.operands ]

class AndMonad(LogicalBinOpMonad):
    binop = AND

class OrMonad(LogicalBinOpMonad):
    binop = OR

class NotMonad(BoolMonad):
    def __init__(monad, operand):
        if operand.type is not bool: raise TypeError
        BoolMonad.__init__(monad, operand.translator)
        monad.operand = operand
    def getsql(monad):
        return [ NOT, monad.operand.getsql() ]

class FuncMonad(Monad):
    type = None
    def __init__(monad, translator):
        monad.translator = translator

def func_monad(type):
    def decorator(monad_func):
        class SpecificFuncMonad(FuncMonad):
            def __call__(monad, *args, **keyargs):
                for arg in args:
                    assert isinstance(arg, Monad)
                for value in keyargs.values():
                    assert isinstance(value, Monad)
                return monad_func(monad, *args, **keyargs)
        SpecificFuncMonad.type = type
        SpecificFuncMonad.__name__ = monad_func.__name__
        return SpecificFuncMonad
    return decorator

@func_monad(type=int)
def FuncLenMonad(monad, x):
    return x.len()

@func_monad(type=int)
def FuncAbsMonad(monad, x):
    return abs(x)

@func_monad(type=None)
def FuncMinMonad(monad, *args):
    return minmax(MIN, monad, *args)

@func_monad(type=None)
def FuncMaxMonad(monad, *args):
    return minmax(MAX, monad, *args)

def minmax(sqlop, monad, *args):
    if len(args) == 0: raise TypeError
    elif len(args) == 1: raise NotImplementedError
    arg_types = set(arg.type for arg in args)
    if len(arg_types) > 1: raise TypeError
    result_type = arg_types.pop()
    sql = [ sqlop ] + [ arg.getsql()[0] for arg in args ]
    return ExprMonad(monad.translator, result_type, sql)

special_functions = {
    len : FuncLenMonad,
    abs : FuncAbsMonad,
    min : FuncMinMonad,
    max : FuncMaxMonad
}