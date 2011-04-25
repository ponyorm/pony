import __builtin__, types
from compiler import ast
from types import NoneType
from operator import attrgetter
from itertools import imap, izip
from decimal import Decimal
from datetime import date, datetime

from pony import ormcore
from pony.decompiling import decompile
from pony.templating import Html, StrHtml
from pony.clobtypes import LongStr, LongUnicode
from pony.sqlbuilding import SQLBuilder
from pony.sqlsymbols import *

__all__ = 'TranslationError', 'fetch', 'select', 'exists'

MAX_ALIAS_LENGTH = 30

class TranslationError(Exception): pass

python_ast_cache = {}
sql_cache = {}

def fetch(gen):
    return select(gen)._fetch(None)

fetch.sum = lambda gen : select(gen).sum()
fetch.min = lambda gen : select(gen).min()
fetch.max = lambda gen : select(gen).max()
fetch.count = lambda gen : select(gen).count()

def select(gen):
    tree, external_names = decompile(gen)
    globals = gen.gi_frame.f_globals
    locals = gen.gi_frame.f_locals
    entities = {}
    variables = {}
    vartypes = {}
    functions = {}
    for name in external_names:
        try: value = locals[name]
        except KeyError:
            try: value = globals[name]
            except KeyError:
                try: value = getattr(__builtin__, name)
                except AttributeError: raise NameError, name
        if value in special_functions: functions[name] = value
        elif type(value) in (types.FunctionType, types.BuiltinFunctionType):
            raise TypeError('Function %r cannot be used inside query' % value.__name__)
        elif type(value) is types.MethodType:
            raise TypeError('Method %r cannot be used inside query' % value.__name__)
        elif isinstance(value, ormcore.EntityMeta):
            entities[name] = value
        elif isinstance(value, ormcore.EntityIter):
            entities[name] = value.entity
        else:
            variables[name] = value
            vartypes[name] = normalize_type(type(value))
    return Query(gen, tree.code, entities, vartypes, functions, variables)

select.sum = lambda gen : select(gen).sum()
select.min = lambda gen : select(gen).min()
select.max = lambda gen : select(gen).max()
select.count = lambda gen : select(gen).count()

def exists(subquery):
    raise TypeError('Function exists() can be used inside query only')

class QueryResult(list):
    def fetch(self):
        return self

class Query(object):
    def __init__(query, gen, tree, entities, vartypes, functions, variables):
        assert isinstance(tree, ast.GenExprInner)
        query._gen = gen
        query._tree = tree
        query._entities = entities
        query._vartypes = vartypes
        query._variables = variables
        query._result = None
        code = gen.gi_frame.f_code
        key = id(code), tuple(sorted(entities.iteritems())), \
              tuple(sorted(vartypes.iteritems())), tuple(sorted(functions.iteritems()))
        query._python_ast_key = key
        translator = python_ast_cache.get(key)
        if translator is None:
            translator = SQLTranslator(tree, entities, vartypes, functions)
            python_ast_cache[key] = translator
        query._translator = translator
        query._database = translator.entity._diagram_.database
        query._order = query.range = None
        query._aggr_func = query._aggr_select = None
    def _construct_sql(query, range):
        translator = query._translator
        sql_key = query._python_ast_key + (query._order, range, query._aggr_func)
        cache_entry = sql_cache.get(sql_key)
        database = query._database
        if cache_entry is None:
            sql_ast = [ SELECT ]
            if query._aggr_func: sql_ast.append(query._aggr_select)
            else: sql_ast.append(translator.select)
            sql_ast.append(translator.from_)
            if translator.where: sql_ast.append(translator.where)
            if query._order:
                alias = translator.alias
                orderby_section = [ ORDER_BY ]
                for attr, asc in query._order:
                    for column in attr.columns:
                        orderby_section.append(([COLUMN, alias, column], asc and ASC or DESC))
                sql_ast = sql_ast + [ orderby_section ]
            if range:
                start, stop = range
                limit = stop - start
                offset = start
                assert limit is not None
                limit_section = [ LIMIT, [ VALUE, limit ]]
                if offset: limit_section.append([ VALUE, offset ])
                sql_ast = sql_ast + [ limit_section ]
            cache = database._get_cache()
            sql, adapter = database.provider.ast2sql(cache.connection, sql_ast)
            cache_entry = sql, adapter
            sql_cache[sql_key] = cache_entry
        else: sql, adapter = cache_entry
        return sql, adapter
    def _exec_sql(query, range):
        sql, adapter = query._construct_sql(range)
        param_dict = {}
        for param_name, extractor in query._translator.extractors.items():
            param_dict[param_name] = extractor(query._variables)
        arguments = adapter(param_dict)
        cursor = query._database._exec_sql(sql, arguments)
        return cursor
    def _fetch(query, range):
        translator = query._translator
        cursor = query._exec_sql(range)
        result = translator.entity._fetch_objects(cursor, translator.attr_offsets)
        if translator.attrname is None: return QueryResult(result)
        return QueryResult(map(attrgetter(translator.attrname), result))
    def fetch(query):
        return query._fetch(None)
    def __iter__(query):
        return iter(query._fetch(None))
    def orderby(query, *args):
        if not args: raise TypeError('query.orderby() requires at least one argument')
        entity = query._translator.entity
        order = []
        for arg in args:
            if isinstance(arg, ormcore.Attribute): order.append((arg, True))
            elif isinstance(arg, ormcore.DescWrapper): order.append((arg.attr, False))
            else: raise TypeError('query.orderby() arguments must be attributes. Got: %r' % arg)
            attr = order[-1][0]
            if entity._adict_.get(attr.name) is not attr: raise TypeError(
                'Attribute %s does not belong to Entity %s' % (attr, entity.__name__))
        new_query = object.__new__(Query)
        new_query.__dict__.update(query.__dict__)
        new_query._order = tuple(order)
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
                if not start: return query._fetch(None)
                elif not query.range: raise TypeError("Parameter 'stop' of slice object should be specified")
                else: stop = query.range[1]
        else:
            try: i = key.__index__()
            except AttributeError:
                try: i = key.__int__()
                except AttributeError: raise TypeError('Incorrect argument type: %r' % key)
            result = query._fetch((i, i+1))
            return result[0]
        if start >= stop: return []
        return query._fetch((start, stop))
    def limit(query, limit, offset=None):
        start = offset or 0
        stop = start + limit
        return query[start:stop]
    def _aggregate(query, funcsymbol):
        translator = query._translator
        attrname = translator.attrname
        if attrname is not None:
            attr = translator.entity._adict_[attrname]
            attr_type = normalize_type(attr.py_type)
            if funcsymbol is SUM and attr_type not in numeric_types:
                raise TranslationError('sum is valid for numeric attributes only')
        elif funcsymbol is not COUNT: raise TranslationError(
            'Attribute should be specified for "%s" aggregate function' % funcsymbol.lower())
        query._aggr_func = funcsymbol
        column_ast = [ COLUMN, translator.alias, attr.column ]
        if funcsymbol is COUNT:
            if attrname is None: aggr_ast = [ COUNT, ALL ]
            else: aggr_ast = [ COUNT, DISTINCT, column_ast ]
        elif funcsymbol is SUM: aggr_ast = [ COALESCE, [ SUM, column_ast ], [ VALUE, 0 ] ]
        else: aggr_ast = [ funcsymbol, column_ast ]
        query._aggr_select = [ AGGREGATES, aggr_ast ]
        cursor = query._exec_sql(None)
        row = cursor.fetchone()
        if row is not None: result = row[0]
        else: result = None
        if result is None:
            if funcsymbol in (SUM, COUNT): result = 0
            else: return None
        if funcsymbol is COUNT: return result
        converter = attr.converters[0]
        return converter.sql2py(result)
    def sum(query):
        return query._aggregate(SUM)
    def min(query):
        return query._aggregate(MIN)
    def max(query):
        return query._aggregate(MAX)
    def count(query):
        return query._aggregate(COUNT)

numeric_types = set([ int, float, Decimal ])
string_types = set([ str, unicode ])
comparable_types = set([ int, float, Decimal, str, unicode, date, datetime, bool ])
primitive_types = set([ int, float, Decimal, str, unicode, date, datetime, bool, buffer ])

type_normalization_dict = { long : int, bool : int,
                            LongStr : str, LongUnicode : unicode,
                            StrHtml : str, Html : unicode }

def normalize_type(t):
    if t is NoneType: return t
    t = type_normalization_dict.get(t, t)
    if t not in primitive_types and not isinstance(t, ormcore.EntityMeta): raise TypeError, t
    return t

some_comparables = set([ (int, float), (int, Decimal), (date, datetime) ])
some_comparables.update([ (t2, t1) for (t1, t2) in some_comparables ])

def are_comparable_types(op, type1, type2):
    # op: '<' | '>' | '=' | '>=' | '<=' | '<>' | '!=' | '=='
    #         | 'in' | 'not' 'in' | 'is' | 'is' 'not'
    if op in ('is', 'is not'): return type1 is not NoneType and type2 is NoneType
    if op in ('<', '<=', '>', '>='):
        return (type1 is type2 and type1 in comparable_types) \
            or (type1, type2) in some_comparables
    if op in ('==', '<>', '!='):
        if type1 is NoneType and type2 is NoneType: return False
        if type1 is NoneType or type2 is NoneType: return True
        elif type1 in primitive_types:
            return type1 is type2 or (type1, type2) in some_comparables
        elif isinstance(type1, ormcore.EntityMeta):
            if not isinstance(type2, ormcore.EntityMeta): return False
            return type1._root_ is type2._root_
        else: return False
    else: assert False

def sqland(items):
    if not items: return []
    if len(items) == 1: return items[0]
    return [ AND ] + items

def sqlor(items):
    if not items: return []
    if len(items) == 1: return items[0]
    return [ OR ] + items

def join_tables(conditions, alias1, alias2, columns1, columns2):
    assert len(columns1) == len(columns2)
    conditions.extend([ EQ, [ COLUMN, alias1, c1 ], [ COLUMN, alias2, c2 ] ]
                     for c1, c2 in izip(columns1, columns2))

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
            stop = pre_method(node)
        else:            
            # print 'PRE', node.__class__.__name__, '-'
            stop = translator.default_pre(node)

        if stop: return
            
        for child in node.getChildNodes():
            translator.dispatch(child)

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
    def __init__(translator, tree, entities, vartypes, functions, outer_iterables={}):
        assert isinstance(tree, ast.GenExprInner), tree
        ASTTranslator.__init__(translator, tree)
        translator.diagram = None
        translator.entities = entities
        translator.vartypes = vartypes
        translator.functions = functions
        translator.outer_iterables = outer_iterables
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
                entity = entities.get(iter_name)
                if entity is None:
                    if iter_name in vartypes: raise NotImplementedError
                    else: raise NameError, iter_name
                diagram = entity._diagram_
                if diagram.database is None: raise TranslationError(
                    'Entity %s is not mapped to a database' % entity.__name__)
                if translator.diagram is None: translator.diagram = diagram
                elif translator.diagram is not diagram: raise TranslationError(
                    'All entities in a query must belong to the same diagram')
            else:
                if len(attr_names) > 1: raise NotImplementedError
                attrname = attr_names[0]
                parent_entity = iterables.get(node.name) or outer_iterables.get(node.name)
                if parent_entity is None: raise TranslationError("Name %r must be defined in query" % node.name)
                attr = parent_entity._adict_.get(attrname)
                if attr is None: raise AttributeError, attrname
                if not attr.is_collection: raise TypeError
                if not isinstance(attr, ormcore.Set): raise NotImplementedError
                entity = attr.py_type
                if not isinstance(entity, ormcore.EntityMeta): raise NotImplementedError
                reverse = attr.reverse
                if not reverse.is_collection:
                    join_tables(conditions, node.name, name, parent_entity._pk_columns_, reverse.columns)
                else:
                    if not isinstance(reverse, ormcore.Set): raise NotImplementedError
                    translator.distinct = True
                    m2m_table = attr.table
                    m2m_alias = '%s--%s' % (node.name, name)
                    aliases[m2m_alias] = m2m_alias
                    translator.from_.append([ m2m_alias, TABLE, m2m_table ])
                    join_tables(conditions, node.name, m2m_alias, parent_entity._pk_columns_, reverse.columns)
                    join_tables(conditions, m2m_alias, name, attr.columns, entity._pk_columns_)
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
        if isinstance(monad, AttrMonad) and not isinstance(monad, ObjectMixin):
            translator.attrname = monad.attr.name
            monad = monad.parent
        if not isinstance(monad, ObjectMixin):
            raise NotImplementedError
        alias = monad.alias
        entity = translator.entity = monad.type
        if isinstance(monad, ObjectIterMonad):
            if alias != translator.tree.quals[-1].assign.name:
                translator.distinct = True
        elif isinstance(monad, ObjectAttrMonad):
            translator.distinct = True
            assert alias in aliases
        elif isinstance(monad, ObjectFlatMonad): pass
        else: assert False
        short_alias = translator.alias = aliases[alias]
        translator.select, translator.attr_offsets = entity._construct_select_clause_(short_alias, translator.distinct)
        if not translator.conditions: translator.where = None
        else: translator.where = [ WHERE, sqland(translator.conditions) ]
    def preGenExpr(translator, node):
        inner_tree = node.code
        outer_iterables = {}
        outer_iterables.update(translator.outer_iterables)
        outer_iterables.update(translator.iterables)
        subtranslator = SQLTranslator(inner_tree, translator.entities, translator.vartypes, translator.functions, outer_iterables)
        node.monad = QuerySetMonad(translator, subtranslator)
        return True
    def postGenExprIf(translator, node):
        monad = node.test.monad
        if monad.type is not bool: monad = monad.nonzero()
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
        entity = translator.iterables.get(name)
        if entity is None: entity = translator.outer_iterables.get(name)
        if entity is not None:
            node.monad = ObjectIterMonad(translator, name, entity)
            return

        value_type = translator.entities.get(name)
        if value_type is not None:
            node.monad = EntityMonad(translator, value_type)
            return
            
        try: value_type = translator.vartypes[name]
        except KeyError:
            func = translator.functions.get(name)
            if func is None: raise NameError(name)
            func_monad_class = special_functions[func]
            node.monad = func_monad_class(translator)
        else:
            if name in ('True', 'False') and issubclass(value_type, int):
                node.monad = ConstMonad(translator, name == 'True' and 1 or 0)
            elif value_type is NoneType: node.monad = ConstMonad(translator, None)
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
        node.monad = node.expr.monad.negate()
    def preCallFunc(translator, node):
        if node.star_args is not None: raise NotImplementedError
        if node.dstar_args is not None: raise NotImplementedError
        if isinstance(node.node, ast.Name):
            pass

        if len(node.args) > 1: return False
        if not node.args: return False
        arg = node.args[0]
        if not isinstance(arg, ast.GenExpr): return False
        translator.dispatch(node.node)
        func_monad = node.node.monad
        translator.dispatch(arg)
        query_set_monad = arg.monad
        node.monad = func_monad(query_set_monad)
        return True
    def postCallFunc(translator, node):
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
    def get_short_alias(translator, alias, entity_name):
        if alias and len(alias) <= MAX_ALIAS_LENGTH: return alias
        name = entity_name[:MAX_ALIAS_LENGTH-3].lower()
        i = translator.alias_counters.setdefault(name, 0) + 1
        short_alias = '%s-%d' % (name, i)
        translator.alias_counters[name] = i
        return short_alias

class Monad(object):
    def __init__(monad, translator, type):
        monad.translator = translator
        monad.type = type
        monad.mixin_init()
    def mixin_init(monad): pass
    def cmp(monad, op, monad2):
        return CmpMonad(op, monad, monad2)
    def contains(monad, item, not_in=False): raise TypeError
    def nonzero(monad): raise TypeError
    def negate(monad):
        return NotMonad(monad)

    def getattr(monad, attrname): raise TypeError
    def __call__(monad, *args, **keyargs): raise TypeError
    def len(monad): raise TypeError
    def sum(monad): raise TypeError
    def min(monad): raise TypeError
    def max(monad): raise TypeError
    def __getitem__(monad, key): raise TypeError

    def __add__(monad, monad2): raise TypeError
    def __sub__(monad, monad2): raise TypeError
    def __mul__(monad, monad2): raise TypeError
    def __div__(monad, monad2): raise TypeError
    def __pow__(monad, monad2): raise TypeError

    def __neg__(monad): raise TypeError
    def abs(monad): raise TypeError

class EntityMonad(Monad):
    def __call__(monad, *args, **keyargs):
        pkval, avdict = monad.normalize_args(args, keyargs)
        if pkval is None or len(avdict) > len(pkval): raise NotImplementedError
        return ObjectConstMonad(monad.translator, monad.type, pkval)
    def normalize_args(monad, args, keyargs):
        entity = monad.type
        if not args: pass
        elif len(args) != len(entity._pk_attrs_): raise TypeError('Invalid count of attrs in primary key')
        else:
            for attr, val_monad in izip(entity._pk_attrs_, args):
                if keyargs.setdefault(attr.name, val_monad) is not val_monad:
                    raise TypeError('Ambiguos value of attribute %s' % attr.name)
        avdict = {}
        get = entity._adict_.get 
        for name, val_monad in keyargs.items():
            val_type = val_monad.type
            attr = get(name)
            if attr is None: raise TypeError('Unknown attribute %r' % name)
            if attr.is_collection: raise NotImplementedError
            if attr.is_ref:
                if not issubclass(val_type, attr.py_type): raise TypeError
                if not isinstance(val_monad, ObjectConstMonad):
                    raise TypeError('Entity constructor arguments in declarative query should be consts')
                avdict[attr] = val_monad
            elif isinstance(val_monad, ConstMonad):
                val = val_monad.value
                avdict[attr] = attr.check(val, None, entity, from_db=False)
            else: raise TypeError('Entity constructor arguments in declarative query should be consts')
        pkval = map(avdict.get, entity._pk_attrs_)
        if None in pkval: pkval = None
        return pkval, avdict

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

numeric_conversions = {
    (int, float): float,
    (int, Decimal): Decimal,
    }
numeric_conversions.update(((t2, t1), t3) for (t1, t2), t3 in numeric_conversions.items())

def make_numeric_binop(sqlop):
    def numeric_binop(monad, monad2):
        if not isinstance(monad2, NumericMixin): raise TypeError
        t1, t2 = monad.type, monad2.type
        if t1 is t2: result_type = t1
        else: result_type = numeric_conversions.get((t1, t2))
        if result_type is None: raise TypeError('Unsupported combination of %s and %s' % (monad.type, monad2.type))
        left_sql = monad.getsql()
        right_sql = monad2.getsql()
        assert len(left_sql) == len(right_sql) == 1
        return NumericExprMonad(monad.translator, result_type, [ sqlop, left_sql[0], right_sql[0] ])
    numeric_binop.__name__ = sqlop
    return numeric_binop

class NumericMixin(object):
    def mixin_init(monad):
        assert monad.type in numeric_types
    __add__ = make_numeric_binop(ADD)
    __sub__ = make_numeric_binop(SUB)
    __mul__ = make_numeric_binop(MUL)
    __div__ = make_numeric_binop(DIV)
    def __pow__(monad, monad2):
        if not isinstance(monad2, NumericMixin): raise TypeError
        left_sql = monad.getsql()
        right_sql = monad2.getsql()
        assert len(left_sql) == len(right_sql) == 1
        return NumericExprMonad(monad.translator, float, [ POW, left_sql[0], right_sql[0] ])
    def __neg__(monad):
        sql = monad.getsql()[0]
        return NumericExprMonad(monad.translator, monad.type, [ NEG, sql ])
    def abs(monad):
        sql = monad.getsql()[0]
        return NumericExprMonad(monad.translator, monad.type, [ ABS, sql ])
    def nonzero(monad):
        return CmpMonad('!=', monad, ConstMonad(monad.translator, 0))
    def negate(monad):
        return CmpMonad('==', monad, ConstMonad(monad.translator, 0))

class DateMixin(object):
    def mixin_init(monad):
        assert monad.type is date

class DatetimeMixin(object):
    def mixin_init(monad):
        assert monad.type is datetime

def make_string_binop(sqlop):
    def string_binop(monad, monad2):
        if monad.type is not monad2.type: raise TypeError
        left_sql = monad.getsql()
        right_sql = monad2.getsql()
        assert len(left_sql) == len(right_sql) == 1
        return StringExprMonad(monad.translator, monad.type, [ sqlop, left_sql[0], right_sql[0] ])
    string_binop.__name__ = sqlop
    return string_binop

class StringMixin(object):
    def mixin_init(monad):
        assert issubclass(monad.type, basestring)
        monad.type = unicode
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
                return ConstMonad(monad.translator, monad.value[start:stop])

            if start is not None and start.type is not int: raise TypeError('string indices must be integers')
            if stop is not None and stop.type is not int: raise TypeError('string indices must be integers')
            
            expr_sql = monad.getsql()[0]

            if start is None: start = ConstMonad(monad.translator, 0)
            
            if isinstance(start, NumericConstMonad):
                if start.value < 0: raise NotImplementedError('Negative slice indices not supported')
                start_sql = [ VALUE, start.value + 1 ]
            else:
                start_sql = start.getsql()[0]
                start_sql = [ ADD, start_sql, [ VALUE, 1 ] ]

            if stop is None:
                len_sql = None
            elif isinstance(stop, NumericConstMonad):
                if stop.value < 0: raise NotImplementedError('Negative slice indices not supported')
                if isinstance(start, NumericConstMonad):
                    len_sql = [ VALUE, stop.value - start.value ]
                else:
                    len_sql = [ SUB, [ VALUE, stop.value ], start.getsql()[0] ]
            else:
                stop_sql = stop.getsql()[0]
                if isinstance(start, NumericConstMonad):
                    len_sql = [ SUB, stop_sql, [ VALUE, start.value ] ]
                else:
                    len_sql = [ SUB, stop_sql, start.getsql()[0] ]

            sql = [ SUBSTR, expr_sql, start_sql, len_sql ]
            return StringExprMonad(monad.translator, monad.type, sql)
        
        if isinstance(monad, StringConstMonad) and isinstance(index, NumericConstMonad):
            return ConstMonad(monad.translator, monad.value[index.value])
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
        return StringExprMonad(monad.translator, monad.type, sql)
    def len(monad):
        sql = monad.getsql()[0]
        return NumericExprMonad(monad.translator, int, [ LENGTH, sql ])
    def contains(monad, item, not_in=False):
        if item.type is not monad.type: raise TypeError
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
        return StringExprMonad(monad.translator, monad.parent.type, [ sqlop, sql[0] ])
    func.__name__ = sqlop
    return func

class StringMethodMonad(MethodMonad):
    call_upper = make_string_func(UPPER)
    call_lower = make_string_func(LOWER)
    def call_startswith(monad, arg):
        parent_sql = monad.parent.getsql()[0]
        if arg.type is not monad.parent.type:
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
        if arg.type is not monad.parent.type:
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
        if chars is not None and chars.type is not monad.parent.type:
            raise TypeError("'chars' argument must be a %s" % monad.parent.type.__name__)
        if chars is None:
            return StringExprMonad(monad.translator, monad.parent.type, [ strip_type, parent_sql ])
        else:
            chars_sql = chars.getsql()[0]
            return StringExprMonad(monad.translator, monad.parent.type, [ strip_type, parent_sql, chars_sql ])
    def call_strip(monad, chars=None):
        return monad.strip(chars, TRIM)
    def call_lstrip(monad, chars=None):
        return monad.strip(chars, LTRIM)
    def call_rstrip(monad, chars=None):
        return monad.strip(chars, RTRIM)
    
class ObjectMixin(object):
    def mixin_init(monad):
        assert isinstance(monad.type, ormcore.EntityMeta)
    def getattr(monad, name):
        translator = monad.translator
        entity = monad.type
        attr = getattr(entity, name) # can raise AttributeError
        if not attr.is_collection:
            return AttrMonad.new(monad, attr)
        elif not translator.inside_expr:
            return AttrSetMonad(monad, [ attr ])
        else:
            return ObjectFlatMonad(monad, attr)

class ObjectIterMonad(ObjectMixin, Monad):
    def __init__(monad, translator, alias, entity):
        Monad.__init__(monad, translator, entity)
        monad.alias = alias
    def getsql(monad):
        entity = monad.type
        return [ [ COLUMN, monad.alias, column ] for attr in entity._pk_attrs_ if not attr.is_collection
                                                 for column in attr.columns ]

class AttrMonad(Monad):
    @staticmethod
    def new(parent, attr, *args, **keyargs):
        type = normalize_type(attr.py_type)
        if type in numeric_types: cls = NumericAttrMonad
        elif type in string_types: cls = StringAttrMonad
        elif type is date: cls = DateAttrMonad
        elif type is datetime: cls = DatetimeAttrMonad
        elif type is buffer: cls = BufferAttrMonad
        elif isinstance(type, ormcore.EntityMeta): cls = ObjectAttrMonad
        else: raise NotImplementedError, type
        return cls(parent, attr, *args, **keyargs)
    def __init__(monad, parent, attr):
        assert monad.__class__ is not AttrMonad
        attr_type = normalize_type(attr.py_type)
        Monad.__init__(monad, parent.translator, attr_type)
        monad.parent = parent
        monad.attr = attr
        monad.alias = None
    def getsql(monad):
        return [ [ COLUMN, monad.parent.alias, column ] for column in monad.attr.columns ]
        
class ObjectAttrMonad(ObjectMixin, AttrMonad):
    def __init__(monad, parent, attr):
        AttrMonad.__init__(monad, parent, attr)
        monad.alias = '-'.join((parent.alias, attr.name))
        monad._make_join()
    def _make_join(monad):
        translator = monad.translator
        parent = monad.parent
        attr = monad.attr
        alias = monad.alias
        entity = monad.type

        short_alias = translator.aliases.get(alias)
        if short_alias is not None: return
        short_alias = translator.get_short_alias(alias, entity.__name__)
        translator.aliases[alias] = short_alias
        translator.from_.append([ short_alias, TABLE, entity._table_ ])
        join_tables(translator.conditions, parent.alias, short_alias, attr.columns, entity._pk_columns_)

class ObjectFlatMonad(ObjectMixin, Monad):
    def __init__(monad, parent, attr):
        assert parent.translator.inside_expr
        type = normalize_type(attr.py_type)
        Monad.__init__(monad, parent.translator, type)
        monad.parent = parent
        monad.attr = attr
        monad.alias = '-'.join((parent.alias, attr.name))
        monad._make_join()
    def _make_join(monad):
        translator = monad.translator
        conditions = translator.conditions
        parent = monad.parent
        attr = monad.attr
        reverse = attr.reverse
        alias = monad.alias
        entity = monad.type
        parent_entity = monad.parent.type

        short_alias = translator.aliases.get(alias)
        assert short_alias is None
        short_alias = translator.get_short_alias(alias, entity.__name__)
        translator.aliases[alias] = short_alias
        if not reverse.is_collection:           
            translator.from_.append([ short_alias, TABLE, entity._table_ ])
            join_tables(conditions, parent.alias, short_alias, parent_entity._pk_columns_, reverse.columns)
        else:
            m2m_table = attr.table
            m2m_alias = monad.translator.get_short_alias(None, 'm2m-')
            translator.from_.append([ m2m_alias, TABLE, m2m_table ])
            join_tables(conditions, parent.alias, m2m_alias, parent_entity._pk_columns_, reverse.columns)
            translator.from_.append([ short_alias, TABLE, entity._table_ ])
            join_tables(conditions, m2m_alias, alias, attr.columns, entity._pk_columns_)
        
class NumericAttrMonad(NumericMixin, AttrMonad): pass
class StringAttrMonad(StringMixin, AttrMonad): pass
class DateAttrMonad(DateMixin, AttrMonad): pass
class DatetimeAttrMonad(DatetimeMixin, AttrMonad): pass
class BufferAttrMonad(AttrMonad): pass

class ParamMonad(Monad):
    def __new__(cls, translator, type, name, parent=None):
        assert cls is ParamMonad
        type = normalize_type(type)
        if type in numeric_types: cls = NumericParamMonad
        elif type in string_types: cls = StringParamMonad
        elif type is date: cls = DateParamMonad
        elif type is datetime: cls = DatetimeParamMonad
        elif type is buffer: cls = BufferParamMonad
        elif isinstance(type, ormcore.EntityMeta): cls = ObjectParamMonad
        else: raise TypeError, type
        return object.__new__(cls)
    def __init__(monad, translator, type, name, parent=None):
        type = normalize_type(type)
        Monad.__init__(monad, translator, type)
        monad.name = name
        monad.parent = parent
        if not isinstance(type, ormcore.EntityMeta):
            provider = translator.diagram.database.provider
            monad.converter = provider.get_converter_by_py_type(type)
        else: monad.converter = None
        if parent is None: monad.extractor = lambda variables : variables[name]
        else: monad.extractor = lambda variables : getattr(parent.extractor(variables), name)
    def getsql(monad):
        monad.add_extractors()
        return [ [ PARAM, monad.name, monad.converter ] ]
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
        entity = monad.type
        assert len(monad.params) == len(entity._pk_converters_)
        return [ [ PARAM, param, converter ] for param, converter in zip(monad.params, entity._pk_converters_) ]
    def add_extractors(monad):
        entity = monad.type
        extractors = monad.translator.extractors
        if len(entity._pk_columns_) == 1:
            extractors[monad.params[0]] = lambda vars, e=monad.extractor : e(vars)._get_raw_pkval_()[0]
        else:
            for i, param in enumerate(monad.params):
                extractors[param] = lambda vars, i=i, e=monad.extractor : e(vars)._get_raw_pkval_()[i]

class StringParamMonad(StringMixin, ParamMonad): pass
class NumericParamMonad(NumericMixin, ParamMonad): pass
class DateParamMonad(DateMixin, ParamMonad): pass
class DatetimeParamMonad(DatetimeMixin, ParamMonad): pass
class BufferParamMonad(ParamMonad): pass

class ExprMonad(Monad):
    @staticmethod
    def new(translator, type, sql):
        if type in numeric_types: cls = NumericExprMonad
        elif type in string_types: cls = StringExprMonad
        elif type is date: cls = DateExprMonad
        elif type is datetime: cls = DatetimeExprMonad
        else: raise NotImplementedError, type
        return cls(translator, type, sql)
    def __init__(monad, translator, type, sql):
        Monad.__init__(monad, translator, type)
        monad.sql = sql
    def getsql(monad):
        return [ monad.sql ]

class StringExprMonad(StringMixin, ExprMonad): pass
class NumericExprMonad(NumericMixin, ExprMonad): pass
class DateExprMonad(DateMixin, ExprMonad): pass
class DatetimeExprMonad(DatetimeMixin, ExprMonad): pass

class ConstMonad(Monad):
    def __new__(cls, translator, value):
        assert cls is ConstMonad
        value_type = normalize_type(type(value))
        if value_type in numeric_types: cls = NumericConstMonad
        elif value_type in string_types: cls = StringConstMonad
        elif value_type is date: cls = DateConstMonad
        elif value_type is datetime: cls = DatetimeConstMonad
        elif value_type is NoneType: cls = NoneMonad
        else: raise TypeError, value_type
        return object.__new__(cls)
    def __init__(monad, translator, value):
        value_type = normalize_type(type(value))
        Monad.__init__(monad, translator, value_type)
        monad.value = value
    def getsql(monad):
        return [ [ VALUE, monad.value ] ]

class NoneMonad(ConstMonad):
    type = NoneType
    def __init__(monad, translator, value=None):
        assert value is None
        ConstMonad.__init__(monad, translator, value)

class StringConstMonad(StringMixin, ConstMonad):
    def len(monad):
        return ConstMonad(monad.translator, len(monad.value))
    
class NumericConstMonad(NumericMixin, ConstMonad): pass
class DateConstMonad(DateMixin, ConstMonad): pass
class DatetimeConstMonad(DatetimeMixin, ConstMonad): pass

class ObjectConstMonad(Monad):
    def __init__(monad, translator, entity, pkval):
        Monad.__init__(monad, translator, entity)
        monad.pkval = pkval
        rawpkval = monad.rawpkval = []
        for attr, val in izip(entity._pk_attrs_, pkval):
            if attr.is_ref:
                assert isinstance(val, ObjectConstMonad)
                rawpkval.extend(val.rawpkval)
            else:
                assert not isinstance(val, Monad)
                rawpkval.append(val)
    def getsql(monad):
        entity = monad.type
        return [ [ VALUE, value ] for value in monad.rawpkval ]

class BoolMonad(Monad):
    def __init__(monad, translator):
        monad.translator = translator
        monad.type = bool

sql_negation = { IN : NOT_IN, EXISTS : NOT_EXISTS, LIKE : NOT_LIKE, BETWEEN : NOT_BETWEEN, IS_NULL : IS_NOT_NULL }
sql_negation.update((value, key) for key, value in sql_negation.items())

class BoolExprMonad(BoolMonad):
    def __init__(monad, translator, sql):
        monad.translator = translator
        monad.type = bool
        monad.sql = sql
    def getsql(monad):
        return monad.sql
    def negate(monad):
        sql = monad.sql
        sqlop = sql[0]
        negated_op = sql_negation.get(sqlop)
        if negated_op is not None:
            negated_sql = [ negated_op ] + sql[1:]
        elif negated_op == NOT:
            assert len(sql) == 2
            negated_sql = sql[1]
        else:
            return NotMonad(monad.translator, sql)
        return BoolExprMonad(monad.translator, negated_sql)

cmp_ops = { '>=' : GE, '>' : GT, '<=' : LE, '<' : LT }        

cmp_negate = { '<' : '>=', '<=' : '>', '==' : '!=', 'is' : 'is not' }
cmp_negate.update((b, a) for a, b in cmp_negate.items())

class CmpMonad(BoolMonad):
    def __init__(monad, op, left, right):
        if not are_comparable_types(op, left.type, right.type): raise TypeError(
            'Incomparable types: %r and %r' % (left.type, right.type))
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
    def negate(monad):
        return CmpMonad(cmp_negate[monad.op], monad.left, monad.right)
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
            return [ cmp_ops[op], left_sql[0], right_sql[0] ]
        if op == '==':
            return sqland([ [ EQ, a, b ] for (a, b) in zip(left_sql, right_sql) ])
        if op == '!=':
            return sqlor([ [ NE, a, b ] for (a, b) in zip(left_sql, right_sql) ])
        assert False

class LogicalBinOpMonad(BoolMonad):
    def __init__(monad, operands):
        assert len(operands) >= 2
        operands = list(operands)
        for i, operand in enumerate(operands):
            if operand.type is not bool: operands[i] = operand.nonzero()
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
        if operand.type is not bool: operand = operand.nonzero()
        BoolMonad.__init__(monad, operand.translator)
        monad.operand = operand
    def negate(monad):
        return monad.operand
    def getsql(monad):
        return [ NOT, monad.operand.getsql() ]

class FuncMonad(Monad):
    type = None
    def __init__(monad, translator):
        monad.translator = translator

special_functions = {}

def func_monad(func, type):
    def decorator(monad_method):
        class SpecificFuncMonad(FuncMonad):
            def __call__(monad, *args, **keyargs):
                for arg in args:
                    assert isinstance(arg, Monad)
                for value in keyargs.values():
                    assert isinstance(value, Monad)
                return monad_method(monad, *args, **keyargs)
        SpecificFuncMonad.type = type
        SpecificFuncMonad.__name__ = monad_method.__name__
        assert func not in special_functions
        special_functions[func] = SpecificFuncMonad
        return SpecificFuncMonad
    return decorator

@func_monad(Decimal, type=Decimal)
def FuncDecimalMonad(monad, x):
    if not isinstance(x, StringConstMonad): raise TypeError
    return ConstMonad(monad.translator, Decimal(x.value))

@func_monad(date, type=date)
def FuncDateMonad(monad, year, month, day):
    for x, name in zip((year, month, day), ('year', 'month', 'day')):
        if not isinstance(x, NumericMixin) or x.type is not int: raise TypeError(
            "'%s' argument of date(year, month, day) function must be int" % name)
        if not isinstance(x, ConstMonad): raise NotImplementedError
    return ConstMonad(monad.translator, date(year.value, month.value, day.value))

@func_monad(datetime, type=datetime)
def FuncDatetimeMonad(monad, *args):
    for x, name in zip(args, ('year', 'month', 'day', 'hour', 'minute', 'second', 'microsecond')):
        if not isinstance(x, NumericMixin) or x.type is not int: raise TypeError(
            "'%s' argument of datetime(...) function must be int" % name)
        if not isinstance(x, ConstMonad): raise NotImplementedError
    return ConstMonad(monad.translator, datetime(*tuple(arg.value for arg in args)))

@func_monad(len, type=int)
def FuncLenMonad(monad, x):
    return x.len()

@func_monad(abs, type=int)
def FuncAbsMonad(monad, x):
    return x.abs()

@func_monad(sum, type=int)
def FuncSumMonad(monad, x):
    return x.sum()

@func_monad(min, type=None)
def FuncMinMonad(monad, *args):
    if not args: raise TypeError
    if len(args) == 1: return args[0].min()
    return minmax(monad, MIN, *args)

@func_monad(max, type=None)
def FuncMaxMonad(monad, *args):
    if not args: raise TypeError
    if len(args) == 1: return args[0].max()
    return minmax(monad, MAX, *args)

def minmax(monad, sqlop, *args):
    assert len(args) > 1
    sql = [ sqlop ] + [ arg.getsql()[0] for arg in args ]
    arg_types = set(arg.type for arg in args)
    if len(arg_types) > 1: raise TypeError
    result_type = arg_types.pop()
    if result_type not in comparable_types: raise TypeError
    return ExprMonad(monad.translator, result_type, sql)

@func_monad(select, type=None)
def FuncSelectMonad(monad, subquery):
    if not isinstance(subquery, QuerySetMonad): raise TypeError
    return subquery

@func_monad(exists, type=None)
def FuncExistsMonad(monad, subquery):
    if not isinstance(subquery, SetMixin): raise TypeError
    return subquery.nonzero()

class SetMixin(object):
    pass

class AttrSetMonad(SetMixin, Monad):
    def __init__(monad, root, path):
        if root.translator.inside_expr: raise NotImplementedError
        item_type = normalize_type(path[-1].py_type)
        Monad.__init__(monad, root.translator, (item_type,))
        monad.root = root
        monad.path = path
    def cmp(monad, op, monad2):
        raise NotImplementedError
    def contains(monad, item, not_in=False):
        item_type = monad.type[0]
        if not are_comparable_types('==', item_type, item.type): raise TypeError, [item_type, item.type ]
        if isinstance(item_type, ormcore.EntityMeta) and len(item_type._pk_columns_) > 1:
            raise NotImplementedError

        alias, expr, from_ast, conditions = monad._subselect()
        if expr is None:
            assert isinstance(item_type, ormcore.EntityMeta)
            expr = [ COLUMN, alias, item_type._pk_columns_[0] ]
        subquery_ast = [ SELECT, [ ALL, expr ], from_ast, [ WHERE, sqland(conditions) ] ]
        sqlop = not_in and NOT_IN or IN
        return BoolExprMonad(monad.translator, [ sqlop, item.getsql()[0], subquery_ast ])
    def getattr(monad, name):
        item_type = monad.type[0]
        if not isinstance(item_type, ormcore.EntityMeta):
            raise AttributeError, name
        entity = item_type
        attr = entity._adict_.get(name)
        if attr is None: raise AttributeError, name
        return AttrSetMonad(monad.root, monad.path + [ attr ])
    def len(monad):
        if not monad.path[-1].reverse: kind = DISTINCT
        else: kind = ALL
        alias, expr, from_ast, conditions = monad._subselect()
        sql_ast = [ SELECT, [ AGGREGATES, [ COUNT, kind, expr ] ], from_ast, [ WHERE, sqland(conditions) ] ]
        return NumericExprMonad(monad.translator, int, sql_ast)
    def sum(monad):
        item_type = monad.type[0]
        if item_type not in numeric_types: raise TypeError
        alias, expr, from_ast, conditions = monad._subselect()
        sql_ast = [ SELECT, [ AGGREGATES, [COALESCE, [ SUM, expr ], [ VALUE, 0 ]]], from_ast, [ WHERE, sqland(conditions) ] ]
        return NumericExprMonad(monad.translator, item_type, sql_ast)
    def min(monad):
        item_type = monad.type[0]
        if item_type not in comparable_types: raise TypeError
        alias, expr, from_ast, conditions = monad._subselect()
        sql_ast = [ SELECT, [ AGGREGATES, [ MIN, expr ] ], from_ast, [ WHERE, sqland(conditions) ] ]
        return ExprMonad.new(monad.translator, item_type, sql_ast)
    def max(monad):
        item_type = monad.type[0]
        if item_type not in comparable_types: raise TypeError
        alias, expr, from_ast, conditions = monad._subselect()
        sql_ast = [ SELECT, [ AGGREGATES, [ MAX, expr ] ], from_ast, [ WHERE, sqland(conditions) ] ]
        return ExprMonad.new(monad.translator, item_type, sql_ast)
    def nonzero(monad):
        alias, expr, from_ast, conditions = monad._subselect()
        sql_ast = [ EXISTS, from_ast, [ WHERE, sqland(conditions) ] ]
        return BoolExprMonad(monad.translator, sql_ast)
    def negate(monad):
        alias, expr, from_ast, conditions = monad._subselect()
        sql_ast = [ NOT_EXISTS, from_ast, [ WHERE, sqland(conditions) ] ]
        return BoolExprMonad(monad.translator, sql_ast)
    def _subselect(monad):
        from_ast = [ FROM ]
        conditions = []
        alias = None
        prev_alias = monad.root.alias
        expr = None 
        for attr in monad.path:
            prev_entity = attr.entity
            reverse = attr.reverse
            if not reverse:
                assert attr is monad.path[-1] and len(attr.columns) == 1
                expr = [ COLUMN, alias, attr.column ]
                if not attr.is_required:
                    conditions.append([ IS_NOT_NULL, [ COLUMN, alias, attr.column ] ])
                break
            
            next_entity = attr.py_type
            assert isinstance(next_entity, ormcore.EntityMeta)
            alias = '-'.join((prev_alias, attr.name))
            alias = monad.translator.get_short_alias(alias, next_entity.__name__)
            if not attr.is_collection:
                from_ast.append([ alias, TABLE, next_entity._table_ ])
                if attr.columns:                    
                    join_tables(conditions, prev_alias, alias, attr.columns, next_entity._pk_columns_)
                else:
                    assert not reverse.is_collection and reverse.columns
                    join_tables(conditions, prev_alias, alias, prev_entity._pk_columns_, reverse.columns)
            elif reverse.is_collection:
                m2m_table = attr.table
                m2m_alias = monad.translator.get_short_alias(None, 'm2m-')
                from_ast.append([ m2m_alias, TABLE, m2m_table ])
                join_tables(conditions, prev_alias, m2m_alias, prev_entity._pk_columns_, reverse.columns)
                from_ast.append([ alias, TABLE, next_entity._table_ ])
                join_tables(conditions, m2m_alias, alias, attr.columns, next_entity._pk_columns_)
            else:
                from_ast.append([ alias, TABLE, next_entity._table_ ])
                join_tables(conditions, prev_alias, alias, prev_entity._pk_columns_, reverse.columns)
            prev_alias = alias
        assert alias is not None
        return alias, expr, from_ast, conditions
    def getsql(monad):
        raise TranslationError

class QuerySetMonad(SetMixin, Monad):
    def __init__(monad, translator, subtranslator):        
        monad.subtranslator = subtranslator
        attr, attr_type = monad._get_attr_info()
        item_type = attr_type or subtranslator.entity
        monad.item_type = item_type
        monad_type = (item_type,)  # todo: better way to represent type "Set of item_type"
        Monad.__init__(monad, translator, monad_type)
    def _get_attr_info(monad):
        sub = monad.subtranslator
        if sub.attrname is None: return None, None
        attr = sub.entity._adict_[sub.attrname]
        return attr, normalize_type(attr.py_type)
    def contains(monad, item, not_in=False):
        item_type = monad.type[0]
        if not are_comparable_types('==', item_type, item.type): raise TypeError, [item_type, item.type ]
        if isinstance(item_type, ormcore.EntityMeta) and len(item_type._pk_columns_) > 1:
            raise NotImplementedError

        attr, attr_type = monad._get_attr_info()
        if attr is None: columns = item_type._pk_columns_
        else: columns = attr.columns
        if len(columns) > 1: raise NotImplementedError

        sub = monad.subtranslator
        select_ast = [ ALL, [ COLUMN, sub.alias, columns[0] ] ]
        conditions = sub.conditions[:]
        if attr is not None and not attr.is_required:
            conditions.append([ IS_NOT_NULL, [ COLUMN, sub.alias, columns[0] ]])
        subquery_ast = [ SELECT, select_ast, sub.from_, [ WHERE, sqland(conditions) ] ]
        sqlop = not_in and NOT_IN or IN
        return BoolExprMonad(monad.translator, [ sqlop, item.getsql()[0], subquery_ast ])
    def nonzero(monad):        
        sub = monad.subtranslator
        sql_ast = [ EXISTS, sub.from_, [ WHERE, sqland(sub.conditions) ] ]
        return BoolExprMonad(monad.translator, sql_ast)
    def negate(monad):
        sub = monad.subtranslator
        sql_ast = [ NOT_EXISTS, sub.from_, [ WHERE, sqland(sub.conditions) ] ]
        return BoolExprMonad(monad.translator, sql_ast)
    def _subselect(monad, item_type, select_ast):
        sub = monad.subtranslator
        sql_ast = [ SELECT, select_ast, sub.from_, [ WHERE, sqland(sub.conditions) ] ]
        return ExprMonad.new(monad.translator, item_type, sql_ast)
    def len(monad):
        attr, attr_type = monad._get_attr_info()
        if attr is not None:
            if len(attr.columns) > 1: raise NotImplementedError
            select_ast = [ AGGREGATES, [ COUNT, DISTINCT, [ COLUMN, monad.subtranslator.alias, attr.column ] ] ]
        else: select_ast = [ AGGREGATES, [ COUNT, ALL ] ]
        return monad._subselect(int, select_ast)
    def sum(monad):
        attr, attr_type = monad._get_attr_info()
        if attr_type not in numeric_types: raise TypeError
        select_ast = [ AGGREGATES, [ COALESCE, [ SUM, [ COLUMN, monad.subtranslator.alias, attr.column ] ], [ VALUE, 0 ] ] ]
        return monad._subselect(attr_type, select_ast)
    def min(monad):
        attr, attr_type = monad._get_attr_info()
        if attr_type not in comparable_types: raise TypeError
        select_ast = [ AGGREGATES, [ MIN, [ COLUMN, monad.subtranslator.alias, attr.column ] ] ]
        return monad._subselect(attr_type, select_ast)
    def max(monad):
        attr, attr_type = monad._get_attr_info()
        if attr_type not in comparable_types: raise TypeError
        select_ast = [ AGGREGATES, [ MAX, [ COLUMN, monad.subtranslator.alias, attr.column ] ] ]
        return monad._subselect(attr_type, select_ast)
