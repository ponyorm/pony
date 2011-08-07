import __builtin__, types
from itertools import izip
from types import NoneType
from compiler import ast
from decimal import Decimal
from datetime import date, datetime

from pony import options
from pony.clobtypes import LongStr, LongUnicode
from pony.sqlbuilding import SQLBuilder
from pony.sqlsymbols import *
from pony.utils import avg, copy_func_attrs
from pony.orm import select, exists, TranslationError, EntityMeta, Set, JOIN

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

type_normalization_dict = { long : int, bool : int, LongStr : str, LongUnicode : unicode }

class SQLTranslator(ASTTranslator):
    MAX_ALIAS_LENGTH = 30
    numeric_types = set([ int, float, Decimal ])
    string_types = set([ str, unicode ])
    comparable_types = set([ int, float, Decimal, str, unicode, date, datetime, bool ])
    primitive_types = set([ int, float, Decimal, str, unicode, date, datetime, bool, buffer ])

    @classmethod
    def normalize_type(translator, type):
        if type is NoneType: return type
        if issubclass(type, basestring):  # Mainly for Html -> unicode & StrHtml -> str conversion
            if type in (str, unicode): return type
            if issubclass(type, str): return str
            if issubclass(type, unicode): return unicode
            assert False
        type = type_normalization_dict.get(type, type)
        if type not in translator.primitive_types and not isinstance(type, EntityMeta): raise TypeError, type
        return type

    some_comparables = set([ (int, float), (int, Decimal), (date, datetime) ])
    some_comparables.update([ (t2, t1) for (t1, t2) in some_comparables ])

    @classmethod
    def are_comparable_types(translator, op, type1, type2):
        # op: '<' | '>' | '=' | '>=' | '<=' | '<>' | '!=' | '=='
        #         | 'in' | 'not' 'in' | 'is' | 'is' 'not'
        # types must be normalized already! 
        if op in ('is', 'is not'): return type1 is not NoneType and type2 is NoneType
        if op in ('<', '<=', '>', '>='):
            return (type1 is type2 and type1 in translator.comparable_types) \
                or (type1, type2) in translator.some_comparables
        if op in ('==', '<>', '!='):
            if type1 is NoneType and type2 is NoneType: return False
            if type1 is NoneType or type2 is NoneType: return True
            elif type1 in translator.primitive_types:
                return type1 is type2 or (type1, type2) in translator.some_comparables
            elif isinstance(type1, EntityMeta):
                if not isinstance(type2, EntityMeta): return False
                return type1._root_ is type2._root_
            else: return False
        else: assert False

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
                if not isinstance(attr, Set): raise NotImplementedError
                entity = attr.py_type
                if not isinstance(entity, EntityMeta): raise NotImplementedError
                reverse = attr.reverse
                if not reverse.is_collection:
                    join_tables(conditions, node.name, name, parent_entity._pk_columns_, reverse.columns)
                else:
                    if not isinstance(reverse, Set): raise NotImplementedError
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
        if isinstance(monad, translator.AttrMonad) and not isinstance(monad, translator.ObjectMixin):
            translator.attrname = monad.attr.name
            monad = monad.parent
        if not isinstance(monad, translator.ObjectMixin):
            raise NotImplementedError
        alias = monad.alias
        entity = translator.entity = monad.type
        if isinstance(monad, translator.ObjectIterMonad):
            if alias != translator.tree.quals[-1].assign.name:
                translator.distinct = True
        elif isinstance(monad, translator.ObjectAttrMonad):
            translator.distinct = True
            assert alias in aliases
        elif isinstance(monad, translator.ObjectFlatMonad): pass
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
        node.monad = translator.QuerySetMonad(translator, subtranslator)
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
            node.monad = translator.ConstMonad(translator, value)
        else:
            node.monad = translator.ListMonad(translator, [ translator.ConstMonad(translator, item) for item in value ])
    def postList(translator, node):
        node.monad = translator.ListMonad(translator, [ item.monad for item in node.nodes ])
    def postTuple(translator, node):
        node.monad = translator.ListMonad(translator, [ item.monad for item in node.nodes ])
    def postName(translator, node):
        name = node.name
        entity = translator.iterables.get(name)
        if entity is None: entity = translator.outer_iterables.get(name)
        if entity is not None:
            node.monad = translator.ObjectIterMonad(translator, name, entity)
            return

        value_type = translator.entities.get(name)
        if value_type is not None:
            node.monad = translator.EntityMonad(translator, value_type)
            return
            
        try: value_type = translator.vartypes[name]
        except KeyError:
            func = translator.functions.get(name)
            if func is None: raise NameError(name)
            func_monad_class = special_functions[func]
            node.monad = func_monad_class(translator)
        else:
            if name in ('True', 'False') and issubclass(value_type, int):
                node.monad = translator.ConstMonad(translator, name == 'True' and 1 or 0)
            elif value_type is NoneType: node.monad = translator.ConstMonad(translator, None)
            else: node.monad = translator.ParamMonad(translator, value_type, name)
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
        node.monad = translator.AndMonad([ subnode.monad for subnode in node.nodes ])
    def postOr(translator, node):
        node.monad = translator.OrMonad([ subnode.monad for subnode in node.nodes ])
    def postNot(translator, node):
        node.monad = node.expr.monad.negate()
    def preCallFunc(translator, node):
        if node.star_args is not None: raise NotImplementedError
        if node.dstar_args is not None: raise NotImplementedError
        if not isinstance(node.node, (ast.Name, ast.Getattr)): raise NotImplementedError
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
        if alias and not options.SIMPLE_ALIASES \
                 and len(alias) <= translator.MAX_ALIAS_LENGTH: return alias
        name = entity_name[:translator.MAX_ALIAS_LENGTH-3].lower()
        i = translator.alias_counters.setdefault(name, 0) + 1
        short_alias = '%s-%d' % (name, i)
        translator.alias_counters[name] = i
        return short_alias

def wrap_monad_method(cls_name, func):
    overrider_name = '%s_%s' % (cls_name, func.__name__)
    def wrapper(monad, *args, **keyargs):
        overrider = getattr(monad.translator, overrider_name, None)
        if overrider is None: return func(monad, *args, **keyargs)
        return overrider(monad, *args, **keyargs)
    return copy_func_attrs(wrapper, func)

class MonadMeta(type):
    def __new__(meta, cls_name, bases, dict):
        for name, func in dict.items():
            if not isinstance(func, types.FunctionType): continue
            if name in ('__new__', '__init__'): continue
            dict[name] = wrap_monad_method(cls_name, func)
        return super(MonadMeta, meta).__new__(meta, cls_name, bases, dict)

class MonadMixin(object):
    __metaclass__ = MonadMeta

class Monad(object):
    __metaclass__ = MonadMeta
    def __init__(monad, translator, type):
        monad.translator = translator
        monad.type = type
        monad.mixin_init()
    def mixin_init(monad):
        pass
    def cmp(monad, op, monad2):
        return monad.translator.CmpMonad(op, monad, monad2)
    def contains(monad, item, not_in=False): raise TypeError
    def nonzero(monad): raise TypeError
    def negate(monad):
        return monad.translator.NotMonad(monad)

    def getattr(monad, attrname):
        try: property_method = getattr(monad, 'attr_' + attrname)
        except AttributeError:
            if not hasattr(monad, 'call_' + attrname):
                raise AttributeError('%r object has no attribute %r' % (monad.type.__name__, attrname))
            translator = monad.translator
            return translator.MethodMonad(translator, monad, attrname)
        return property_method()
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

class MethodMonad(Monad):
    def __init__(monad, translator, parent, attrname):
        Monad.__init__(monad, translator, 'METHOD')
        monad.parent = parent
        monad.attrname = attrname
    def __call__(monad, *args, **keyargs):
        method = getattr(monad.parent, 'call_' + monad.attrname)
        return method(*args, **keyargs)

class EntityMonad(Monad):
    def __getitem__(monad, key):
        if isinstance(key, ListMonad):
            for item in key.items:
                if not isinstance(item, ConstMonad): raise NotImplementedError, key
            pkval = tuple(item.value for item in key.items)
        elif isinstance(key, ConstMonad):
            pkval = key.value
        else: raise NotImplementedError, key
        if not isinstance(pkval, tuple): pkval = (pkval,)
        entity = monad.type
        if len(pkval) != len(entity._pk_attrs_): raise TypeError('Invalid count of attrs in primary key')
        translator = monad.translator
        for attr, val in izip(entity._pk_attrs_, pkval):
            type1 = translator.normalize_type(attr.py_type)
            type2 = translator.normalize_type(type(val))
            if not translator.are_comparable_types('==', type1, type2): raise TypeError(
                'Incomparable types: %r and %r' % (attr.py_type, type(val)))
        return translator.ObjectConstMonad(translator, monad.type, pkval)
    def __call__(monad, *args, **keyargs):
        pkval, avdict = monad.normalize_args(args, keyargs)
        if pkval is None or len(avdict) > len(pkval): raise NotImplementedError
        translator = monad.translator
        return translator.ObjectConstMonad(translator, monad.type, pkval)
    def normalize_args(monad, args, keyargs):
        translator = monad.translator
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
                if not isinstance(val_monad, translator.ObjectConstMonad):
                    raise TypeError('Entity constructor arguments in declarative query should be consts')
                avdict[attr] = val_monad
            elif isinstance(val_monad, translator.ConstMonad):
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
        translator = monad.translator
        for item in monad.items:
            if not translator.are_comparable_types('==', x.type, item.type): raise TypeError
        left_sql = x.getsql()
        if len(left_sql) == 1:
            if not_in: sql = [ NOT_IN, left_sql[0], [ item.getsql()[0] for item in monad.items ] ]
            else: sql = [ IN, left_sql[0], [ item.getsql()[0] for item in monad.items ] ]
        elif not_in:
            sql = sqland([ sqlor([ [ NE, a, b ]  for a, b in zip(left_sql, item.getsql()) ]) for item in monad.items ])
        else:
            sql = sqlor([ sqland([ [ EQ, a, b ]  for a, b in zip(left_sql, item.getsql()) ]) for item in monad.items ])
        return translator.BoolExprMonad(translator, sql)

numeric_conversions = {
    (int, float): float,
    (int, Decimal): Decimal,
    }
numeric_conversions.update(((t2, t1), t3) for (t1, t2), t3 in numeric_conversions.items())

def make_numeric_binop(sqlop):
    def numeric_binop(monad, monad2):
        translator = monad.translator
        if not isinstance(monad2, translator.NumericMixin): raise TypeError
        t1, t2 = monad.type, monad2.type
        if t1 is t2: result_type = t1
        else: result_type = numeric_conversions.get((t1, t2))
        if result_type is None: raise TypeError('Unsupported combination of %s and %s' % (monad.type, monad2.type))
        left_sql = monad.getsql()
        right_sql = monad2.getsql()
        assert len(left_sql) == len(right_sql) == 1
        return translator.NumericExprMonad(translator, result_type, [ sqlop, left_sql[0], right_sql[0] ])
    numeric_binop.__name__ = sqlop
    return numeric_binop

class NumericMixin(MonadMixin):
    def mixin_init(monad):
        assert monad.type in monad.translator.numeric_types
    __add__ = make_numeric_binop(ADD)
    __sub__ = make_numeric_binop(SUB)
    __mul__ = make_numeric_binop(MUL)
    __div__ = make_numeric_binop(DIV)
    def __pow__(monad, monad2):
        translator = monad.translator
        if not isinstance(monad2, translator.NumericMixin): raise TypeError
        left_sql = monad.getsql()
        right_sql = monad2.getsql()
        assert len(left_sql) == len(right_sql) == 1
        return translator.NumericExprMonad(translator, float, [ POW, left_sql[0], right_sql[0] ])
    def __neg__(monad):
        sql = monad.getsql()[0]
        translator = monad.translator
        return translator.NumericExprMonad(translator, monad.type, [ NEG, sql ])
    def abs(monad):
        sql = monad.getsql()[0]
        translator = monad.translator
        return translator.NumericExprMonad(translator, monad.type, [ ABS, sql ])
    def nonzero(monad):
        translator = monad.translator
        return translator.CmpMonad('!=', monad, translator.ConstMonad(translator, 0))
    def negate(monad):
        translator = monad.translator
        return translator.CmpMonad('==', monad, translator.ConstMonad(translator, 0))

class DateMixin(MonadMixin):
    def mixin_init(monad):
        assert monad.type is date
    def attr_year(monad):
        sql = [ TO_INT, [ SUBSTR, monad.getsql()[0], [ VALUE, 1 ], [ VALUE, 4 ] ] ]
        translator = monad.translator
        return translator.NumericExprMonad(translator, int, sql)
    
class DatetimeMixin(DateMixin):
    def mixin_init(monad):
        assert monad.type is datetime

def make_string_binop(sqlop):
    def string_binop(monad, monad2):
        if monad.type is not monad2.type: raise TypeError
        left_sql = monad.getsql()
        right_sql = monad2.getsql()
        assert len(left_sql) == len(right_sql) == 1
        translator = monad.translator
        return translator.StringExprMonad(translator, monad.type, [ sqlop, left_sql[0], right_sql[0] ])
    string_binop.__name__ = sqlop
    return string_binop

def make_string_func(sqlop):
    def func(monad):
        sql = monad.getsql()
        assert len(sql) == 1
        translator = monad.translator
        return translator.StringExprMonad(translator, monad.type, [ sqlop, sql[0] ])
    func.__name__ = sqlop
    return func

class StringMixin(MonadMixin):
    def mixin_init(monad):
        assert issubclass(monad.type, basestring), monad.type
        monad.type = unicode
    __add__ = make_string_binop(CONCAT)
    def __getitem__(monad, index):
        translator = monad.translator
        if isinstance(index, slice):
            if index.step is not None: raise TypeError("Slice 'step' attribute is not supported")
            start, stop = index.start, index.stop
            if start is None and stop is None: return monad
            if isinstance(monad, translator.StringConstMonad) \
               and (start is None or isinstance(start, translator.NumericConstMonad)) \
               and (stop is None or isinstance(stop, translator.NumericConstMonad)):
                if start is not None: start = start.value
                if stop is not None: stop = stop.value
                return translator.ConstMonad(translator, monad.value[start:stop])

            if start is not None and start.type is not int: raise TypeError('string indices must be integers')
            if stop is not None and stop.type is not int: raise TypeError('string indices must be integers')
            
            expr_sql = monad.getsql()[0]

            if start is None: start = translator.ConstMonad(translator, 0)
            
            if isinstance(start, translator.NumericConstMonad):
                if start.value < 0: raise NotImplementedError('Negative slice indices not supported')
                start_sql = [ VALUE, start.value + 1 ]
            else:
                start_sql = start.getsql()[0]
                start_sql = [ ADD, start_sql, [ VALUE, 1 ] ]

            if stop is None:
                len_sql = None
            elif isinstance(stop, translator.NumericConstMonad):
                if stop.value < 0: raise NotImplementedError('Negative slice indices not supported')
                if isinstance(start, translator.NumericConstMonad):
                    len_sql = [ VALUE, stop.value - start.value ]
                else:
                    len_sql = [ SUB, [ VALUE, stop.value ], start.getsql()[0] ]
            else:
                stop_sql = stop.getsql()[0]
                if isinstance(start, translator.NumericConstMonad):
                    len_sql = [ SUB, stop_sql, [ VALUE, start.value ] ]
                else:
                    len_sql = [ SUB, stop_sql, start.getsql()[0] ]

            sql = [ SUBSTR, expr_sql, start_sql, len_sql ]
            return translator.StringExprMonad(translator, monad.type, sql)
        
        if isinstance(monad, translator.StringConstMonad) and isinstance(index, translator.NumericConstMonad):
            return translator.ConstMonad(translator, monad.value[index.value])
        if index.type is not int: raise TypeError('string indices must be integers')
        expr_sql = monad.getsql()[0]
        if isinstance(index, translator.NumericConstMonad):
            value = index.value
            if value >= 0: value += 1
            index_sql = [ VALUE, value ]
        else:
            inner_sql = index.getsql()[0]
            index_sql = [ ADD, inner_sql, [ CASE, None, [ ([GE, inner_sql, [ VALUE, 0 ]], [ VALUE, 1 ]) ], [ VALUE, 0 ] ] ]
        sql = [ SUBSTR, expr_sql, index_sql, [ VALUE, 1 ] ]
        return translator.StringExprMonad(translator, monad.type, sql)
    def len(monad):
        sql = monad.getsql()[0]
        translator = monad.translator
        return translator.NumericExprMonad(translator, int, [ LENGTH, sql ])
    def contains(monad, item, not_in=False):
        if item.type is not monad.type: raise TypeError
        translator = monad.translator
        if isinstance(item, translator.StringConstMonad):
            item_sql = [ VALUE, '%%%s%%' % item.value ]
        else:
            item_sql = [ CONCAT, [ VALUE, '%' ], item.getsql()[0], [ VALUE, '%' ] ]
        sql = [ LIKE, monad.getsql()[0], item_sql ]
        return translator.BoolExprMonad(translator, sql)
    call_upper = make_string_func(UPPER)
    call_lower = make_string_func(LOWER)
    def call_startswith(monad, arg):
        if arg.type is not monad.type:  # ???
            raise TypeError("Argument of 'startswith' method must be a string")
        translator = monad.translator
        if isinstance(arg, translator.StringConstMonad):
            assert isinstance(arg.value, basestring)
            arg_sql = [ VALUE, arg.value + '%' ]
        else:
            arg_sql = arg.getsql()[0]
            arg_sql = [ CONCAT, arg_sql, [ VALUE, '%' ] ]
        parent_sql = monad.getsql()[0]
        sql = [ LIKE, parent_sql, arg_sql ]
        return translator.BoolExprMonad(translator, sql)
    def call_endswith(monad, arg):
        translator = monad.translator
        if arg.type is not monad.type:
            raise TypeError("Argument of 'endswith' method must be a string")
        if isinstance(arg, translator.StringConstMonad):
            assert isinstance(arg.value, basestring)
            arg_sql = [ VALUE, '%' + arg.value ]
        else:
            arg_sql = arg.getsql()[0]
            arg_sql = [ CONCAT, [ VALUE, '%' ], arg_sql ]
        parent_sql = monad.getsql()[0]
        sql = [ LIKE, parent_sql, arg_sql ]
        return translator.BoolExprMonad(translator, sql)
    def strip(monad, chars, strip_type):
        if chars is not None and chars.type is not monad.type:
            raise TypeError("'chars' argument must be a %s" % monad.type.__name__)
        parent_sql = monad.getsql()[0]
        sql = [ strip_type, parent_sql ]
        if chars is not None: sql.append(chars.getsql()[0])
        translator = monad.translator
        return translator.StringExprMonad(translator, monad.type, sql)
    def call_strip(monad, chars=None):
        return monad.strip(chars, TRIM)
    def call_lstrip(monad, chars=None):
        return monad.strip(chars, LTRIM)
    def call_rstrip(monad, chars=None):
        return monad.strip(chars, RTRIM)
    
class ObjectMixin(MonadMixin):
    def mixin_init(monad):
        assert isinstance(monad.type, EntityMeta)
    def getattr(monad, name):
        translator = monad.translator
        entity = monad.type
        attr = getattr(entity, name) # can raise AttributeError
        if not attr.is_collection:
            return translator.AttrMonad.new(monad, attr)
        elif not translator.inside_expr:
            return translator.AttrSetMonad(monad, [ attr ])
        else:
            return translator.ObjectFlatMonad(monad, attr)

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
        translator = parent.translator
        type = translator.normalize_type(attr.py_type)
        if type in translator.numeric_types: cls = translator.NumericAttrMonad
        elif type in translator.string_types: cls = translator.StringAttrMonad
        elif type is date: cls = translator.DateAttrMonad
        elif type is datetime: cls = translator.DatetimeAttrMonad
        elif type is buffer: cls = translator.BufferAttrMonad
        elif isinstance(type, EntityMeta): cls = translator.ObjectAttrMonad
        else: raise NotImplementedError, type
        return cls(parent, attr, *args, **keyargs)
    def __init__(monad, parent, attr):
        assert monad.__class__ is not AttrMonad
        translator = parent.translator
        attr_type = translator.normalize_type(attr.py_type)
        Monad.__init__(monad, parent.translator, attr_type)
        monad.parent = parent
        monad.attr = attr
        monad.alias = None
    def getsql(monad):
        alias = monad.parent.alias
        short_alias = monad.translator.aliases.get(alias)
        return [ [ COLUMN, short_alias, column ] for column in monad.attr.columns ]
        
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
        translator = parent.translator
        assert translator.inside_expr
        type = translator.normalize_type(attr.py_type)
        Monad.__init__(monad, translator, type)
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
            join_tables(conditions, m2m_alias, short_alias, attr.columns, entity._pk_columns_)
        
class NumericAttrMonad(NumericMixin, AttrMonad): pass
class StringAttrMonad(StringMixin, AttrMonad): pass
class DateAttrMonad(DateMixin, AttrMonad): pass
class DatetimeAttrMonad(DatetimeMixin, AttrMonad): pass
class BufferAttrMonad(AttrMonad): pass

class ParamMonad(Monad):
    def __new__(cls, translator, type, name, parent=None):
        assert cls is ParamMonad
        type = translator.normalize_type(type)
        if type in translator.numeric_types: cls = translator.NumericParamMonad
        elif type in translator.string_types: cls = translator.StringParamMonad
        elif type is date: cls = translator.DateParamMonad
        elif type is datetime: cls = translator.DatetimeParamMonad
        elif type is buffer: cls = translator.BufferParamMonad
        elif isinstance(type, EntityMeta): cls = translator.ObjectParamMonad
        else: raise TypeError, type
        return object.__new__(cls)
    def __init__(monad, translator, type, name, parent=None):
        type = translator.normalize_type(type)
        Monad.__init__(monad, translator, type)
        monad.name = name
        monad.parent = parent
        if not isinstance(type, EntityMeta):
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
        translator = monad.translator
        return translator.ParamMonad(translator, attr.py_type, name, monad)
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
        if type in translator.numeric_types: cls = translator.NumericExprMonad
        elif type in translator.string_types: cls = translator.StringExprMonad
        elif type is date: cls = translator.DateExprMonad
        elif type is datetime: cls = translator.DatetimeExprMonad
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
        assert cls is translator.ConstMonad
        value_type = translator.normalize_type(type(value))
        if value_type in translator.numeric_types: cls = translator.NumericConstMonad
        elif value_type in translator.string_types: cls = translator.StringConstMonad
        elif value_type is date: cls = translator.DateConstMonad
        elif value_type is datetime: cls = translator.DatetimeConstMonad
        elif value_type is NoneType: cls = translator.NoneMonad
        else: raise TypeError, value_type
        return object.__new__(cls)
    def __init__(monad, translator, value):
        value_type = translator.normalize_type(type(value))
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
        return monad.translator.ConstMonad(monad.translator, len(monad.value))
    
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
                assert isinstance(val, translator.ObjectConstMonad)
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
        translator = monad.translator
        sql = monad.sql
        sqlop = sql[0]
        negated_op = sql_negation.get(sqlop)
        if negated_op is not None:
            negated_sql = [ negated_op ] + sql[1:]
        elif negated_op == NOT:
            assert len(sql) == 2
            negated_sql = sql[1]
        else: return translator.NotMonad(translator, sql)
        return translator.BoolExprMonad(translator, negated_sql)

cmp_ops = { '>=' : GE, '>' : GT, '<=' : LE, '<' : LT }        

cmp_negate = { '<' : '>=', '<=' : '>', '==' : '!=', 'is' : 'is not' }
cmp_negate.update((b, a) for a, b in cmp_negate.items())

class CmpMonad(BoolMonad):
    def __init__(monad, op, left, right):
        translator = left.translator
        if not translator.are_comparable_types(op, left.type, right.type): raise TypeError(
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
        BoolMonad.__init__(monad, translator)
        monad.op = op
        monad.left = left
        monad.right = right
    def negate(monad):
        return monad.translator.CmpMonad(cmp_negate[monad.op], monad.left, monad.right)
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

special_functions = SQLTranslator.special_functions = {}

def func_monad(func, type=None):
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
    translator = monad.translator
    if not isinstance(x, translator.StringConstMonad): raise TypeError
    return translator.ConstMonad(translator, Decimal(x.value))

@func_monad(date, type=date)
def FuncDateMonad(monad, year, month, day):
    translator = monad.translator
    for x, name in zip((year, month, day), ('year', 'month', 'day')):
        if not isinstance(x, translator.NumericMixin) or x.type is not int: raise TypeError(
            "'%s' argument of date(year, month, day) function must be int" % name)
        if not isinstance(x, translator.ConstMonad): raise NotImplementedError
    return translator.ConstMonad(translator, date(year.value, month.value, day.value))

@func_monad(datetime, type=datetime)
def FuncDatetimeMonad(monad, *args):
    translator = monad.translator
    for x, name in zip(args, ('year', 'month', 'day', 'hour', 'minute', 'second', 'microsecond')):
        if not isinstance(x, translator.NumericMixin) or x.type is not int: raise TypeError(
            "'%s' argument of datetime(...) function must be int" % name)
        if not isinstance(x, translator.ConstMonad): raise NotImplementedError
    return translator.ConstMonad(translator, datetime(*tuple(arg.value for arg in args)))

@func_monad(len, type=int)
def FuncLenMonad(monad, x):
    return x.len()

@func_monad(abs, type=int)
def FuncAbsMonad(monad, x):
    return x.abs()

@func_monad(sum, type=int)
def FuncSumMonad(monad, x):
    return x.sum()

@func_monad(avg, type=float)
def FuncAvgMonad(monad, x):
    return x.avg()

@func_monad(min)
def FuncMinMonad(monad, *args):
    if not args: raise TypeError
    if len(args) == 1: return args[0].min()
    return minmax(monad, MIN, *args)

@func_monad(max)
def FuncMaxMonad(monad, *args):
    if not args: raise TypeError
    if len(args) == 1: return args[0].max()
    return minmax(monad, MAX, *args)

def minmax(monad, sqlop, *args):
    assert len(args) > 1
    translator = monad.translator
    sql = [ sqlop ] + [ arg.getsql()[0] for arg in args ]
    arg_types = set(arg.type for arg in args)
    if len(arg_types) > 1: raise TypeError
    result_type = arg_types.pop()
    if result_type not in translator.comparable_types: raise TypeError
    return translator.ExprMonad(translator, result_type, sql)

@func_monad(select)
def FuncSelectMonad(monad, subquery):
    translator = monad.translator
    if not isinstance(subquery, translator.QuerySetMonad): raise TypeError
    return subquery

@func_monad(exists)
def FuncExistsMonad(monad, subquery):
    if not isinstance(subquery, monad.translator.SetMixin): raise TypeError
    return subquery.nonzero()

@func_monad(JOIN)
def JoinMonad(monad, x):
    return x

class SetMixin(MonadMixin):
    pass

class AttrSetMonad(SetMixin, Monad):
    def __init__(monad, root, path):
        if root.translator.inside_expr: raise NotImplementedError
        translator = root.translator
        item_type = translator.normalize_type(path[-1].py_type)
        Monad.__init__(monad, translator, (item_type,))
        monad.root = root
        monad.path = path
    def cmp(monad, op, monad2):
        raise NotImplementedError
    def contains(monad, item, not_in=False):
        translator = monad.translator
        item_type = monad.type[0]
        if not translator.are_comparable_types('==', item_type, item.type): raise TypeError, [item_type, item.type ]
        if isinstance(item_type, EntityMeta) and len(item_type._pk_columns_) > 1:
            raise NotImplementedError

        alias, expr, from_ast, conditions = monad._subselect()
        if expr is None:
            assert isinstance(item_type, EntityMeta)
            expr = [ COLUMN, alias, item_type._pk_columns_[0] ]
        subquery_ast = [ SELECT, [ ALL, expr ], from_ast, [ WHERE, sqland(conditions) ] ]
        sqlop = not_in and NOT_IN or IN
        return translator.BoolExprMonad(translator, [ sqlop, item.getsql()[0], subquery_ast ])
    def getattr(monad, name):
        item_type = monad.type[0]
        if not isinstance(item_type, EntityMeta):
            raise AttributeError, name
        entity = item_type
        attr = entity._adict_.get(name)
        if attr is None: raise AttributeError, name
        return monad.translator.AttrSetMonad(monad.root, monad.path + [ attr ])
    def len(monad):
        if not monad.path[-1].reverse: kind = DISTINCT
        else: kind = ALL
        alias, expr, from_ast, conditions = monad._subselect()
        sql_ast = [ SELECT, [ AGGREGATES, [ COUNT, kind, expr ] ], from_ast, [ WHERE, sqland(conditions) ] ]
        translator = monad.translator
        return translator.NumericExprMonad(translator, int, sql_ast)
    def sum(monad):
        translator = monad.translator
        item_type = monad.type[0]
        if item_type not in translator.numeric_types: raise TypeError, item_type
        alias, expr, from_ast, conditions = monad._subselect()
        sql_ast = [ SELECT, [ AGGREGATES, [COALESCE, [ SUM, expr ], [ VALUE, 0 ]]], from_ast, [ WHERE, sqland(conditions) ] ]
        return translator.NumericExprMonad(translator, item_type, sql_ast)
    def avg(monad):
        item_type = monad.type[0]
        if item_type not in translator.numeric_types: raise TypeError, item_type
        alias, expr, from_ast, conditions = monad._subselect()
        sql_ast = [ SELECT, [ AGGREGATES, [ AVG, expr ] ], from_ast, [ WHERE, sqland(conditions) ] ]
        translator = monad.translator
        return translator.NumericExprMonad(translator, float, sql_ast)
    def min(monad):
        translator = monad.translator
        item_type = monad.type[0]
        if item_type not in translator.comparable_types: raise TypeError, item_type
        alias, expr, from_ast, conditions = monad._subselect()
        sql_ast = [ SELECT, [ AGGREGATES, [ MIN, expr ] ], from_ast, [ WHERE, sqland(conditions) ] ]
        return translator.ExprMonad.new(translator, item_type, sql_ast)
    def max(monad):
        translator = monad.translator
        item_type = monad.type[0]
        if item_type not in translator.comparable_types: raise TypeError
        alias, expr, from_ast, conditions = monad._subselect()
        sql_ast = [ SELECT, [ AGGREGATES, [ MAX, expr ] ], from_ast, [ WHERE, sqland(conditions) ] ]
        return translator.ExprMonad.new(monad.translator, item_type, sql_ast)
    def nonzero(monad):
        alias, expr, from_ast, conditions = monad._subselect()
        sql_ast = [ EXISTS, from_ast, [ WHERE, sqland(conditions) ] ]
        translator = monad.translator
        return translator.BoolExprMonad(translator, sql_ast)
    def negate(monad):
        alias, expr, from_ast, conditions = monad._subselect()
        sql_ast = [ NOT_EXISTS, from_ast, [ WHERE, sqland(conditions) ] ]
        translator = monad.translator
        return translator.BoolExprMonad(translator, sql_ast)
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
            assert isinstance(next_entity, EntityMeta)
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
        monad.translator = translator
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
        return attr, sub.normalize_type(attr.py_type)
    def contains(monad, item, not_in=False):
        translator = monad.translator
        item_type = monad.type[0]
        if not translator.are_comparable_types('==', item_type, item.type): raise TypeError, [item_type, item.type ]
        if isinstance(item_type, EntityMeta) and len(item_type._pk_columns_) > 1:
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
        return translator.BoolExprMonad(translator, [ sqlop, item.getsql()[0], subquery_ast ])
    def nonzero(monad):        
        sub = monad.subtranslator
        sql_ast = [ EXISTS, sub.from_, [ WHERE, sqland(sub.conditions) ] ]
        translator = monad.translator
        return translator.BoolExprMonad(translator, sql_ast)
    def negate(monad):
        sub = monad.subtranslator
        sql_ast = [ NOT_EXISTS, sub.from_, [ WHERE, sqland(sub.conditions) ] ]
        translator = monad.translator
        return translator.BoolExprMonad(translator, sql_ast)
    def _subselect(monad, item_type, select_ast):
        sub = monad.subtranslator
        sql_ast = [ SELECT, select_ast, sub.from_, [ WHERE, sqland(sub.conditions) ] ]
        translator = monad.translator
        return translator.ExprMonad.new(translator, item_type, sql_ast)
    def len(monad):
        attr, attr_type = monad._get_attr_info()
        if attr is not None:
            if len(attr.columns) > 1: raise NotImplementedError
            select_ast = [ AGGREGATES, [ COUNT, DISTINCT, [ COLUMN, monad.subtranslator.alias, attr.column ] ] ]
        else: select_ast = [ AGGREGATES, [ COUNT, ALL ] ]
        return monad._subselect(int, select_ast)
    def sum(monad):
        translator = monad.translator
        attr, attr_type = monad._get_attr_info()
        if attr_type not in translator.numeric_types: raise TypeError, attr_type
        select_ast = [ AGGREGATES, [ COALESCE, [ SUM, [ COLUMN, monad.subtranslator.alias, attr.column ] ], [ VALUE, 0 ] ] ]
        return monad._subselect(attr_type, select_ast)
    def avg(monad):
        attr, attr_type = monad._get_attr_info()
        if attr_type not in translator.numeric_types: raise TypeError, attr_type
        select_ast = [ AGGREGATES, [ AVG, [ COLUMN, monad.subtranslator.alias, attr.column ] ] ]
        return monad._subselect(float, select_ast)
    def min(monad):
        translator = monad.translator
        attr, attr_type = monad._get_attr_info()
        if attr_type not in translator.comparable_types: raise TypeError, attr_type
        select_ast = [ AGGREGATES, [ MIN, [ COLUMN, monad.subtranslator.alias, attr.column ] ] ]
        return monad._subselect(attr_type, select_ast)
    def max(monad):
        translator = monad.translator
        attr, attr_type = monad._get_attr_info()
        if attr_type not in translator.comparable_types: raise TypeError
        select_ast = [ AGGREGATES, [ MAX, [ COLUMN, monad.subtranslator.alias, attr.column ] ] ]
        return monad._subselect(attr_type, select_ast)

for name, value in globals().items():
    if name.endswith('Monad') or name.endswith('Mixin'):
        setattr(SQLTranslator, name, value)
del name, value
