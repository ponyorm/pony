import __builtin__, types, sys, decimal, re
from itertools import izip, count
from types import NoneType
from compiler import ast
from decimal import Decimal
from datetime import date, datetime

from pony import options
from pony.dbapiprovider import LongStr, LongUnicode
from pony.utils import avg, copy_func_attrs, is_ident, throw
from pony.orm import query, exists, ERDiagramError, TranslationError, \
                     EntityMeta, Set, JOIN, AsciiStr

class IncomparableTypesError(TypeError):
    def __init__(exc, type1, type2):
        msg = 'Incomparable types %r and %r in expression: {EXPR}' % (type2str(type1), type2str(type2))
        TypeError.__init__(exc, msg)
        exc.type1 = type1
        exc.type2 = type2    

def sqland(items):
    if not items: return []
    if len(items) == 1: return items[0]
    result = [ 'AND' ]
    for item in items:
        if item[0] == 'AND': result.extend(item[1:])
        else: result.append(item)
    return result

def sqlor(items):
    if not items: return []
    if len(items) == 1: return items[0]
    result = [ 'OR' ]
    for item in items:
        if item[0] == 'OR': result.extend(item[1:])
        else: result.append(item)
    return result

def join_tables(alias1, alias2, columns1, columns2):
    assert len(columns1) == len(columns2)
    return sqland([ [ 'EQ', [ 'COLUMN', alias1, c1 ], [ 'COLUMN', alias2, c2 ] ] for c1, c2 in izip(columns1, columns2) ])

class ASTTranslator(object):
    def __init__(translator, tree):
        translator.tree = tree
        translator.pre_methods = {}
        translator.post_methods = {}
    def dispatch(translator, node):
        cls = node.__class__

        try: pre_method = translator.pre_methods[cls]
        except KeyError:
            pre_method = getattr(translator, 'pre' + cls.__name__, translator.default_pre)
            translator.pre_methods[cls] = pre_method
        stop = translator.call(pre_method, node)

        if stop: return
            
        for child in node.getChildNodes():
            translator.dispatch(child)

        try: post_method = translator.post_methods[cls]
        except KeyError:
            post_method = getattr(translator, 'post' + cls.__name__, translator.default_post)
            translator.post_methods[cls] = post_method
        translator.call(post_method, node)
    def call(translator, method, node):
        return method(node)
    def default_pre(translator, node):
        pass
    def default_post(translator, node):
        pass

def priority(p):
    def decorator(func):
        def new_func(translator, node):
            node.priority = p
            for child in node.getChildNodes():
                if getattr(child, 'priority', 0) >= p: child.src = '(%s)' % child.src
            return func(translator, node)
        return copy_func_attrs(new_func, func)
    return decorator

def binop_src(op, node):
    return op.join((node.left.src, node.right.src))

def ast2src(tree):
    try: PythonTranslator(tree)
    except NotImplementedError: return repr(tree)
    return tree.src

class PythonTranslator(ASTTranslator):
    def __init__(translator, tree):
        ASTTranslator.__init__(translator, tree)
        translator.dispatch(tree)
    def call(translator, method, node):
        node.src = method(node)
    def default_post(translator, node):
        throw(NotImplementedError, node)
    def postGenExpr(translator, node):
        return '(%s)' % node.code.src        
    def postGenExprInner(translator, node):
        return node.expr.src + ' ' + ' '.join(qual.src for qual in node.quals)
    def postGenExprFor(translator, node):
        src = 'for %s in %s' % (node.assign.src, node.iter.src)
        if node.ifs:
            ifs = ' '.join(if_.src for if_ in node.ifs)
            src += ' ' + ifs
        return src
    def postGenExprIf(translator, node):
        return 'if %s' % node.test.src 
    @priority(14)
    def postOr(translator, node):
        return ' or '.join(expr.src for expr in node.nodes)
    @priority(13)
    def postAnd(translator, node):
        return ' and '.join(expr.src for expr in node.nodes)
    @priority(12)
    def postNot(translator, node):
        return 'not ' + node.expr.src
    @priority(11)
    def postCompare(translator, node):
        result = [ node.expr.src ]
        for op, expr in node.ops: result.extend((op, expr.src))
        return ' '.join(result)
    @priority(10)
    def postBitor(translator, node):
        return ' | '.join(expr.src for expr in node.nodes)
    @priority(9)
    def postBitxor(translator, node):
        return ' ^ '.join(expr.src for expr in node.nodes)
    @priority(8)
    def postBitand(translator, node):
        return ' & '.join(expr.src for expr in node.nodes)
    @priority(7)
    def postLeftShift(translator, node):
        return binop_src(' << ', node)
    @priority(7)
    def postRightShift(translator, node):
        return binop_src(' >> ', node)
    @priority(6)
    def postAdd(translator, node):
        return binop_src(' + ', node)
    @priority(6)
    def postSub(translator, node):
        return binop_src(' - ', node)
    @priority(5)
    def postMul(translator, node):
        return binop_src(' * ', node)
    @priority(5)
    def postDiv(translator, node):
        return binop_src(' / ', node)
    @priority(5)
    def postMod(translator, node):
        return binop_src(' % ', node)
    @priority(4)
    def postUnarySub(translator, node):
        return '-' + node.expr.src
    @priority(4)
    def postUnaryAdd(translator, node):
        return '+' + node.expr.src
    @priority(4)
    def postInvert(translator, node):
        return '~' + node.expr.src
    @priority(3)
    def postPower(translator, node):
        return binop_src(' ** ', node)
    def postGetattr(translator, node):
        node.priority = 2
        return '.'.join((node.expr.src, node.attrname))
    def postCallFunc(translator, node):
        node.priority = 2
        args = [ arg.src for arg in node.args ]
        if node.star_args: args.append('*'+node.star_args.src)
        if node.dstar_args: args.append('**'+node.dstar_args.src)
        if len(args) == 1 and isinstance(node.args[0], ast.GenExpr):
            return node.node.src + args[0]
        return '%s(%s)' % (node.node.src, ', '.join(args))
    def postSubscript(translator, node):
        node.priority = 2
        if len(node.subs) == 1:
            sub = node.subs[0]
            if isinstance(sub, ast.Const) and type(sub.value) is tuple and len(sub.value) > 1:
                key = sub.src
                assert key.startswith('(') and key.endswith(')')
                key = key[1:-1]
            else: key = sub.src
        else: key = ', '.join([ sub.src for sub in node.subs ])
        return '%s[%s]' % (node.expr.src, key)
    def postSlice(translator, node):
        node.priority = 2
        return '%s[%s:%s]' % (node.expr.src, node.lower.src, node.upper.src)
    def postSliceobj(translator, node):
        return ':'.join(item.src for item in node.nodes)
    def postConst(translator, node):
        node.priority = 1
        return repr(node.value)
    def postList(translator, node):
        node.priority = 1
        return '[%s]' % ', '.join(item.src for item in node.nodes)
    def postTuple(translator, node):
        node.priority = 1
        if len(node.nodes) == 1: return '(%s,)' % node.nodes[0].src
        else: return '(%s)' % ', '.join(item.src for item in node.nodes)
    def postAssTuple(translator, node):
        node.priority = 1
        if len(node.nodes) == 1: return '(%s,)' % node.nodes[0].src
        else: return '(%s)' % ', '.join(item.src for item in node.nodes)
    def postDict(translator, node):
        node.priority = 1
        return '{%s}' % ', '.join('%s:%s' % (key.src, value.src) for key, value in node.items)
    def postSet(translator, node):
        node.priority = 1
        return '{%s}' % ', '.join(item.src for item in node.nodes)
    def postBackquote(translator, node):
        node.priority = 1
        return '`%s`' % node.expr.src
    def postName(translator, node):
        node.priority = 1
        return node.name
    def postAssName(translator, node):
        node.priority = 1
        return node.name
    def postKeyword(translator, node):
        return '='.join((node.name, node.expr.src))

class SetType(object):
    __slots__ = 'item_type'
    def __init__(self, item_type):
        self.item_type = item_type
    def __eq__(self, other):
        return type(other) is SetType and self.item_type == other.item_type
    def __ne__(self, other):
        return type(other) is not SetType and self.item_type != other.item_type
    def __hash__(self):
        return hash(self.item_type) + 1

def type2str(t):
    if isinstance(t, tuple): return 'list'
    if type(t) is SetType: return 'Set of ' + type2str(t.item_type)
    try: return t.__name__
    except: return str(t)

type_normalization_dict = { long : int, bool : int, LongStr : str, LongUnicode : unicode }

class SQLTranslator(ASTTranslator):
    dialect = None
    row_value_syntax = True
    numeric_types = set([ int, float, Decimal ])
    string_types = set([ str, AsciiStr, unicode ])
    comparable_types = set([ int, float, Decimal, str, AsciiStr, unicode, date, datetime, bool ])
    primitive_types = set([ int, float, Decimal, str, AsciiStr, unicode, date, datetime, bool, buffer ])

    def default_post(translator, node):
        throw(NotImplementedError)

    def call(translator, method, node):
        try: monad = method(node)
        except Exception:
            try:
                exc_class, exc, tb = sys.exc_info()
                if not exc.args: exc.args = (ast2src(node),)
                else:
                    msg = exc.args[0]
                    if isinstance(msg, basestring) and '{EXPR}' in msg:
                        msg = msg.replace('{EXPR}', ast2src(node))
                        exc.args = (msg,) + exc.args[1:]
                raise exc_class, exc, tb
            finally: del tb
        else:
            if monad is None: return
            node.monad = monad
            monad.node = node
            if not hasattr(monad, 'aggregated'):
                for child in node.getChildNodes():
                    m = getattr(child, 'monad', None) 
                    if m and getattr(m, 'aggregated', False):
                        monad.aggregated = True
                        break
                else: monad.aggregated = False
            return monad

    @classmethod
    def get_normalized_type_of(translator, value):
        if isinstance(value, str):
            try: value.decode('ascii')
            except UnicodeDecodeError: pass
            else: return AsciiStr
        elif isinstance(value, unicode):
            try: value.encode('ascii')
            except UnicodeEncodeError: pass
            else: return AsciiStr
        return translator.normalize_type(type(value))

    @classmethod
    def normalize_type(translator, type):
        if type is NoneType: return type
        if issubclass(type, basestring):  # Mainly for Html -> unicode & StrHtml -> str conversion
            if type in (str, AsciiStr, unicode): return type
            if issubclass(type, str): return str
            if issubclass(type, unicode): return unicode
            assert False
        type = type_normalization_dict.get(type, type)
        if type not in translator.primitive_types and not isinstance(type, EntityMeta): throw(TypeError, type)
        return type

    coercions = {
        (int, float) : float,
        (int, Decimal) : Decimal,
        (date, datetime) : datetime,
        (AsciiStr, str) : str,
        (AsciiStr, unicode) : unicode
        }
    coercions.update(((t2, t1), t3) for ((t1, t2), t3) in coercions.items())

    @classmethod
    def coerce_types(translator, type1, type2):
        if type1 is type2: return type1
        return translator.coercions.get((type1, type2))

    @classmethod
    def check_comparable(translator, left_monad, right_monad, op='=='):
        t1, t2 = left_monad.type, right_monad.type
        if t1 == 'METHOD': raise_forgot_parentheses(left_monad)
        if t2 == 'METHOD': raise_forgot_parentheses(right_monad)
        if not translator.are_comparable_types(t1, t2, op):
            if op in ('in', 'not in') and isinstance(t2, SetType): t2 = t2.item_type
            throw(IncomparableTypesError, t1, t2)

    @classmethod
    def are_comparable_types(translator, type1, type2, op='=='):
        # types must be normalized already!
        if op in ('in', 'not in'):
            if not isinstance(type2, SetType): return False
            op = '=='
            type2 = type2.item_type
        if op in ('is', 'is not'):
            return type1 is not NoneType and type2 is NoneType
        if isinstance(type1, tuple):
            if not isinstance(type2, tuple): return False
            if len(type1) != len(type2): return False
            for item1, item2 in izip(type1, type2):
                if not translator.are_comparable_types(item1, item2): return False
            return True
        if op in ('==', '<>', '!='):
            if type1 is NoneType and type2 is NoneType: return False
            if type1 is NoneType or type2 is NoneType: return True
            if type1 in translator.primitive_types:
                if type1 is type2: return True
                if (type1, type2) in translator.coercions: return True
                if not isinstance(type1, type) or not isinstance(type2, type): return False
                if issubclass(type1, (int, long)) and issubclass(type2, basestring): return True
                if issubclass(type2, (int, long)) and issubclass(type1, basestring): return True
                return False
            if isinstance(type1, EntityMeta):
                if not isinstance(type2, EntityMeta): return False
                return type1._root_ is type2._root_
            return False
        if type1 is type2 and type1 in translator.comparable_types: return True
        return (type1, type2) in translator.coercions

    def __init__(translator, tree, databases, entities, vartypes, functions, parent_translator=None):
        assert isinstance(tree, ast.GenExprInner), tree
        ASTTranslator.__init__(translator, tree)
        translator.database = None
        translator.databases = databases
        translator.entities = entities
        translator.vartypes = vartypes
        translator.functions = functions
        if not parent_translator: subquery = Subquery()
        else: subquery = Subquery(parent_translator.subquery)
        translator.subquery = subquery
        tablerefs = subquery.tablerefs
        translator.parent = parent_translator
        translator.extractors = {}
        translator.distinct = False
        translator.where = None
        translator.conditions = subquery.conditions
        translator.groupby = None
        translator.having = None
        translator.having_conditions = []
        translator.aggregated = False
        translator.inside_expr = False
        translator.inside_not = False
        translator.hint_join = False
        for i, qual in enumerate(tree.quals):
            assign = qual.assign
            if not isinstance(assign, ast.AssName): throw(NotImplementedError, ast2src(assign))
            if assign.flags != 'OP_ASSIGN': throw(TypeError, ast2src(assign))

            name = assign.name
            if name in tablerefs: throw(TranslationError, 'Duplicate name: %r' % name)
            if name.startswith('__'): throw(TranslationError, 'Illegal name: %r' % name)
            assert name not in tablerefs

            node = qual.iter
            attr_names = []
            while isinstance(node, ast.Getattr):
                attr_names.append(node.attrname)
                node = node.expr
            if not isinstance(node, ast.Name): throw(TypeError, ast2src(node))
            node_name = node.name

            if node_name in databases:
                db_name = node_name
                db = databases[db_name]
                if not attr_names: throw(TypeError, 'Entity name is not specified after database name %r' % db_name)
                entity_name = attr_names[0]
                try: entity = getattr(db, entity_name)
                except AttributeError: throw(AttributeError, 
                    'Entity %r is not found in database %r' % (entity_name, db_name))
                entity_name = db_name + '.' + entity_name
                entity2 = entities.setdefault(entity_name, entity)
                node_name = entity_name
                assert entity2 is entity
                attr_names.pop(0)

            if not attr_names:
                if i > 0: translator.distinct = True
                entity = entities.get(node_name)
                if entity is None: throw(TranslationError, ast2src(qual.iter))
                tablerefs[name] = TableRef(subquery, name, entity)
            else:
                attr_names.reverse()
                name_path = node_name
                parent_tableref = subquery.get_tableref(node_name)
                if parent_tableref is None: throw(TranslationError, "Name %r must be defined in query" % node_name)
                parent_entity = parent_tableref.entity
                last_index = len(attr_names) - 1
                for j, attrname in enumerate(attr_names):
                    attr = parent_entity._adict_.get(attrname)
                    if attr is None: throw(AttributeError, attrname)
                    entity = attr.py_type
                    if not isinstance(entity, EntityMeta):
                        throw(NotImplementedError, 'for %s in %s' % (name, ast2src(qual.iter)))
                    if attr.is_collection:
                        if not isinstance(attr, Set): throw(NotImplementedError, ast2src(qual.iter))
                        reverse = attr.reverse
                        if reverse.is_collection:
                            if not isinstance(reverse, Set): throw(NotImplementedError, ast2src(qual.iter))
                            translator.distinct = True
                        elif parent_tableref.alias != tree.quals[i-1].assign.name:
                            translator.distinct = True
                    if j == last_index: name_path = name
                    else: name_path += '-' + attr.name
                    tableref = JoinedTableRef(subquery, name_path, parent_tableref, attr)
                    tablerefs[name_path] = tableref
                    parent_tableref = tableref
                    parent_entity = entity

            database = entity._database_
            if database.schema is None: throw(ERDiagramError, 
                'Mapping is not generated for entity %r' % entity.__name__)
            if translator.database is None: translator.database = database
            elif translator.database is not database: throw(TranslationError, 
                'All entities in a query must belong to the same database')

            for if_ in qual.ifs:
                assert isinstance(if_, ast.GenExprIf)
                translator.dispatch(if_)
                if isinstance(if_.monad, translator.AndMonad): cond_monads = if_.monad.operands
                else: cond_monads = [ if_.monad ]
                for m in cond_monads:
                    if not m.aggregated: translator.conditions.append(m.getsql())
                    else: translator.having_conditions.append(m.getsql())
                
        translator.inside_expr = True
        translator.dispatch(tree.expr)
        assert not translator.hint_join
        assert not translator.inside_not
        monad = tree.expr.monad
        if isinstance(monad, translator.ParamMonad): throw(TranslationError,
            "External parameter '%s' cannot be used as query result" % ast2src(tree.expr))
        groupby_monads = None
        expr_type = monad.type
        if isinstance(expr_type, SetType): expr_type = expr_type.item_type
        if isinstance(expr_type, EntityMeta):
            if monad.aggregated: throw(TranslationError)
            if translator.aggregated: groupby_monads = [ monad ]
            else: translator.distinct |= monad.requires_distinct()
            if isinstance(monad, translator.ObjectMixin):
                entity = monad.type
                tableref = monad.tableref
            elif isinstance(monad, translator.AttrSetMonad):
                entity = monad.type.item_type
                tableref = monad.make_tableref(translator.subquery)
            else: assert False
            translator.tableref = tableref
            alias, pk_columns = tableref.make_join(pk_only=parent_translator is not None)
            translator.alias = alias
            translator.expr_type = entity
            translator.expr_columns = [ [ 'COLUMN', alias, column ] for column in pk_columns ]
            translator.row_layout = None
            discr_criteria = entity._construct_discriminator_criteria_()
            if discr_criteria: translator.conditions.insert(0, discr_criteria)
        else:
            translator.alias = None
            if isinstance(monad, translator.ListMonad):
                expr_monads = monad.items
                translator.expr_type = tuple(m.type for m in expr_monads)  # ?????
                expr_columns = []
                for m in expr_monads: expr_columns.extend(m.getsql())
                translator.expr_columns = expr_columns
            else:
                expr_monads = [ monad ]
                translator.expr_type = monad.type
                translator.expr_columns = monad.getsql()
            if translator.aggregated:
                groupby_monads = [ m for m in expr_monads if not m.aggregated ]
            else: translator.distinct = True
            row_layout = []
            offset = 0
            provider = translator.database.provider
            for m in expr_monads:
                expr_type = m.type
                if isinstance(expr_type, SetType): expr_type = expr_type.item_type
                if isinstance(expr_type, EntityMeta):
                    next_offset = offset + len(expr_type._pk_columns_)
                    def func(values, constructor=expr_type._get_by_raw_pkval_):
                        if None in values: return None
                        return constructor(values)
                    row_layout.append((func, slice(offset, next_offset)))
                    offset = next_offset
                else:
                    converter = provider.get_converter_by_py_type(expr_type)
                    def func(value, sql2py=converter.sql2py):
                        if value is None: return None
                        return sql2py(value)
                    row_layout.append((func, offset))
                    offset += 1
            translator.row_layout = row_layout

        if groupby_monads:
            translator.groupby = [ 'GROUP_BY' ]
            for m in groupby_monads: translator.groupby.extend(m.getsql())

        if translator.having_conditions:
            if not translator.groupby: throw(TranslationError,
                'In order to use aggregated functions sucn as SUM(), COUNT(), etc., '
                'query must have grouping columns (i.e. resulting non-aggregated values)')
            translator.having = [ 'HAVING' ] + translator.having_conditions

        first_from_item = translator.subquery.from_ast[1]
        if len(first_from_item) > 3:
            assert len(first_from_item) == 4
            assert parent_translator
            join_condition = first_from_item.pop()
            translator.conditions.insert(0, join_condition)
        translator.where = [ 'WHERE' ] + translator.conditions
    def preGenExpr(translator, node):
        inner_tree = node.code
        subtranslator = translator.__class__(inner_tree, translator.databases, translator.entities, translator.vartypes, translator.functions, translator)
        return translator.QuerySetMonad(translator, subtranslator)
    def postGenExprIf(translator, node):
        monad = node.test.monad
        if monad.type is not bool: monad = monad.nonzero()
        return monad
    def preCompare(translator, node):
        monads = []
        ops = node.ops
        left = node.expr
        translator.dispatch(left)
        inside_not = translator.inside_not
        # op: '<' | '>' | '=' | '>=' | '<=' | '<>' | '!=' | '=='
        #         | 'in' | 'not in' | 'is' | 'is not'
        for op, right in node.ops:
            translator.inside_not = inside_not
            if op == 'not in': translator.inside_not = not inside_not
            translator.dispatch(right)
            if op.endswith('in'): monad = right.monad.contains(left.monad, op == 'not in')
            else: monad = left.monad.cmp(op, right.monad)
            monad.aggregated = getattr(left.monad, 'aggregated', False) or getattr(right.monad, 'aggregated', False)
            monads.append(monad)
            left = right
        translator.inside_not = inside_not
        if len(monads) == 1: return monads[0]
        return translator.AndMonad(monads)
    def postConst(translator, node):
        value = node.value
        if type(value) is not tuple:
            return translator.ConstMonad.new(translator, value)
        else:
            return translator.ListMonad(translator, [ translator.ConstMonad.new(translator, item) for item in value ])
    def postList(translator, node):
        return translator.ListMonad(translator, [ item.monad for item in node.nodes ])
    def postTuple(translator, node):
        return translator.ListMonad(translator, [ item.monad for item in node.nodes ])
    def postName(translator, node):
        name = node.name
        tableref = translator.subquery.get_tableref(name)
        if tableref is not None:
            entity = tableref.entity
            return translator.ObjectIterMonad(translator, tableref, entity)

        database = translator.databases.get(name)
        if database is not None:
            return translator.DatabaseMonad(translator, database)

        entity = translator.entities.get(name)
        if entity is not None:
            return translator.EntityMonad(translator, entity)
            
        try: value_type = translator.vartypes[name]
        except KeyError:
            func = translator.functions.get(name)
            if func is None: throw(NameError, name)
            func_monad_class = special_functions[func]
            return func_monad_class(translator)
        else:
            if name in ('True', 'False') and issubclass(value_type, int):
                return translator.ConstMonad.new(translator, name == 'True' and 1 or 0)
            elif value_type is NoneType: return translator.ConstMonad.new(translator, None)
            else: return translator.ParamMonad.new(translator, value_type, name)
    def postAdd(translator, node):
        return node.left.monad + node.right.monad
    def postSub(translator, node):
        return node.left.monad - node.right.monad
    def postMul(translator, node):
        return node.left.monad * node.right.monad
    def postDiv(translator, node):
        return node.left.monad / node.right.monad
    def postPower(translator, node):
        return node.left.monad ** node.right.monad
    def postUnarySub(translator, node):
        return -node.expr.monad
    def postGetattr(translator, node):
        return node.expr.monad.getattr(node.attrname)
    def postAnd(translator, node):
        return translator.AndMonad([ subnode.monad for subnode in node.nodes ])
    def postOr(translator, node):
        return translator.OrMonad([ subnode.monad for subnode in node.nodes ])
    def preNot(translator, node):
        translator.inside_not = not translator.inside_not
    def postNot(translator, node):
        translator.inside_not = not translator.inside_not
        return node.expr.monad.negate()
    def preCallFunc(translator, node):
        if node.star_args is not None: throw(NotImplementedError, '*%s is not supported' % ast2src(node.star_args))
        if node.dstar_args is not None: throw(NotImplementedError, '**%s is not supported' % ast2src(node.dstar_args))
        if not isinstance(node.node, (ast.Name, ast.Getattr)): throw(NotImplementedError)
        if len(node.args) > 1: return
        if not node.args: return
        arg = node.args[0]
        if isinstance(arg, ast.GenExpr):
            translator.dispatch(node.node)
            func_monad = node.node.monad
            translator.dispatch(arg)
            query_set_monad = arg.monad
            return func_monad(query_set_monad)
        if not isinstance(arg, ast.Lambda): return
        lambda_expr = arg
        if not isinstance(node.node, ast.Getattr): throw(NotImplementedError)
        expr = node.node.expr
        translator.dispatch(expr)
        if not isinstance(expr.monad, translator.EntityMonad): throw(NotImplementedError)
        entity = expr.monad.type
        for n, v in translator.entities.iteritems():
            if entity is v: entity_name = n; break
        else: assert False
        if node.node.attrname != 'query': throw(TypeError)
        if len(lambda_expr.argnames) != 1: throw(TypeError)
        if lambda_expr.varargs: throw(TypeError)
        if lambda_expr.kwargs: throw(TypeError)
        if lambda_expr.defaults: throw(TypeError)
        name = lambda_expr.argnames[0]            
        cond_expr = lambda_expr.code
        if_expr = ast.GenExprIf(cond_expr)
        for_expr = ast.GenExprFor(ast.AssName(name, 'OP_ASSIGN'), ast.Name(entity_name), [ if_expr ])
        inner_expr = ast.GenExprInner(ast.Name(name), [ for_expr ])
        subtranslator = translator.__class__(inner_expr, translator.databases, translator.entities, translator.vartypes, translator.functions, translator)
        return translator.QuerySetMonad(translator, subtranslator)
    def postCallFunc(translator, node):
        args = []
        keyargs = {}
        for arg in node.args:
            if isinstance(arg, ast.Keyword):
                keyargs[arg.name] = arg.expr.monad
            else: args.append(arg.monad)
        func_monad = node.node.monad
        return func_monad(*args, **keyargs)
    def postKeyword(translator, node):
        pass  # this node will be processed by postCallFunc
    def postSubscript(translator, node):
        assert node.flags == 'OP_APPLY'
        assert isinstance(node.subs, list)
        if len(node.subs) > 1:
            for x in node.subs:
                if isinstance(x, ast.Sliceobj): throw(TypeError)
            key = translator.ListMonad(translator, [ item.monad for item in node.subs ])
            return node.expr.monad[key]
        sub = node.subs[0]
        if isinstance(sub, ast.Sliceobj):
            start, stop, step = (sub.nodes+[None])[:3]
            return node.expr.monad[start:stop:step]
        else: return node.expr.monad[sub.monad]
    def postSlice(translator, node):
        assert node.flags == 'OP_APPLY'
        expr_monad = node.expr.monad
        upper = node.upper
        if upper is not None: upper = upper.monad
        lower = node.lower
        if lower is not None: lower = lower.monad
        return expr_monad[lower:upper]
    def postSliceobj(translator, node):
        pass

max_alias_length = 30

class Subquery(object):
    def __init__(subquery, parent_subquery=None):
        subquery.parent_subquery = parent_subquery
        subquery.from_ast = [ 'FROM' ]
        subquery.conditions = []
        subquery.tablerefs = {}
        if parent_subquery is None:
            subquery.alias_counters = {}
            subquery.expr_counter = count(1).next
        else:
            subquery.alias_counters = parent_subquery.alias_counters
            subquery.expr_counter = parent_subquery.expr_counter
    def get_tableref(subquery, name_path):
        tableref = subquery.tablerefs.get(name_path)
        if tableref is not None: return tableref
        if subquery.parent_subquery:
            return subquery.parent_subquery.get_tableref(name_path)
        return None
    def add_tableref(subquery, name_path, parent_tableref, attr):
        tablerefs = subquery.tablerefs
        assert name_path not in tablerefs
        tableref = JoinedTableRef(subquery, name_path, parent_tableref, attr)
        tablerefs[name_path] = tableref
        return tableref
    def get_short_alias(subquery, name_path, entity_name):
        if name_path:
            if is_ident(name_path): return name_path
            if not options.SIMPLE_ALIASES and len(name_path) <= max_alias_length:
                return name_path
        name = entity_name[:max_alias_length-3].lower()
        i = subquery.alias_counters.setdefault(name, 0) + 1
        alias = '%s-%d' % (name, i)
        subquery.alias_counters[name] = i
        return alias

class TableRef(object):
    def __init__(tableref, subquery, name, entity):
        tableref.subquery = subquery
        tableref.alias = tableref.name_path = name
        tableref.entity = entity
        tableref.joined = False
    def make_join(tableref, pk_only=False):
        if not tableref.joined:
            tableref.subquery.from_ast.append([ tableref.alias, 'TABLE', tableref.entity._table_ ])
            tableref.joined = True
        return tableref.alias, tableref.entity._pk_columns_

class JoinedTableRef(object):
    def __init__(tableref, subquery, name_path, parent_tableref, attr):
        tableref.subquery = subquery
        tableref.name_path = name_path
        tableref.alias = None
        tableref.optimized = None
        tableref.parent_tableref = parent_tableref
        tableref.attr = attr
        tableref.entity = attr.py_type
        assert isinstance(tableref.entity, EntityMeta)
        tableref.joined = False
    def make_join(tableref, pk_only=False):
        if tableref.joined:
            if pk_only or not tableref.optimized:
                return tableref.alias, tableref.pk_columns
        attr = tableref.attr
        parent_pk_only = attr.pk_offset is not None or attr.is_collection
        parent_alias, left_pk_columns = tableref.parent_tableref.make_join(parent_pk_only)
        left_entity = attr.entity
        right_entity = attr.py_type
        pk_columns = right_entity._pk_columns_
        if not attr.is_collection:
            if not attr.columns:
                reverse = attr.reverse
                assert reverse.columns and not reverse.is_collection
                alias = tableref.subquery.get_short_alias(tableref.name_path, right_entity.__name__)
                join_cond = join_tables(parent_alias, alias, left_pk_columns, reverse.columns)
            else:
                if attr.pk_offset is not None:
                    offset = attr.pk_columns_offset
                    left_columns = left_pk_columns[offset:offset+len(attr.columns)]
                else: left_columns = attr.columns
                if pk_only:
                    tableref.alias = parent_alias
                    tableref.pk_columns = left_columns
                    tableref.optimized = True
                    tableref.joined = True
                    return parent_alias, left_columns
                alias = tableref.subquery.get_short_alias(tableref.name_path, right_entity.__name__)
                join_cond = join_tables(parent_alias, alias, left_columns, pk_columns)
            tableref.subquery.from_ast.append([ alias, 'TABLE', right_entity._table_, join_cond ])
        elif not attr.reverse.is_collection:
            alias = tableref.subquery.get_short_alias(tableref.name_path, right_entity.__name__)
            join_cond = join_tables(parent_alias, alias, left_pk_columns, attr.reverse.columns)
            tableref.subquery.from_ast.append([ alias, 'TABLE', right_entity._table_, join_cond ])
        else:
            right_m2m_columns = attr.symmetric and attr.reverse_columns or attr.columns
            if not tableref.joined:
                m2m_table = attr.table
                m2m_alias = tableref.subquery.get_short_alias(None, 't')
                reverse_columns = attr.symmetric and attr.columns or attr.reverse.columns
                m2m_join_cond = join_tables(parent_alias, m2m_alias, left_pk_columns, reverse_columns)
                tableref.subquery.from_ast.append([ m2m_alias, 'TABLE', m2m_table, m2m_join_cond ])
                if pk_only:
                    tableref.alias = m2m_alias
                    tableref.pk_columns = right_m2m_columns
                    tableref.optimized = True
                    tableref.joined = True
                    return m2m_alias, tableref.pk_columns
            elif tableref.optimized:
                assert not pk_only
                m2m_alias = tableref.alias
            alias = tableref.subquery.get_short_alias(tableref.name_path, right_entity.__name__)
            join_cond = join_tables(m2m_alias, alias, right_m2m_columns, pk_columns)
            tableref.subquery.from_ast.append([ alias, 'TABLE', right_entity._table_, join_cond ])
        tableref.alias = alias 
        tableref.pk_columns = pk_columns
        tableref.optimized = False
        tableref.joined = True
        return tableref.alias, pk_columns

def wrap_monad_method(cls_name, func):
    overrider_name = '%s_%s' % (cls_name, func.__name__)
    def wrapper(monad, *args, **keyargs):
        method = getattr(monad.translator, overrider_name, func)
        return method(monad, *args, **keyargs)
    return copy_func_attrs(wrapper, func)

class MonadMeta(type):
    def __new__(meta, cls_name, bases, cls_dict):
        for name, func in cls_dict.items():
            if not isinstance(func, types.FunctionType): continue
            if name in ('__new__', '__init__'): continue
            cls_dict[name] = wrap_monad_method(cls_name, func)
        return super(MonadMeta, meta).__new__(meta, cls_name, bases, cls_dict)

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
    def contains(monad, item, not_in=False): throw(TypeError)
    def nonzero(monad): throw(TypeError)
    def negate(monad):
        return monad.translator.NotMonad(monad)
    def getattr(monad, attrname):
        try: property_method = getattr(monad, 'attr_' + attrname)
        except AttributeError:
            if not hasattr(monad, 'call_' + attrname):
                throw(AttributeError, '%r object has no attribute %r' % (type2str(monad.type), attrname))
            translator = monad.translator
            return translator.MethodMonad(translator, monad, attrname)
        return property_method()
    def len(monad): throw(TypeError)
    def count(monad):
        translator = monad.translator
        translator.aggregated = True
        if monad.aggregated: throw(TranslationError, 'Aggregated functions cannot be nested. Got: {EXPR}')
        expr = monad.getsql()
        count_kind = 'DISTINCT'
        if monad.type is bool:
            expr = [ 'CASE', None, [ [ expr, [ 'VALUE', 1 ] ] ], [ 'VALUE', None ] ]
            count_kind = 'ALL'
        elif len(expr) == 1: expr = expr[0]
        elif translator.row_value_syntax == True: expr = ['ROW'] + expr
        elif translator.dialect == 'SQLite':
            alias, pk_columns = monad.tableref.make_join(pk_only=False)
            expr = [ 'COLUMN', alias, 'ROWID' ]
        else: throw(NotImplementedError)
        result = translator.ExprMonad.new(translator, int, [ 'COUNT', count_kind, expr ])
        result.aggregated = True
        return result
    def aggregate(monad, func_name):
        translator = monad.translator
        translator.aggregated = True
        if monad.aggregated: throw(TranslationError, 'Aggregated functions cannot be nested. Got: {EXPR}')
        expr_type = monad.type
        # if isinstance(expr_type, SetType): expr_type = expr_type.item_type
        if func_name in ('SUM', 'AVG'):
            if expr_type not in translator.numeric_types:
                throw(TypeError, "Function '%s' expects argument of numeric type, got %r in {EXPR}"
                                 % (func_name, type2str(expr_type)))
        elif func_name in ('MIN', 'MAX'):
            if expr_type not in translator.comparable_types:
                throw(TypeError, "Function '%s' cannot be applied to type %r in {EXPR}"
                                 % (func_name, type2str(expr_type)))
        else: assert False
        expr = monad.getsql()
        if len(expr) == 1: expr = expr[0]
        elif translator.row_value_syntax == True: expr = ['ROW'] + expr
        else: throw(NotImplementedError, 'Database does not support entities '
                    'with composite primary keys inside aggregate functions. Got: {EXPR}')
        if func_name == 'AVG': result_type = float
        else: result_type = expr_type
        result = translator.ExprMonad.new(translator, result_type, [ func_name, expr ])
        result.aggregated = True
        return result
    def __call__(monad, *args, **keyargs): throw(TypeError)
    def __getitem__(monad, key): throw(TypeError)
    def __add__(monad, monad2): throw(TypeError)
    def __sub__(monad, monad2): throw(TypeError)
    def __mul__(monad, monad2): throw(TypeError)
    def __div__(monad, monad2): throw(TypeError)
    def __pow__(monad, monad2): throw(TypeError)
    def __neg__(monad): throw(TypeError)
    def abs(monad): throw(TypeError)

typeerror_re = re.compile(r'\(\) takes (no|(?:exactly|at (?:least|most)))(?: (\d+))? arguments \((\d+) given\)')

def reraise_improved_typeerror(exc, func_name, orig_func_name):
    if not exc.args: throw(exc)
    msg = exc.args[0]
    if not msg.startswith(func_name): throw(exc)
    msg = msg[len(func_name):]
    match = typeerror_re.match(msg)
    if not match:
        exc.args = (orig_func_name + msg,)
        throw(exc)
    what, takes, given = match.groups()
    takes, given = int(takes), int(given)
    if takes: what = '%s %d' % (what, takes-1)
    plural = takes > 2 and 's' or ''
    new_msg = '%s() takes %s argument%s (%d given)' % (orig_func_name, what, plural, given-1)
    exc.args = (new_msg,)
    throw(exc)

def raise_forgot_parentheses(monad):
    assert monad.type == 'METHOD'
    throw(TranslationError, 'You seems to forgot parentheses after %s' % ast2src(monad.node))

class MethodMonad(Monad):
    def __init__(monad, translator, parent, attrname):
        Monad.__init__(monad, translator, 'METHOD')
        monad.parent = parent
        monad.attrname = attrname
    def getattr(monad, attrname):
        raise_forgot_parentheses(monad)
    def __call__(monad, *args, **keyargs):
        method = getattr(monad.parent, 'call_' + monad.attrname)
        try: return method(*args, **keyargs)
        except TypeError, exc: reraise_improved_typeerror(exc, method.__name__, monad.attrname)

    def contains(monad, item, not_in=False): raise_forgot_parentheses(monad)
    def nonzero(monad): raise_forgot_parentheses(monad)
    def negate(monad): raise_forgot_parentheses(monad)
    def aggregate(monad, func_name): raise_forgot_parentheses(monad)
    def __getitem__(monad, key): raise_forgot_parentheses(monad)

    def __add__(monad, monad2): raise_forgot_parentheses(monad)
    def __sub__(monad, monad2): raise_forgot_parentheses(monad)
    def __mul__(monad, monad2): raise_forgot_parentheses(monad)
    def __div__(monad, monad2): raise_forgot_parentheses(monad)
    def __pow__(monad, monad2): raise_forgot_parentheses(monad)

    def __neg__(monad): raise_forgot_parentheses(monad)
    def abs(monad): raise_forgot_parentheses(monad)

class DatabaseMonad(Monad):
    def __init__(monad, translator, database):
        Monad.__init__(monad, translator, 'DATABASE')
        Monad.database = database
    def getattr(monad, attrname):
        database = monad.database
        entity = getattr(database, attrname)
        if not isinstance(entity, EntityMeta): throw(NotImplementedError)
        return EntityMonad(monad.translator, entity)

class EntityMonad(Monad):
    def __getitem__(monad, key):
        translator = monad.translator
        if isinstance(key, translator.ConstMonad): pk_monads = [ key ]
        elif isinstance(key, translator.ListMonad): pk_monads = key.items
        elif isinstance(key, slice): throw(TypeError, 'Slice is not supported in {EXPR}')
        else: throw(NotImplementedError)
        entity = monad.type
        if len(pk_monads) != len(entity._pk_attrs_): throw(TypeError, 
            'Invalid count of attrs in primary key (%d instead of %d) in expression: {EXPR}'
            % (len(pk_monads), len(entity._pk_attrs_)))
        return translator.ObjectConstMonad(translator, monad.type, pk_monads)
    def normalize_args(monad, keyargs):  # pragma: no cover
        translator = monad.translator
        entity = monad.type
        avdict = {}
        get = entity._adict_.get 
        for name, val_monad in keyargs.items():
            val_type = val_monad.type
            attr = get(name)
            if attr is None: throw(TypeError, 'Unknown attribute %r' % name)
            if attr.is_collection: throw(NotImplementedError)
            if attr.is_ref:
                if not issubclass(val_type, attr.py_type): throw(TypeError)
                if not isinstance(val_monad, translator.ObjectConstMonad):
                    throw(TypeError, 'Entity constructor arguments in declarative query should be consts')
                avdict[attr] = val_monad
            elif isinstance(val_monad, translator.ConstMonad):
                val = val_monad.value
                avdict[attr] = attr.check(val, None, entity, from_db=False)
            else: throw(TypeError, 'Entity constructor arguments in declarative query should be consts')
        pkval = map(avdict.get, entity._pk_attrs_)
        if None in pkval: pkval = None
        return pkval, avdict
    def call_query(monad):
        throw(NotImplementedError)

class ListMonad(Monad):
    def __init__(monad, translator, items):
        Monad.__init__(monad, translator, tuple(item.type for item in items))
        monad.items = items
    def contains(monad, x, not_in=False):
        translator = monad.translator
        for item in monad.items: translator.check_comparable(item, x)
        left_sql = x.getsql()
        if len(left_sql) == 1:
            if not_in: sql = [ 'NOT_IN', left_sql[0], [ item.getsql()[0] for item in monad.items ] ]
            else: sql = [ 'IN', left_sql[0], [ item.getsql()[0] for item in monad.items ] ]
        elif not_in:
            sql = sqland([ sqlor([ [ 'NE', a, b ]  for a, b in zip(left_sql, item.getsql()) ]) for item in monad.items ])
        else:
            sql = sqlor([ sqland([ [ 'EQ', a, b ]  for a, b in zip(left_sql, item.getsql()) ]) for item in monad.items ])
        return translator.BoolExprMonad(translator, sql)

class BufferMixin(MonadMixin):
    pass

_binop_errmsg = 'Unsupported operand types %r and %r for operation %r in expression: {EXPR}'

def make_numeric_binop(op, sqlop):
    def numeric_binop(monad, monad2):
        translator = monad.translator
        if isinstance(monad2, (translator.AttrSetMonad, translator.NumericSetExprMonad)):
            return translator.NumericSetExprMonad(op, sqlop, monad, monad2)
        if monad2.type == 'METHOD': raise_forgot_parentheses(monad2)
        result_type = translator.coerce_types(monad.type, monad2.type)
        if result_type is None:
            throw(TypeError, _binop_errmsg % (type2str(monad.type), type2str(monad2.type), op))
        left_sql = monad.getsql()[0]
        right_sql = monad2.getsql()[0]
        return translator.NumericExprMonad(translator, result_type, [ sqlop, left_sql, right_sql ])
    numeric_binop.__name__ = sqlop
    return numeric_binop

class NumericMixin(MonadMixin):
    def mixin_init(monad):
        assert monad.type in monad.translator.numeric_types, monad.type
    __add__ = make_numeric_binop('+', 'ADD')
    __sub__ = make_numeric_binop('-', 'SUB')
    __mul__ = make_numeric_binop('*', 'MUL')
    __div__ = make_numeric_binop('/', 'DIV')
    def __pow__(monad, monad2):
        translator = monad.translator
        if not isinstance(monad2, translator.NumericMixin):
            throw(TypeError, _binop_errmsg % (type2str(monad.type), type2str(monad2.type), '**'))
        left_sql = monad.getsql()
        right_sql = monad2.getsql()
        assert len(left_sql) == len(right_sql) == 1
        return translator.NumericExprMonad(translator, float, [ 'POW', left_sql[0], right_sql[0] ])
    def __neg__(monad):
        sql = monad.getsql()[0]
        translator = monad.translator
        return translator.NumericExprMonad(translator, monad.type, [ 'NEG', sql ])
    def abs(monad):
        sql = monad.getsql()[0]
        translator = monad.translator
        return translator.NumericExprMonad(translator, monad.type, [ 'ABS', sql ])
    def nonzero(monad):
        translator = monad.translator
        return translator.CmpMonad('!=', monad, translator.ConstMonad.new(translator, 0))
    def negate(monad):
        translator = monad.translator
        return translator.CmpMonad('==', monad, translator.ConstMonad.new(translator, 0))

def datetime_attr_factory(name):
    def attr_func(monad):
        sql = [ name, monad.getsql()[0] ]
        translator = monad.translator
        return translator.NumericExprMonad(translator, int, sql)
    attr_func.__name__ = name.lower()
    return attr_func

class DateMixin(MonadMixin):
    def mixin_init(monad):
        assert monad.type is date
    attr_year = datetime_attr_factory('YEAR')
    attr_month = datetime_attr_factory('MONTH')
    attr_day = datetime_attr_factory('DAY')
    
class DatetimeMixin(DateMixin):
    def mixin_init(monad):
        assert monad.type is datetime
    attr_hour = datetime_attr_factory('HOUR')
    attr_minute = datetime_attr_factory('MINUTE')
    attr_second = datetime_attr_factory('SECOND')

def make_string_binop(op, sqlop):
    def string_binop(monad, monad2):
        translator = monad.translator
        if not translator.are_comparable_types(monad.type, monad2.type, sqlop):
            if monad2.type == 'METHOD': raise_forgot_parentheses(monad2)
            throw(TypeError, _binop_errmsg % (type2str(monad.type), type2str(monad2.type), op))
        left_sql = monad.getsql()
        right_sql = monad2.getsql()
        assert len(left_sql) == len(right_sql) == 1
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
    __add__ = make_string_binop('+', 'CONCAT')
    def __getitem__(monad, index):
        translator = monad.translator
        if isinstance(index, translator.ListMonad): throw(TypeError, "String index must be of 'int' type. Got 'tuple' in {EXPR}")
        elif isinstance(index, slice):
            if index.step is not None: throw(TypeError, 'Step is not supported in {EXPR}')
            start, stop = index.start, index.stop
            if start is None and stop is None: return monad
            if isinstance(monad, translator.StringConstMonad) \
               and (start is None or isinstance(start, translator.NumericConstMonad)) \
               and (stop is None or isinstance(stop, translator.NumericConstMonad)):
                if start is not None: start = start.value
                if stop is not None: stop = stop.value
                return translator.ConstMonad.new(translator, monad.value[start:stop])

            if start is not None and start.type is not int:
                throw(TypeError, "Invalid type of start index (expected 'int', got %r) in string slice {EXPR}" % type2str(start.type))
            if stop is not None and stop.type is not int:
                throw(TypeError, "Invalid type of stop index (expected 'int', got %r) in string slice {EXPR}" % type2str(stop.type))
            expr_sql = monad.getsql()[0]

            if start is None: start = translator.ConstMonad.new(translator, 0)
            
            if isinstance(start, translator.NumericConstMonad):
                if start.value < 0: throw(NotImplementedError, 'Negative indices are not supported in string slice {EXPR}')
                start_sql = [ 'VALUE', start.value + 1 ]
            else:
                start_sql = start.getsql()[0]
                start_sql = [ 'ADD', start_sql, [ 'VALUE', 1 ] ]

            if stop is None:
                len_sql = None
            elif isinstance(stop, translator.NumericConstMonad):
                if stop.value < 0: throw(NotImplementedError, 'Negative indices are not supported in string slice {EXPR}')
                if isinstance(start, translator.NumericConstMonad):
                    len_sql = [ 'VALUE', stop.value - start.value ]
                else:
                    len_sql = [ 'SUB', [ 'VALUE', stop.value ], start.getsql()[0] ]
            else:
                stop_sql = stop.getsql()[0]
                if isinstance(start, translator.NumericConstMonad):
                    len_sql = [ 'SUB', stop_sql, [ 'VALUE', start.value ] ]
                else:
                    len_sql = [ 'SUB', stop_sql, start.getsql()[0] ]

            sql = [ 'SUBSTR', expr_sql, start_sql, len_sql ]
            return translator.StringExprMonad(translator, monad.type, sql)
        
        if isinstance(monad, translator.StringConstMonad) and isinstance(index, translator.NumericConstMonad):
            return translator.ConstMonad.new(translator, monad.value[index.value])
        if index.type is not int: throw(TypeError, 
            'String indices must be integers. Got %r in expression {EXPR}' % type2str(index.type))
        expr_sql = monad.getsql()[0]
        if isinstance(index, translator.NumericConstMonad):
            value = index.value
            if value >= 0: value += 1
            index_sql = [ 'VALUE', value ]
        else:
            inner_sql = index.getsql()[0]
            index_sql = [ 'ADD', inner_sql, [ 'CASE', None, [ (['GE', inner_sql, [ 'VALUE', 0 ]], [ 'VALUE', 1 ]) ], [ 'VALUE', 0 ] ] ]
        sql = [ 'SUBSTR', expr_sql, index_sql, [ 'VALUE', 1 ] ]
        return translator.StringExprMonad(translator, monad.type, sql)
    def nonzero(monad):
        sql = monad.getsql()[0]
        translator = monad.translator
        return translator.BoolExprMonad(translator, [ 'GT', [ 'LENGTH', sql ], [ 'VALUE', 0 ]])
    def len(monad):
        sql = monad.getsql()[0]
        translator = monad.translator
        return translator.NumericExprMonad(translator, int, [ 'LENGTH', sql ])
    def contains(monad, item, not_in=False):
        translator = monad.translator
        translator.check_comparable(item, monad, 'LIKE')
        if isinstance(item, translator.StringConstMonad):
            item_sql = [ 'VALUE', '%%%s%%' % item.value ]
        else:
            item_sql = [ 'CONCAT', [ 'VALUE', '%' ], item.getsql()[0], [ 'VALUE', '%' ] ]
        sql = [ 'LIKE', monad.getsql()[0], item_sql ]
        return translator.BoolExprMonad(translator, sql)
    call_upper = make_string_func('UPPER')
    call_lower = make_string_func('LOWER')
    def call_startswith(monad, arg):
        translator = monad.translator
        if not translator.are_comparable_types(monad.type, arg.type, None):
            if arg.type == 'METHOD': raise_forgot_parentheses(arg)
            throw(TypeError, 'Expected %r argument but got %r in expression {EXPR}'
                            % (type2str(monad.type), type2str(arg.type)))
        if isinstance(arg, translator.StringConstMonad):
            assert isinstance(arg.value, basestring)
            arg_sql = [ 'VALUE', arg.value + '%' ]
        else:
            arg_sql = arg.getsql()[0]
            arg_sql = [ 'CONCAT', arg_sql, [ 'VALUE', '%' ] ]
        parent_sql = monad.getsql()[0]
        sql = [ 'LIKE', parent_sql, arg_sql ]
        return translator.BoolExprMonad(translator, sql)
    def call_endswith(monad, arg):
        translator = monad.translator
        if not translator.are_comparable_types(monad.type, arg.type, None):
            if arg.type == 'METHOD': raise_forgot_parentheses(arg)
            throw(TypeError, 'Expected %r argument but got %r in expression {EXPR}'
                            % (type2str(monad.type), type2str(arg.type)))
        if isinstance(arg, translator.StringConstMonad):
            assert isinstance(arg.value, basestring)
            arg_sql = [ 'VALUE', '%' + arg.value ]
        else:
            arg_sql = arg.getsql()[0]
            arg_sql = [ 'CONCAT', [ 'VALUE', '%' ], arg_sql ]
        parent_sql = monad.getsql()[0]
        sql = [ 'LIKE', parent_sql, arg_sql ]
        return translator.BoolExprMonad(translator, sql)
    def strip(monad, chars, strip_type):
        translator = monad.translator
        if chars is not None and not translator.are_comparable_types(monad.type, chars.type, None):
            if chars.type == 'METHOD': raise_forgot_parentheses(chars)
            throw(TypeError, "'chars' argument must be of %r type in {EXPR}, got: %r"
                            % (type2str(monad.type), type2str(chars.type)))
        parent_sql = monad.getsql()[0]
        sql = [ strip_type, parent_sql ]
        if chars is not None: sql.append(chars.getsql()[0])
        return translator.StringExprMonad(translator, monad.type, sql)
    def call_strip(monad, chars=None):
        return monad.strip(chars, 'TRIM')
    def call_lstrip(monad, chars=None):
        return monad.strip(chars, 'LTRIM')
    def call_rstrip(monad, chars=None):
        return monad.strip(chars, 'RTRIM')
    
class ObjectMixin(MonadMixin):
    def mixin_init(monad):
        assert isinstance(monad.type, EntityMeta)
    def getattr(monad, name):
        translator = monad.translator
        entity = monad.type
        try: attr = entity._adict_[name]
        except KeyError: throw(AttributeError)
        if not attr.is_collection:
            return translator.AttrMonad.new(monad, attr)
        else:
            return translator.AttrSetMonad(monad, attr)
    def requires_distinct(monad, joined=False):
        return monad.attr.reverse.is_collection or monad.parent.requires_distinct(joined)

class ObjectIterMonad(ObjectMixin, Monad):
    def __init__(monad, translator, tableref, entity):
        Monad.__init__(monad, translator, entity)
        monad.tableref = tableref
    def getsql(monad, subquery=None):
        entity = monad.type
        alias, pk_columns = monad.tableref.make_join(pk_only=True)
        return [ [ 'COLUMN', alias, column ] for column in pk_columns ]
    def requires_distinct(monad, joined=False):
        return monad.tableref.name_path != monad.translator.tree.quals[-1].assign.name

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
        else: throw(NotImplementedError, type)
        return cls(parent, attr, *args, **keyargs)
    def __new__(cls, parent, attr):
        if cls is AttrMonad: assert False, 'Abstract class'
        return Monad.__new__(cls)
    def __init__(monad, parent, attr):
        assert monad.__class__ is not AttrMonad
        translator = parent.translator
        attr_type = translator.normalize_type(attr.py_type)
        Monad.__init__(monad, parent.translator, attr_type)
        monad.parent = parent
        monad.attr = attr
    def getsql(monad, subquery=None):
        parent = monad.parent
        attr = monad.attr
        entity = attr.entity
        pk_only = attr.pk_offset is not None
        alias, parent_columns = monad.parent.tableref.make_join(pk_only)
        if not pk_only: columns = attr.columns
        elif not entity._pk_is_composite_: columns = parent_columns
        else:
            offset = attr.pk_columns_offset
            columns = parent_columns[offset:offset+len(attr.columns)]
        return [ [ 'COLUMN', alias, column ] for column in columns ]
        
class ObjectAttrMonad(ObjectMixin, AttrMonad):
    def __init__(monad, parent, attr):
        AttrMonad.__init__(monad, parent, attr)
        translator = monad.translator
        parent_monad = monad.parent
        entity = monad.type
        name_path = '-'.join((parent_monad.tableref.name_path, attr.name))
        monad.tableref = translator.subquery.get_tableref(name_path)
        if monad.tableref is None:
            parent_subquery = parent_monad.tableref.subquery
            monad.tableref = parent_subquery.add_tableref(name_path, parent_monad.tableref, attr)

class NumericAttrMonad(NumericMixin, AttrMonad): pass
class StringAttrMonad(StringMixin, AttrMonad): pass
class DateAttrMonad(DateMixin, AttrMonad): pass
class DatetimeAttrMonad(DatetimeMixin, AttrMonad): pass
class BufferAttrMonad(BufferMixin, AttrMonad): pass

class ParamMonad(Monad):
    @staticmethod
    def new(translator, type, name, parent=None):
        type = translator.normalize_type(type)
        if type in translator.numeric_types: cls = translator.NumericParamMonad
        elif type in translator.string_types: cls = translator.StringParamMonad
        elif type is date: cls = translator.DateParamMonad
        elif type is datetime: cls = translator.DatetimeParamMonad
        elif type is buffer: cls = translator.BufferParamMonad
        elif isinstance(type, EntityMeta): cls = translator.ObjectParamMonad
        else: throw(NotImplementedError, type)
        return cls(translator, type, name, parent)
    def __new__(cls, translator, type, name, parent=None):
        if cls is ParamMonad: assert False, 'Abstract class'
        return Monad.__new__(cls)
    def __init__(monad, translator, type, name, parent=None):
        type = translator.normalize_type(type)
        Monad.__init__(monad, translator, type)
        monad.name = name
        monad.parent = parent
        if not isinstance(type, EntityMeta):
            provider = translator.database.provider
            monad.converter = provider.get_converter_by_py_type(type)
        else: monad.converter = None
        if parent is None: monad.extractor = lambda variables : variables[name]
        else: monad.extractor = lambda variables : getattr(parent.extractor(variables), name)
    def getsql(monad, subquery=None):
        monad.add_extractors()
        return [ [ 'PARAM', monad.name, monad.converter ] ]
    def add_extractors(monad):
        monad.translator.extractors[monad.name] = monad.extractor

class ObjectParamMonad(ObjectMixin, ParamMonad):
    def __init__(monad, translator, entity, name, parent=None):
        assert translator.database is entity._database_
        monad.params = [ '-'.join((name, path)) for path in entity._pk_paths_ ]
        ParamMonad.__init__(monad, translator, entity, name, parent)
    def getattr(monad, name):
        entity = monad.type
        try: attr = entity._adict_[name]
        except KeyError: throw(AttributeError)
        if attr.is_collection: throw(NotImplementedError)
        translator = monad.translator
        return translator.ParamMonad.new(translator, attr.py_type, name, monad)
    def getsql(monad, subquery=None):
        monad.add_extractors()
        entity = monad.type
        assert len(monad.params) == len(entity._pk_converters_)
        return [ [ 'PARAM', param, converter ] for param, converter in zip(monad.params, entity._pk_converters_) ]
    def add_extractors(monad):
        entity = monad.type
        extractors = monad.translator.extractors
        if len(entity._pk_columns_) == 1:
            extractors[monad.params[0]] = lambda vars, e=monad.extractor : e(vars)._get_raw_pkval_()[0]
        else:
            for i, param in enumerate(monad.params):
                extractors[param] = lambda vars, i=i, e=monad.extractor : e(vars)._get_raw_pkval_()[i]
    def requires_distinct(monad, joined=False):
        assert False

class StringParamMonad(StringMixin, ParamMonad): pass
class NumericParamMonad(NumericMixin, ParamMonad): pass
class DateParamMonad(DateMixin, ParamMonad): pass
class DatetimeParamMonad(DatetimeMixin, ParamMonad): pass
class BufferParamMonad(BufferMixin, ParamMonad): pass

class ExprMonad(Monad):
    @staticmethod
    def new(translator, type, sql):
        if type in translator.numeric_types: cls = translator.NumericExprMonad
        elif type in translator.string_types: cls = translator.StringExprMonad
        elif type is date: cls = translator.DateExprMonad
        elif type is datetime: cls = translator.DatetimeExprMonad
        else: throw(NotImplementedError, type)
        return cls(translator, type, sql)
    def __new__(cls, translator, type, sql):
        if cls is ExprMonad: assert False, 'Abstract class'
        return Monad.__new__(cls)
    def __init__(monad, translator, type, sql):
        Monad.__init__(monad, translator, type)
        monad.sql = sql
    def getsql(monad, subquery=None):
        return [ monad.sql ]

class StringExprMonad(StringMixin, ExprMonad): pass
class NumericExprMonad(NumericMixin, ExprMonad): pass
class DateExprMonad(DateMixin, ExprMonad): pass
class DatetimeExprMonad(DatetimeMixin, ExprMonad): pass

class ConstMonad(Monad):
    @staticmethod
    def new(translator, value):
        value_type = translator.get_normalized_type_of(value)
        if value_type in translator.numeric_types: cls = translator.NumericConstMonad
        elif value_type in translator.string_types: cls = translator.StringConstMonad
        elif value_type is date: cls = translator.DateConstMonad
        elif value_type is datetime: cls = translator.DatetimeConstMonad
        elif value_type is NoneType: cls = translator.NoneMonad
        elif value_type is buffer: cls = translator.BufferConstMonad
        else: throw(NotImplementedError, value_type)
        return cls(translator, value)
    def __new__(cls, translator, value):
        if cls is ConstMonad: assert False, 'Abstract class'
        return Monad.__new__(cls)
    def __init__(monad, translator, value):
        value_type = translator.get_normalized_type_of(value)
        Monad.__init__(monad, translator, value_type)
        monad.value = value
    def getsql(monad, subquery=None):
        return [ [ 'VALUE', monad.value ] ]

class NoneMonad(ConstMonad):
    type = NoneType
    def __init__(monad, translator, value=None):
        assert value is None
        ConstMonad.__init__(monad, translator, value)

class BufferConstMonad(BufferMixin, ConstMonad): pass

class StringConstMonad(StringMixin, ConstMonad):
    def len(monad):
        return monad.translator.ConstMonad.new(monad.translator, len(monad.value))
    
class NumericConstMonad(NumericMixin, ConstMonad): pass
class DateConstMonad(DateMixin, ConstMonad): pass
class DatetimeConstMonad(DatetimeMixin, ConstMonad): pass

class ObjectConstMonad(Monad):
    def __init__(monad, translator, entity, pk_monads):
        for attr, pk_monad in izip(entity._pk_attrs_, pk_monads):
            attr_type = translator.normalize_type(attr.py_type)
            if not translator.are_comparable_types(attr_type, pk_monad.type):
                throw(TypeError, "Attribute %s of type %r cannot be compared with value of %r type in expression: {EXPR}"
                                % (attr, type2str(attr_type), type2str(pk_monad.type)))
        Monad.__init__(monad, translator, entity)
        monad.pk_monads = pk_monads
        rawpkval = monad.rawpkval = []
        for pk_monad in pk_monads:
            if isinstance(pk_monad, translator.ConstMonad): rawpkval.append(pk_monad.value)
            elif isinstance(pk_monad, translator.ObjectConstMonad): rawpkval.extend(pk_monad.rawpkval)
            else: assert False, pk_monad
    def getsql(monad, subquery=None):
        entity = monad.type
        return [ [ 'VALUE', value ] for value in monad.rawpkval ]
    def getattr(monad, name):
        entity = monad.type
        try: attr = entity._adict_[name]
        except KeyError: throw(AttributeError)
        if attr.is_collection: throw(NotImplementedError)
        monad.extractor = lambda variables: entity._get_by_raw_pkval_(monad.rawpkval)
        translator = monad.translator
        return translator.ParamMonad.new(translator, attr.py_type, name, monad)

class BoolMonad(Monad):
    def __init__(monad, translator):
        monad.translator = translator
        monad.type = bool

sql_negation = { 'IN' : 'NOT_IN', 'EXISTS' : 'NOT_EXISTS', 'LIKE' : 'NOT_LIKE', 'BETWEEN' : 'NOT_BETWEEN', 'IS_NULL' : 'IS_NOT_NULL' }
sql_negation.update((value, key) for key, value in sql_negation.items())

class BoolExprMonad(BoolMonad):
    def __init__(monad, translator, sql):
        monad.translator = translator
        monad.type = bool
        monad.sql = sql
    def getsql(monad, subquery=None):
        return monad.sql
    def negate(monad):
        translator = monad.translator
        sql = monad.sql
        sqlop = sql[0]
        negated_op = sql_negation.get(sqlop)
        if negated_op is not None:
            negated_sql = [ negated_op ] + sql[1:]
        elif negated_op == 'NOT':
            assert len(sql) == 2
            negated_sql = sql[1]
        else: return translator.NotMonad(translator, sql)
        return translator.BoolExprMonad(translator, negated_sql)

cmp_ops = { '>=' : 'GE', '>' : 'GT', '<=' : 'LE', '<' : 'LT' }        

cmp_negate = { '<' : '>=', '<=' : '>', '==' : '!=', 'is' : 'is not' }
cmp_negate.update((b, a) for a, b in cmp_negate.items())

class CmpMonad(BoolMonad):
    def __init__(monad, op, left, right):
        translator = left.translator
        translator.check_comparable(left, right, op)
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
    def getsql(monad, subquery=None):
        op = monad.op
        sql = []
        left_sql = monad.left.getsql()
        if op == 'is':
            return sqland([ [ 'IS_NULL', item ] for item in left_sql ])
        if op == 'is not':
            return sqland([ [ 'IS_NOT_NULL', item ] for item in left_sql ])
        right_sql = monad.right.getsql()
        assert len(left_sql) == len(right_sql)
        if op in ('<', '<=', '>', '>='):
            assert len(left_sql) == len(right_sql) == 1
            return [ cmp_ops[op], left_sql[0], right_sql[0] ]
        if op == '==':
            return sqland([ [ 'EQ', a, b ] for (a, b) in zip(left_sql, right_sql) ])
        if op == '!=':
            return sqlor([ [ 'NE', a, b ] for (a, b) in zip(left_sql, right_sql) ])
        assert False

class LogicalBinOpMonad(BoolMonad):
    def __init__(monad, operands):
        assert len(operands) >= 2
        items = []
        translator = operands[0].translator
        monad.translator = translator
        for operand in operands:
            if operand.type is not bool: items.append(operand.nonzero())
            elif isinstance(operand, translator.LogicalBinOpMonad) and monad.binop == operand.binop:
                items.extend(operand.operands)
            else: items.append(operand)
        BoolMonad.__init__(monad, items[0].translator)
        monad.operands = items
    def getsql(monad, subquery=None):
        return [ monad.binop ] + [ operand.getsql() for operand in monad.operands ]

class AndMonad(LogicalBinOpMonad):
    binop = 'AND'

class OrMonad(LogicalBinOpMonad):
    binop = 'OR'

class NotMonad(BoolMonad):
    def __init__(monad, operand):
        if operand.type is not bool: operand = operand.nonzero()
        BoolMonad.__init__(monad, operand.translator)
        monad.operand = operand
    def negate(monad):
        return monad.operand
    def getsql(monad, subquery=None):
        return [ 'NOT', monad.operand.getsql() ]

special_functions = SQLTranslator.special_functions = {}

class FuncMonadMeta(MonadMeta):
    def __new__(meta, cls_name, bases, cls_dict):
        func = cls_dict.get('func')
        monad_cls = super(FuncMonadMeta, meta).__new__(meta, cls_name, bases, cls_dict)
        if func: special_functions[func] = monad_cls
        return monad_cls

class FuncMonad(Monad):
    __metaclass__ = FuncMonadMeta
    type = 'function'
    def __init__(monad, translator):
        monad.translator = translator
    def __call__(monad, *args, **keyargs):
        translator = monad.translator
        for arg in args:
            assert isinstance(arg, translator.Monad)
        for value in keyargs.values():
            assert isinstance(value, translator.Monad)
        try: return monad.call(*args, **keyargs)
        except TypeError, exc: reraise_improved_typeerror(exc, 'call', monad.func.__name__)

class FuncBufferMonad(FuncMonad):
    func = buffer
    def call(monad, x):
        translator = monad.translator
        if not isinstance(x, translator.StringConstMonad): throw(TypeError)
        return translator.ConstMonad.new(translator, buffer(x.value))

class FuncDecimalMonad(FuncMonad):
    func = Decimal
    def call(monad, x):
        translator = monad.translator
        if not isinstance(x, translator.StringConstMonad): throw(TypeError)
        return translator.ConstMonad.new(translator, Decimal(x.value))

class FuncDateMonad(FuncMonad):
    func = date
    def call(monad, year, month, day):
        translator = monad.translator
        for x, name in zip((year, month, day), ('year', 'month', 'day')):
            if not isinstance(x, translator.NumericMixin) or x.type is not int: throw(TypeError, 
                "'%s' argument of date(year, month, day) function must be of 'int' type. Got: %r" % (name, type2str(x.type)))
            if not isinstance(x, translator.ConstMonad): throw(NotImplementedError)
        return translator.ConstMonad.new(translator, date(year.value, month.value, day.value))
    def call_today(monad):
        translator = monad.translator
        return translator.DateExprMonad(translator, date, [ 'TODAY' ])

class FuncDatetimeMonad(FuncDateMonad):
    func = datetime
    def call(monad, *args):
        translator = monad.translator
        for x, name in zip(args, ('year', 'month', 'day', 'hour', 'minute', 'second', 'microsecond')):
            if not isinstance(x, translator.NumericMixin) or x.type is not int: throw(TypeError, 
                "'%s' argument of datetime(...) function must be of 'int' type. Got: %r" % (name, type2str(x.type)))
            if not isinstance(x, translator.ConstMonad): throw(NotImplementedError)
        return translator.ConstMonad.new(translator, datetime(*tuple(arg.value for arg in args)))
    def call_now(monad):
        translator = monad.translator
        return translator.DatetimeExprMonad(translator, datetime, [ 'NOW' ])

class FuncLenMonad(FuncMonad):
    func = len
    def call(monad, x):
        return x.len()

class FuncCountMonad(FuncMonad):
    func = count
    def call(monad, x=None):
        translator = monad.translator
        if isinstance(x, translator.StringConstMonad) and x.value == '*': x = None
        if x is not None: return x.count()
        result = translator.ExprMonad.new(translator, int, [ 'COUNT', 'ALL' ])
        result.aggregated = True
        return result

class FuncAbsMonad(FuncMonad):
    func = abs
    def call(monad, x):
        return x.abs()

class FuncSumMonad(FuncMonad):
    func = sum
    def call(monad, x):
        return x.aggregate('SUM')

class FuncAvgMonad(FuncMonad):
    func = avg
    def call(monad, x):
        return x.aggregate('AVG')

class FuncMinMonad(FuncMonad):
    func = min
    def call(monad, *args):
        if not args: throw(TypeError, 'min() function expected at least one argument')
        if len(args) == 1: return args[0].aggregate('MIN')
        return minmax(monad, 'MIN', *args)

class FuncMaxMonad(FuncMonad):
    func = max
    def call(monad, *args):
        if not args: throw(TypeError, 'max() function expected at least one argument')
        if len(args) == 1: return args[0].aggregate('MAX')
        return minmax(monad, 'MAX', *args)

def minmax(monad, sqlop, *args):
    assert len(args) > 1
    translator = monad.translator
    t = args[0].type
    if t == 'METHOD': raise_forgot_parentheses(args[0])
    if t not in translator.comparable_types: throw(TypeError, 
        "Value of type %r is not valid as argument of %r function in expression {EXPR}"
        % (type2str(t), sqlop.lower()))
    for arg in args[1:]:
        t2 = arg.type
        if t2 == 'METHOD': raise_forgot_parentheses(arg)
        t3 = translator.coerce_types(t, t2)
        if t3 is None: throw(IncomparableTypesError, t, t2)
        t = t3
    sql = [ sqlop ] + [ arg.getsql()[0] for arg in args ]
    return translator.ExprMonad.new(translator, t, sql)

class FuncSelectMonad(FuncMonad):
    func = query
    def call(monad, queryset):
        translator = monad.translator
        if not isinstance(queryset, translator.QuerySetMonad): throw(TypeError, 
            "'query' function expects generator expression, got: {EXPR}")
        return queryset

class FuncExistsMonad(FuncMonad):
    func = exists
    def call(monad, arg):
        if not isinstance(arg, monad.translator.SetMixin): throw(TypeError, 
            "'exists' function expects generator expression or collection, got: {EXPR}")
        return arg.nonzero()

class JoinMonad(Monad):
    def __init__(monad, translator):
        monad.translator = translator
        monad.hint_join_prev = translator.hint_join
        translator.hint_join = True
    def __call__(monad, x):
        monad.translator.hint_join = monad.hint_join_prev
        return x
special_functions[JOIN] = JoinMonad

class SetMixin(MonadMixin):
    pass

def make_attrset_binop(op, sqlop):
    def attrset_binop(monad, monad2):
        NumericSetExprMonad = monad.translator.NumericSetExprMonad
        return NumericSetExprMonad(op, sqlop, monad, monad2)
    return attrset_binop

class AttrSetMonad(SetMixin, Monad):
    def __init__(monad, parent, attr):
        translator = parent.translator
        item_type = translator.normalize_type(attr.py_type)
        Monad.__init__(monad, translator, SetType(item_type))
        monad.parent = parent
        monad.attr = attr
        monad.subquery = None
        monad.tableref = None
    def cmp(monad, op, monad2):
        translator = monad.translator
        if type(monad2.type) is SetType and \
           translator.are_comparable_types(monad.type.item_type, monad2.type.item_type): pass
        elif monad.type != monad2.type: translator.check_comparable(monad, monad2)
        throw(NotImplementedError)
    def contains(monad, item, not_in=False):
        translator = monad.translator
        translator.check_comparable(item, monad, 'in')
        if not translator.hint_join:
            sqlop = not_in and 'NOT_IN' or 'IN'
            subquery = monad._subselect()
            expr_list = subquery.expr_list
            from_ast = subquery.from_ast
            conditions = subquery.outer_conditions + subquery.conditions
            if len(expr_list) == 1:
                subquery_ast = [ 'SELECT', [ 'ALL' ] + expr_list, from_ast, [ 'WHERE' ] + conditions ]
                return translator.BoolExprMonad(translator, [ sqlop, item.getsql()[0], subquery_ast ])
            elif translator.row_value_syntax:
                subquery_ast = [ 'SELECT', [ 'ALL' ] + expr_list, from_ast, [ 'WHERE' ] + conditions ]
                return translator.BoolExprMonad(translator, [ sqlop, [ 'ROW' ] + item.getsql(), subquery_ast ])
            else:
                conditions += [ [ 'EQ', expr1, expr2 ] for expr1, expr2 in izip(item.getsql(), expr_list) ]
                subquery_ast = [ not_in and 'NOT_EXISTS' or 'EXISTS', from_ast, [ 'WHERE' ] + conditions ] 
                return translator.BoolExprMonad(translator, subquery_ast)
        else: throw(NotImplementedError)
    def getattr(monad, name):
        entity = monad.type.item_type
        if not isinstance(entity, EntityMeta): throw(AttributeError)
        attr = entity._adict_.get(name)
        if attr is None: throw(AttributeError)
        return monad.translator.AttrSetMonad(monad, attr)
    def requires_distinct(monad, joined=False):
        if monad.parent.requires_distinct(joined): return True
        reverse = monad.attr.reverse
        if not reverse: return True
        if reverse.is_collection:
            translator = monad.translator
            if not translator.hint_join: return True
            if isinstance(monad.parent, monad.translator.AttrSetMonad): return True
        return False
    def count(monad):
        translator = monad.translator

        subquery = monad._subselect()
        expr_list = subquery.expr_list
        from_ast = subquery.from_ast
        inner_conditions = subquery.conditions
        outer_conditions = subquery.outer_conditions

        distinct = monad.requires_distinct(joined=translator.hint_join)
        sql_ast = make_aggr = None
        extra_grouping = False
        if not distinct:
            make_aggr = lambda expr_list: [ 'COUNT', 'ALL' ]
        elif len(expr_list) == 1:
            make_aggr = lambda expr_list: [ 'COUNT', 'DISTINCT' ] + expr_list
        elif translator.dialect == 'Oracle':
            extra_grouping = True
            if translator.hint_join: make_aggr = lambda expr_list: [ 'COUNT', 'ALL' ]
            else: make_aggr = lambda expr_list: [ 'COUNT', 'ALL', [ 'COUNT', 'ALL' ] ]
        elif translator.row_value_syntax:
            make_aggr = lambda expr_list: [ 'COUNT', 'DISTINCT' ] + expr_list
        elif translator.dialect == 'SQLite':
            if translator.hint_join:  # Same join as in Oracle
                extra_grouping = True
                make_aggr = lambda expr_list: [ 'COUNT', 'ALL' ]
            elif translator.sqlite_version < (3, 6, 21):
                alias, pk_columns = monad.tableref.make_join(pk_only=False)
                make_aggr = lambda expr_list: [ 'COUNT', 'DISTINCT', [ 'COLUMN', alias, 'ROWID' ] ]
            else:
                sql_ast = [ 'SELECT', [ 'AGGREGATES', [ 'COUNT', 'ALL' ] ],
                          [ 'FROM', [ 't', 'SELECT', [
                              [ 'DISTINCT' ] + expr_list, from_ast,
                              [ 'WHERE' ] + outer_conditions + inner_conditions ] ] ] ]
        else: throw(NotImplementedError)  # Must not be here
        if not sql_ast:
            subselect_func = translator.hint_join and monad._joined_subselect \
                             or monad._aggregated_scalar_subselect
            sql_ast = subselect_func(make_aggr, extra_grouping)
        return translator.ExprMonad.new(translator, int, sql_ast)
    len = count
    def aggregate(monad, func_name):
        translator = monad.translator
        item_type = monad.type.item_type

        if func_name in ('SUM', 'AVG'):        
            if item_type not in translator.numeric_types: throw(TypeError, 
                "Function %s() expects query or items of numeric type, got %r in {EXPR}"
                % (func_name.lower(), type2str(item_type)))
        elif func_name in ('MIN', 'MAX'):
            if item_type not in translator.comparable_types: throw(TypeError, 
                "Function %s() expects query or items of comparable type, got %r in {EXPR}"
                % (func_name.lower(), type2str(item_type)))
        else: assert False
            
        subselect_func = translator.hint_join and monad._joined_subselect \
                         or monad._aggregated_scalar_subselect
        sql_ast = subselect_func(lambda expr_list: [ func_name ] + expr_list)
        result_type = func_name == 'AVG' and float or item_type
        return translator.ExprMonad.new(monad.translator, result_type, sql_ast)
    def nonzero(monad):
        subquery = monad._subselect()
        sql_ast = [ 'EXISTS', subquery.from_ast,
                    [ 'WHERE' ] + subquery.outer_conditions + subquery.conditions ]
        translator = monad.translator
        return translator.BoolExprMonad(translator, sql_ast)
    def negate(monad):
        subquery = monad._subselect()
        sql_ast = [ 'NOT_EXISTS', subquery.from_ast,
                    [ 'WHERE' ] + subquery.outer_conditions + subquery.conditions ]
        translator = monad.translator
        return translator.BoolExprMonad(translator, sql_ast)
    def make_tableref(monad, subquery):
        parent = monad.parent
        attr = monad.attr
        translator = monad.translator
        if isinstance(parent, ObjectMixin): parent_tableref = parent.tableref
        elif isinstance(parent, translator.AttrSetMonad): parent_tableref = parent.make_tableref(subquery)
        else: assert False
        if attr.reverse: 
            name_path = parent_tableref.name_path + '-' + attr.name
            monad.tableref = subquery.get_tableref(name_path) \
                             or subquery.add_tableref(name_path, parent_tableref, attr)
        else: monad.tableref = parent_tableref
        return monad.tableref
    def make_expr_list(monad):
        attr = monad.attr
        pk_only = attr.reverse or attr.pk_offset is not None
        alias, columns = monad.tableref.make_join(pk_only)
        if attr.reverse: pass
        elif pk_only:
            offset = attr.pk_columns_offset
            columns = columns[offset:offset+len(attr.columns)]
        else: columns = attr.columns
        return [ [ 'COLUMN', alias, column ] for column in columns ]
    def _aggregated_scalar_subselect(monad, make_aggr, extra_grouping=False):
        translator = monad.translator
        subquery = monad._subselect()
        sql_ast = [ 'SELECT', [ 'AGGREGATES', make_aggr(subquery.expr_list) ], subquery.from_ast,
                    [ 'WHERE' ] + subquery.outer_conditions + subquery.conditions ]
        if extra_grouping:  # This is for Oracle only, with COUNT(COUNT(*))
            sql_ast.append([ 'GROUP_BY' ] + subquery.expr_list)
        return sql_ast
    def _joined_subselect(monad, make_aggr, extra_grouping=False):
        translator = monad.translator
        subquery = monad._subselect()
        expr_list = subquery.expr_list
        from_ast = subquery.from_ast
        inner_conditions = subquery.conditions
        outer_conditions = subquery.outer_conditions
        
        groupby_columns = [ inner_column[:] for cond, outer_column, inner_column in outer_conditions ]
        assert len(set(alias for _, alias, column in groupby_columns)) == 1

        if extra_grouping:
            inner_alias = translator.subquery.get_short_alias(None, 't')
            inner_columns = [ 'DISTINCT' ]
            col_mapping = {}
            col_names = set()
            for i, column_ast in enumerate(groupby_columns + expr_list):
                assert column_ast[0] == 'COLUMN'
                tname, cname = column_ast[1:]
                if cname not in col_names:
                    col_mapping[tname, cname] = cname
                    col_names.add(cname)
                    expr = [ 'AS', column_ast, cname ]
                    new_name = cname
                else:
                    new_name = 'expr-%d' % translator.subquery.expr_counter()
                    col_mapping[tname, cname] = new_name
                    expr = [ 'AS', column_ast, new_name ]
                inner_columns.append(expr)
                if i < len(groupby_columns):
                    groupby_columns[i] = [ 'COLUMN', inner_alias, new_name ]
            inner_select = [ inner_columns, from_ast ]
            if inner_conditions: inner_select.append([ 'WHERE' ] + inner_conditions)
            from_ast = [ 'FROM', [ inner_alias, 'SELECT', inner_select ] ]
            outer_conditions = outer_conditions[:]
            for i, (cond, outer_column, inner_column) in enumerate(outer_conditions):
                assert inner_column[0] == 'COLUMN'
                tname, cname = inner_column[1:]
                new_name = col_mapping[tname, cname]
                outer_conditions[i] = [ cond, outer_column, [ 'COLUMN', inner_alias, new_name ] ]
                
        subquery_columns = [ 'ALL' ]
        for column_ast in groupby_columns:
            assert column_ast[0] == 'COLUMN'
            subquery_columns.append([ 'AS', column_ast, column_ast[2] ])
        expr_name = 'expr-%d' % translator.subquery.expr_counter()
        subquery_columns.append([ 'AS', make_aggr(expr_list), expr_name ])

        subquery_ast = [ subquery_columns, from_ast ]
        if inner_conditions and not extra_grouping:
            subquery_ast.append([ 'WHERE' ] + inner_conditions)
        subquery_ast.append([ 'GROUP_BY' ] + groupby_columns)

        alias = translator.subquery.get_short_alias(None, 't')
        for cond in outer_conditions: cond[2][1] = alias
        translator.subquery.from_ast.append([ alias, 'SELECT', subquery_ast, sqland(outer_conditions) ])
        return [ 'COLUMN', alias, expr_name ]
    def _subselect(monad):
        if monad.subquery is not None: return monad.subquery
        parent = monad.parent
        attr = monad.attr
        subquery = Subquery(monad.translator.subquery)
        monad.make_tableref(subquery)
        subquery.expr_list = monad.make_expr_list()
        if not attr.reverse and not attr.is_required:
            subquery.conditions.extend([ 'IS_NOT_NULL', expr ] for expr in subquery.expr_list)
        subquery.outer_conditions = [ subquery.from_ast[1].pop() ]
        monad.subquery = subquery
        return subquery
    def getsql(monad, subquery=None):
        parent = monad.parent
        if subquery is None: subquery = monad.translator.subquery
        monad.make_tableref(subquery)
        return monad.make_expr_list()
    __add__ = make_attrset_binop('+', 'ADD')
    __sub__ = make_attrset_binop('-', 'SUB')
    __mul__ = make_attrset_binop('*', 'MUL')
    __div__ = make_attrset_binop('/', 'DIV')

def make_numericset_binop(op, sqlop):
    def numericset_binop(monad, monad2):
        NumericSetExprMonad = monad.translator.NumericSetExprMonad
        return NumericSetExprMonad(op, sqlop, monad, monad2)
    return numericset_binop

class NumericSetExprMonad(SetMixin, Monad):
    def __init__(monad, op, sqlop, left, right):
        t1 = left.type
        if isinstance(t1, SetType): t1 = t1.item_type
        t2 = right.type
        if isinstance(t2, SetType): t2 = t2.item_type
        translator = left.translator
        result_type = translator.coerce_types(t1, t2)
        if result_type not in translator.numeric_types:
            throw(TypeError, _binop_errmsg % (type2str(left.type), type2str(right.type), op))
        Monad.__init__(monad, translator, result_type)
        monad.op = op
        monad.sqlop = sqlop
        monad.left = left
        monad.right = right
    def aggregate(monad, func_name):
        translator = monad.translator
        subquery = Subquery(translator.subquery)
        expr = [ monad.sqlop, monad.left.getsql(subquery), monad.right.getsql(subquery) ]
        subquery.outer_conditions = [ subquery.from_ast[1].pop() ]
        if func_name == 'AVG': result_type = float
        else: result_type = monad.type
        return translator.ExprMonad.new(translator, result_type,
            [ 'SELECT', [ 'AGGREGATES', [ func_name, monad.getsql(subquery)[0] ] ],
              subquery.from_ast,
              [ 'WHERE' ] + subquery.outer_conditions + subquery.conditions ])
    def getsql(monad, subquery=None):
        if subquery is None: subquery = monad.translator.subquery
        left = monad.left
        left_expr = left.getsql(subquery)[0]
        right = monad.right
        right_expr = right.getsql(subquery)[0]
        if isinstance(left, NumericMixin): left_path = ''
        else: left_path = left.tableref.name_path + '-'
        if isinstance(right, NumericMixin): right_path = ''
        else: right_path = right.tableref.name_path + '-'
        if left_path.startswith(right_path): tableref = left.tableref
        elif right_path.startswith(left_path): tableref = right.tableref
        else: throw(TranslationError, 'Cartesian product detected in %s' % ast2src(monad.node))
        monad.tableref = tableref
        return [ [ monad.sqlop, left_expr, right_expr ] ]
    __add__ = make_numericset_binop('+', 'ADD')
    __sub__ = make_numericset_binop('-', 'SUB')
    __mul__ = make_numericset_binop('*', 'MUL')
    __div__ = make_numericset_binop('/', 'DIV')

class QuerySetMonad(SetMixin, Monad):
    def __init__(monad, translator, subtranslator):
        monad.translator = translator
        monad.subtranslator = subtranslator
        item_type = subtranslator.expr_type
        monad.item_type = item_type
        monad_type = SetType(item_type)
        Monad.__init__(monad, translator, monad_type)
    def contains(monad, item, not_in=False):
        translator = monad.translator
        translator.check_comparable(item, monad, 'in')
        sub = monad.subtranslator
        columns_ast = sub.expr_columns
        conditions = sub.conditions[:]
        if not translator.hint_join:
            if len(columns_ast) == 1 or translator.row_value_syntax:
                select_ast = [ 'ALL' ] + columns_ast
                subquery_ast = [ 'SELECT', select_ast, sub.subquery.from_ast ]
                subquery_expr = sub.tree.expr.monad
                if isinstance(subquery_expr, translator.AttrMonad) and subquery_expr.attr.is_required: pass
                else: conditions += [ [ 'IS_NOT_NULL', column_ast ] for column_ast in sub.expr_columns ]
                if conditions: subquery_ast.append([ 'WHERE' ] + conditions)
                if len(columns_ast) == 1: expr_ast = item.getsql()[0]
                else: expr_ast = [ 'ROW' ] + item.getsql()
                sql_ast = [ not_in and 'NOT_IN' or 'IN', expr_ast, subquery_ast ]
                return translator.BoolExprMonad(translator, sql_ast)
            else:
                if isinstance(item, translator.ListMonad):
                    item_columns = []
                    for subitem in item.items: item_columns.extend(subitem.getsql())
                else: item_columns = item.getsql()
                conditions += [ [ 'EQ', expr1, expr2 ] for expr1, expr2 in izip(item_columns, columns_ast) ]
                subquery_ast = [ not_in and 'NOT_EXISTS' or 'EXISTS', sub.subquery.from_ast, [ 'WHERE' ] + conditions ]
                return translator.BoolExprMonad(translator, subquery_ast)
        else: throw(NotImplementedError)
    def nonzero(monad):        
        sub = monad.subtranslator
        sql_ast = [ 'EXISTS', sub.subquery.from_ast, [ 'WHERE' ] + sub.conditions ]
        translator = monad.translator
        return translator.BoolExprMonad(translator, sql_ast)
    def negate(monad):
        sub = monad.subtranslator
        sql_ast = [ 'NOT_EXISTS', sub.subquery.from_ast, [ 'WHERE' ] + sub.conditions ]
        translator = monad.translator
        return translator.BoolExprMonad(translator, sql_ast)
    def _subselect(monad, item_type, select_ast):
        sub = monad.subtranslator
        sql_ast = [ 'SELECT', select_ast, sub.subquery.from_ast, [ 'WHERE' ] + sub.conditions ]
        translator = monad.translator
        return translator.ExprMonad.new(translator, item_type, sql_ast)
    def count(monad):
        sub = monad.subtranslator
        expr_type = sub.expr_type
        if isinstance(expr_type, (tuple, EntityMeta)):
            if not sub.distinct:
                select_ast = [ 'AGGREGATES', [ 'COUNT', 'ALL' ] ]
                return monad._subselect(int, select_ast)
            translator = monad.translator
            if len(sub.expr_columns) == 1:
                select_ast = [ 'AGGREGATES', [ 'COUNT', 'DISTINCT' ] + sub.expr_columns ]
                return monad._subselect(int, select_ast)
            if translator.dialect == 'Oracle':
                sql_ast = [ 'SELECT', [ 'AGGREGATES', [ 'COUNT', 'ALL', [ 'COUNT', 'ALL' ] ] ],
                            sub.subquery.from_ast, [ 'WHERE' ] + sub.conditions,
                            [ 'GROUP_BY' ] + sub.expr_columns ]
                return translator.ExprMonad.new(translator, int, sql_ast)
            if translator.row_value_syntax:
                select_ast = [ 'AGGREGATES', [ 'COUNT', 'DISTINCT' ] + sub.expr_columns ]
                return monad._subselect(int, select_ast)
            if translator.dialect == 'SQLite':
                if True or translator.sqlite_version < (3, 6, 21):
                    if sub.aggregated: throw(TranslationError)
                    alias, pk_columns = sub.tableref.make_join(pk_only=False)
                    sql_ast = [ 'SELECT', [ 'AGGREGATES',
                                  [ 'COUNT', 'DISTINCT', [ 'COLUMN', alias, 'ROWID' ] ] ],
                                sub.subquery.from_ast, [ 'WHERE' ] + sub.conditions ]
                else:
                    alias = translator.subquery.get_short_alias(None, 't')
                    sql_ast = [ 'SELECT', [ 'AGGREGATES', [ 'COUNT', 'ALL' ] ],
                                [ 'FROM', [ alias, 'SELECT', [
                                  [ 'DISTINCT' ] + sub.expr_columns, sub.subquery.from_ast,
                                  [ 'WHERE' ] + sub.conditions ] ] ] ]
                return translator.ExprMonad.new(translator, int, sql_ast)
            throw(NotImplementedError)  # Must not be here
        elif len(sub.expr_columns) == 1:
            select_ast = [ 'AGGREGATES', [ 'COUNT', 'DISTINCT', sub.expr_columns[0] ] ]
            return monad._subselect(int, select_ast)
        else: throw(NotImplementedError)
    len = count
    def aggregate(monad, func_name):
        translator = monad.translator
        sub = monad.subtranslator
        expr_type = sub.expr_type
        if func_name in ('SUM', 'AVG'):
            if expr_type not in translator.numeric_types: throw(TypeError, 
                "Function %s() expects query or items of numeric type, got %r in {EXPR}"
                % (func_name.lower(), type2str(expr_type)))
        elif func_name in ('MIN', 'MAX'):
            if expr_type not in translator.comparable_types: throw(TypeError, 
                "Function %s() cannot be applied to type %r in {EXPR}"
                % (func_name.lower(), type2str(expr_type)))
        else: assert False        
        assert len(sub.expr_columns) == 1
        select_ast = [ 'AGGREGATES', [ func_name, sub.expr_columns[0] ] ]
        result_type = func_name == 'AVG' and float or expr_type
        return monad._subselect(result_type, select_ast)

for name, value in globals().items():
    if name.endswith('Monad') or name.endswith('Mixin'):
        setattr(SQLTranslator, name, value)
del name, value
