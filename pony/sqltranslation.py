import __builtin__, types, sys, decimal, re
from itertools import izip, count
from types import NoneType
from compiler import ast
from decimal import Decimal
from datetime import date, datetime

from pony import options
from pony.dbapiprovider import LongStr, LongUnicode
from pony.utils import avg, copy_func_attrs, is_ident, throw
from pony.orm import (
    query, exists, ERDiagramError, TranslationError, EntityMeta, Set, JOIN, AsciiStr,
    aggregate_functions, COUNT, SUM, MIN, MAX, AVG
    )

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
        return node.code.src        
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
    max_alias_length = 30
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
    def are_comparable_types(translator, type1, type2, op='=='):
        # types must be normalized already! 
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
        translator.tablerefs = tablerefs = {}
        translator.parent = parent_translator
        if parent_translator is None:
            translator.alias_counters = {}
            translator.expr_counter = count(1).next
        else:
            translator.alias_counters = parent_translator.alias_counters
            translator.expr_counter = parent_translator.expr_counter
        translator.extractors = {}
        translator.distinct = False
        translator.from_ = [ 'FROM' ]
        translator.conditions = []
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
                tablerefs[name] = TableRef(translator, name, entity)
            else:
                attr_names.reverse()
                name_path = node_name
                parent_tableref = translator.get_tableref(node_name)
                if parent_tableref is None: throw(TranslationError, "Name %r must be defined in query" % node_name)
                parent_entity = parent_tableref.entity
                last_index = len(attr_names) - 1
                for j, attrname in enumerate(attr_names):
                    attr = parent_entity._adict_.get(attrname)
                    if attr is None: throw(AttributeError, attrname)
                    if not attr.is_collection: throw(TypeError, '%s is not collection' % ast2src(qual.iter))
                    if not isinstance(attr, Set): throw(NotImplementedError, ast2src(qual.iter))
                    entity = attr.py_type
                    if not isinstance(entity, EntityMeta): throw(NotImplementedError, ast2src(qual.iter))
                    reverse = attr.reverse
                    if reverse.is_collection:
                        if not isinstance(reverse, Set): throw(NotImplementedError, ast2src(qual.iter))
                        translator.distinct = True
                    elif parent_tableref.alias != tree.quals[i-1].assign.name:
                        translator.distinct = True
                    if j == last_index: name_path = name
                    else: name_path += '-' + attr.name
                    tableref = JoinedTableRef(translator, name_path, parent_tableref, attr)
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
                translator.conditions.append(if_.monad.getsql())
        translator.inside_expr = True
        translator.dispatch(tree.expr)
        assert not translator.hint_join
        assert not translator.inside_not
        monad = tree.expr.monad
        if isinstance(monad, translator.ParamMonad): throw(TranslationError,
            "External parameter '%s' cannot be used as query result" % ast2src(tree.expr))
        translator.groupby_monads = translator.groupby = None
        if isinstance(monad, translator.ObjectMixin):
            if monad.aggregated: throw(TranslationError)
            if translator.aggregated: translator.groupby_monads = [ monad ]
            else: translator.distinct |= monad.requires_distinct()
            alias, _ = monad.tableref.make_join()
            translator.alias = alias
            translator.expr_type = entity = monad.type
            translator.expr_columns = [ [ 'COLUMN', alias, column ] for column in entity._pk_columns_ ]
            translator.row_layout = None
            discr_criteria = entity._construct_discriminator_criteria_()
            if discr_criteria: translator.conditions.insert(0, discr_criteria)
        else:
            translator.alias = None
            if isinstance(monad, translator.ListMonad):
                expr_monads = monad.items
                translator.expr_type = tuple(m.type for m in expr_monads)
                expr_columns = []
                for m in expr_monads: expr_columns.extend(m.getsql())
                translator.expr_columns = expr_columns
            else:
                expr_monads = [ monad ]
                translator.expr_type = monad.type
                translator.expr_columns = monad.getsql()
            if translator.aggregated:
                translator.groupby_monads = [ m for m in expr_monads if not m.aggregated ]
            else: translator.distinct = True
            row_layout = []
            offset = 0
            provider = translator.database.provider
            for m in expr_monads:
                expr_type = m.type
                if isinstance(expr_type, EntityMeta):
                    next_offset = offset+len(expr_type._pk_attrs_)
                    row_layout.append((expr_type._get_by_raw_pkval_, slice(offset, next_offset)))
                    offset = next_offset
                else:
                    converter = provider.get_converter_by_py_type(expr_type)
                    row_layout.append((converter.sql2py, offset))
                    offset += 1
            translator.row_layout = row_layout

        if translator.groupby_monads:
            translator.groupby = [ 'GROUP_BY' ]
            for m in translator.groupby_monads: translator.groupby.extend(m.getsql())

        first_from_item = translator.from_[1]
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
            if op.endswith('in'):
                monads.append(right.monad.contains(left.monad, op == 'not in'))
            else: monads.append(left.monad.cmp(op, right.monad))
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
        tableref = translator.get_tableref(name)
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
        if not isinstance(arg, ast.GenExpr): return
        translator.dispatch(node.node)
        func_monad = node.node.monad
        translator.dispatch(arg)
        query_set_monad = arg.monad
        return func_monad(query_set_monad)
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
    def get_tableref(translator, name_path):
        while translator is not None:
            tableref = translator.tablerefs.get(name_path)
            if tableref is not None: return tableref
            translator = translator.parent
    def get_short_alias(translator, name_path, entity_name):
        if name_path:
            if is_ident(name_path): return name_path
            if not options.SIMPLE_ALIASES and len(name_path) <= translator.max_alias_length:
                return name_path
        name = entity_name[:translator.max_alias_length-3].lower()
        i = translator.alias_counters.setdefault(name, 0) + 1
        alias = '%s-%d' % (name, i)
        translator.alias_counters[name] = i
        return alias

class TableRef(object):
    def __init__(tableref, translator, name, entity):
        tableref.translator = translator
        tableref.alias = tableref.name_path = name
        tableref.entity = entity
        tableref.joined = False
    def make_join(tableref, pk_only=False):
        if not tableref.joined:
            tableref.translator.from_.append([ tableref.alias, 'TABLE', tableref.entity._table_ ])
            tableref.joined = True
        return tableref.alias, tableref.entity._pk_columns_

class JoinedTableRef(object):
    def __init__(tableref, translator, name_path, parent_tableref, attr, from_ast=None):
        tableref.translator = translator
        if from_ast is not None: tableref.from_ast = from_ast
        else: tableref.from_ast = translator.from_            
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
                alias = tableref.translator.get_short_alias(tableref.name_path, right_entity.__name__)
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
                alias = tableref.translator.get_short_alias(tableref.name_path, right_entity.__name__)
                join_cond = join_tables(parent_alias, alias, left_columns, pk_columns)
            tableref.from_ast.append([ alias, 'TABLE', right_entity._table_, join_cond ])
        elif not attr.reverse.is_collection:
            alias = tableref.translator.get_short_alias(tableref.name_path, right_entity.__name__)
            join_cond = join_tables(parent_alias, alias, left_pk_columns, attr.reverse.columns)
            tableref.from_ast.append([ alias, 'TABLE', right_entity._table_, join_cond ])
        else:
            if not tableref.joined:
                m2m_table = attr.table
                m2m_alias = tableref.translator.get_short_alias(None, 't')
                reverse_columns = attr.symmetric and attr.columns or attr.reverse.columns
                m2m_join_cond = join_tables(parent_alias, m2m_alias, left_pk_columns, reverse_columns)
                tableref.from_ast.append([ m2m_alias, 'TABLE', m2m_table, m2m_join_cond ])
            if attr.symmetric: right_m2m_columns = attr.reverse_columns
            else: right_m2m_columns = attr.columns
            if pk_only:
                tableref.alias = m2m_alias
                tableref.pk_columns = right_m2m_columns
                tableref.optimized = True
                tableref.joined = True
                return m2m_alias, tableref.pk_columns
            alias = tableref.translator.get_short_alias(tableref.name_path, right_entity.__name__)
            join_cond = join_tables(m2m_alias, alias, right_m2m_columns, pk_columns)
            tableref.from_ast.append([ alias, 'TABLE', right_entity._table_, join_cond ])
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
    def __call__(monad, *args, **keyargs): throw(TypeError)
    def len(monad): throw(TypeError)
    def sum(monad): throw(TypeError)
    def min(monad): throw(TypeError)
    def max(monad): throw(TypeError)
    def avg(monad): throw(TypeError)
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

class MethodMonad(Monad):
    def __init__(monad, translator, parent, attrname):
        Monad.__init__(monad, translator, 'METHOD')
        monad.parent = parent
        monad.attrname = attrname
    def __call__(monad, *args, **keyargs):
        method = getattr(monad.parent, 'call_' + monad.attrname)
        try: return method(*args, **keyargs)
        except TypeError, exc: reraise_improved_typeerror(exc, method.__name__, monad.attrname)

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

class ListMonad(Monad):
    def __init__(monad, translator, items):
        Monad.__init__(monad, translator, tuple(item.type for item in items))
        monad.items = items
    def contains(monad, x, not_in=False):
        translator = monad.translator
        for item in monad.items:
            if not translator.are_comparable_types(item.type, x.type):
                throw(IncomparableTypesError, x.type, item.type)
        left_sql = x.getsql()
        if len(left_sql) == 1:
            if not_in: sql = [ 'NOT_IN', left_sql[0], [ item.getsql()[0] for item in monad.items ] ]
            else: sql = [ 'IN', left_sql[0], [ item.getsql()[0] for item in monad.items ] ]
        elif not_in:
            sql = sqland([ sqlor([ [ 'NE', a, b ]  for a, b in zip(left_sql, item.getsql()) ]) for item in monad.items ])
        else:
            sql = sqlor([ sqland([ [ 'EQ', a, b ]  for a, b in zip(left_sql, item.getsql()) ]) for item in monad.items ])
        return translator.BoolExprMonad(translator, sql)

class BufferMixin(MonadMixin): pass

numeric_conversions = {
    (int, float): float,
    (int, Decimal): Decimal,
    }
numeric_conversions.update(((t2, t1), t3) for (t1, t2), t3 in numeric_conversions.items())

_binop_errmsg = 'Unsupported operand types %r and %r for operation %r in expression: {EXPR}'

def make_numeric_binop(op, sqlop):
    def numeric_binop(monad, monad2):
        translator = monad.translator
        if not isinstance(monad2, translator.NumericMixin):
            throw(TypeError, _binop_errmsg % (type2str(monad.type), type2str(monad2.type), op))
        t1, t2 = monad.type, monad2.type
        if t1 is t2: result_type = t1
        else: result_type = numeric_conversions.get((t1, t2))
        if result_type is None:
            throw(TypeError, _binop_errmsg % (type2str(monad.type), type2str(monad2.type), op))
        left_sql = monad.getsql()
        right_sql = monad2.getsql()
        assert len(left_sql) == len(right_sql) == 1
        return translator.NumericExprMonad(translator, result_type, [ sqlop, left_sql[0], right_sql[0] ])
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
        if not translator.are_comparable_types(item.type, monad.type, 'LIKE'):
            throw(IncomparableTypesError, item.type, monad.type)
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
        elif not translator.inside_expr:
            return translator.AttrSetMonad(monad, [ attr ])
        else:
            return translator.ObjectFlatMonad(monad, attr)
    def requires_distinct(monad):
        return monad.attr.reverse.is_collection or monad.parent.requires_distinct()

class ObjectIterMonad(ObjectMixin, Monad):
    def __init__(monad, translator, tableref, entity):
        Monad.__init__(monad, translator, entity)
        monad.tableref = tableref
    def getsql(monad):
        entity = monad.type
        alias, pk_columns = monad.tableref.make_join()
        return [ [ 'COLUMN', alias, column ] for column in pk_columns ]
    def requires_distinct(monad):
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
        return Monad.__new__(cls, parent, attr)
    def __init__(monad, parent, attr):
        assert monad.__class__ is not AttrMonad
        translator = parent.translator
        attr_type = translator.normalize_type(attr.py_type)
        Monad.__init__(monad, parent.translator, attr_type)
        monad.parent = parent
        monad.attr = attr
    def getsql(monad):
        parent = monad.parent
        attr = monad.attr
        if isinstance(parent, ObjectAttrMonad) and attr.pk_offset is not None:
            parent_columns = parent.getsql()
            entity = attr.entity
            if len(entity._pk_attrs_) == 1: return parent_columns
            return parent_columns[attr.pk_columns_offset:attr.pk_columns_offset+len(attr.columns)]
        alias, _ = monad.parent.tableref.make_join()
        return [ [ 'COLUMN', alias, column ] for column in monad.attr.columns ]
        
class ObjectAttrMonad(ObjectMixin, AttrMonad):
    def __init__(monad, parent, attr):
        AttrMonad.__init__(monad, parent, attr)
        translator = monad.translator
        parent_monad = monad.parent
        entity = monad.type
        name_path = '-'.join((parent_monad.tableref.name_path, attr.name))
        monad.tableref = translator.get_tableref(name_path)
        if monad.tableref is None:
            parent_tableref_translator = parent_monad.tableref.translator
            monad.tableref = JoinedTableRef(parent_tableref_translator, name_path, parent_monad.tableref, attr)
            parent_tableref_translator.tablerefs[name_path] = monad.tableref

flatmonad_errmsg = "aggregated expressions like {EXPR} are not allowed before 'for'"

class ObjectFlatMonad(ObjectMixin, Monad):
    def __init__(monad, parent, attr):
        translator = parent.translator
        assert translator.inside_expr
        type = translator.normalize_type(attr.py_type)
        Monad.__init__(monad, translator, type)
        monad.parent = parent
        monad.attr = attr

        translator = monad.translator
        conditions = translator.conditions
        reverse = attr.reverse
        entity = monad.type
        parent_entity = monad.parent.type

        name_path = '-'.join((parent.tableref.name_path, attr.name))
        assert translator.get_tableref(name_path) is None
        monad.tableref = JoinedTableRef(translator, name_path, parent.tableref, attr)
        translator.tablerefs[name_path] = monad.tableref
    def len(monad): throw(NotImplementedError, flatmonad_errmsg)
    def sum(monad): throw(NotImplementedError, flatmonad_errmsg)
    def min(monad): throw(NotImplementedError, flatmonad_errmsg)
    def max(monad): throw(NotImplementedError, flatmonad_errmsg)
    def avg(monad): throw(NotImplementedError, flatmonad_errmsg)
        
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
        return Monad.__new__(cls, translator, type, name, parent)
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
    def getsql(monad):
        monad.add_extractors()
        return [ [ 'PARAM', monad.name, monad.converter ] ]
    def add_extractors(monad):
        monad.translator.extractors[monad.name] = monad.extractor

class ObjectParamMonad(ObjectMixin, ParamMonad):
    def __init__(monad, translator, entity, name, parent=None):
        if translator.database is not entity._database_: throw(TranslationError, 
            'All entities in a query must belong to the same database')
        monad.params = [ '-'.join((name, path)) for path in entity._pk_paths_ ]
        ParamMonad.__init__(monad, translator, entity, name, parent)
    def getattr(monad, name):
        entity = monad.type
        try: attr = entity._adict_[name]
        except KeyError: throw(AttributeError)
        if attr.is_collection: throw(NotImplementedError)
        translator = monad.translator
        return translator.ParamMonad.new(translator, attr.py_type, name, monad)
    def getsql(monad):
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
    def requires_distinct(monad):
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
        return Monad.__new__(cls, translator, type, sql)
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
        return Monad.__new__(cls, translator, value)
    def __init__(monad, translator, value):
        value_type = translator.get_normalized_type_of(value)
        Monad.__init__(monad, translator, value_type)
        monad.value = value
    def getsql(monad):
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
    def getsql(monad):
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
    def getsql(monad):
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
        if not translator.are_comparable_types(left.type, right.type, op):
            throw(IncomparableTypesError, left.type, right.type)
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
        operands = list(operands)
        for i, operand in enumerate(operands):
            if operand.type is not bool: operands[i] = operand.nonzero()
        BoolMonad.__init__(monad, operands[0].translator)
        monad.operands = operands
    def getsql(monad):
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
    def getsql(monad):
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
        for arg in args:
            assert isinstance(arg, Monad)
        for value in keyargs.values():
            assert isinstance(value, Monad)
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

class FuncAbsMonad(FuncMonad):
    func = abs
    def call(monad, x):
        return x.abs()

class FuncSumMonad(FuncMonad):
    func = sum
    def call(monad, x):
        return x.sum()

class FuncAvgMonad(FuncMonad):
    func = avg
    def call(monad, x):
        return x.avg()

class FuncMinMonad(FuncMonad):
    func = min
    def call(monad, *args):
        if not args: throw(TypeError, 'min expected at least one argument')
        if len(args) == 1: return args[0].min()
        return minmax(monad, 'MIN', *args)

class FuncMaxMonad(FuncMonad):
    func = max
    def call(monad, *args):
        if not args: throw(TypeError, 'max expected at least one argument')
        if len(args) == 1: return args[0].max()
        return minmax(monad, 'MAX', *args)

def minmax(monad, sqlop, *args):
    assert len(args) > 1
    translator = monad.translator
    sql = [ sqlop ] + [ arg.getsql()[0] for arg in args ]
    arg_types = [ arg.type for arg in args ]
    t = arg_types[0]
    if t not in translator.comparable_types: throw(TypeError, 
        "Value of type %r is not valid as argument of %r function in expression {EXPR}"
        % (type2str(t), sqlop.lower()))
    for t2 in arg_types[1:]:
        t3 = translator.coerce_types(t, t2)
        if t3 is None: throw(IncomparableTypesError, t, t2)
        t = t3
    return translator.ExprMonad.new(translator, t, sql)

class FuncSelectMonad(FuncMonad):
    func = query
    def call(monad, subquery):
        translator = monad.translator
        if not isinstance(subquery, translator.QuerySetMonad): throw(TypeError, 
            "'query' function expects generator expression, got: {EXPR}")
        return subquery

class FuncExistsMonad(FuncMonad):
    func = exists
    def call(monad, subquery):
        if not isinstance(subquery, monad.translator.SetMixin): throw(TypeError, 
            "'exists' function expects generator expression or collection, got: {EXPR}")
        return subquery.nonzero()

class FuncAggrMonad(FuncMonad):
    def call(monad, x):
        if x.aggregated: throw(TranslationError, 'Aggregated functions cannot be nested. Got: {EXPR}')
        translator = monad.translator
        translator.aggregated = True
        expr_type = x.type
        func_name = monad.func.__name__
        if func_name in ('SUM', 'AVG') and expr_type not in translator.numeric_types:
            throw(TypeError, "Function '%s' expects argument of numeric type, got %r in {EXPR}"
                             % (func_name, type2str(expr_type)))
        elif func_name == 'COUNT' and isinstance(expr_type, EntityMeta): pass
        elif expr_type not in translator.comparable_types:
            throw(TypeError, "Function '%s' cannot be applied to type %r in {EXPR}"
                             % (func_name, type2str(expr_type)))
        expr = x.getsql()
        if len(expr) == 1: expr = expr[0]
        elif translator.row_value_syntax == True: expr = ['ROW'] + expr
        else: throw(NotImplementedError, 'Entities with composite primary keys '
                    'not supported inside aggregate functions. Got: {EXPR}')
        def new_expr(type, sql): return translator.ExprMonad.new(translator, type, sql)
        if func_name == 'COUNT':
            result = new_expr(int, [ 'COUNT', 'DISTINCT', expr ])
        else:
            if func_name == 'AVG': result_type = float
            else: result_type = x.type
            result = new_expr(result_type, [ func_name, expr ])
        result.aggregated = True
        return result

class FuncCountMonad(FuncAggrMonad): func = COUNT
class FuncSumMonad(FuncAggrMonad): func = SUM
class FuncMinMonad(FuncAggrMonad): func = MIN
class FuncMaxMonad(FuncAggrMonad): func = MAX
class FuncAvgMonad(FuncAggrMonad): func = AVG

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

class AttrSetMonad(SetMixin, Monad):
    def __init__(monad, root, path):
        if root.translator.inside_expr: throw(NotImplementedError)
        translator = root.translator
        item_type = translator.normalize_type(path[-1].py_type)
        Monad.__init__(monad, translator, SetType(item_type))
        monad.root = root
        monad.path = path
    def cmp(monad, op, monad2):
        if type(monad2.type) is SetType and monad.translator.are_comparable_types(monad.type.item_type, monad2.type.item_type): pass
        elif monad.type != monad2.type: throw(IncomparableTypesError, monad.type, monad2.type)
        throw(NotImplementedError)
    def contains(monad, item, not_in=False):
        translator = monad.translator
        item_type = monad.type.item_type
        if not translator.are_comparable_types(item.type, item_type):
            throw(IncomparableTypesError, item.type, item_type)
        if not translator.hint_join:
            sqlop = not_in and 'NOT_IN' or 'IN'
            expr_list, from_ast, inner_conditions, outer_conditions = monad._subselect()
            conditions = outer_conditions + inner_conditions
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
        item_type = monad.type.item_type
        if not isinstance(item_type, EntityMeta):
            throw(AttributeError)
        entity = item_type
        attr = entity._adict_.get(name)
        if attr is None: throw(AttributeError)
        return monad.translator.AttrSetMonad(monad.root, monad.path + [ attr ])
    def len(monad):
        expr_list, from_ast, inner_conditions, outer_conditions = monad._subselect()
        distinct = False
        for attr in monad.path:
            reverse = attr.reverse
            if reverse and reverse.is_collection:
                distinct = True
                break
        if not distinct:
            sql_ast = [ 'SELECT', [ 'AGGREGATES', [ 'COUNT', 'ALL' ] ],
                        from_ast, [ 'WHERE' ] + outer_conditions + inner_conditions ]
        else:
            sql_ast = [ 'SELECT', [ 'AGGREGATES', [ 'COUNT', 'ALL' ] ],
                      [ 'FROM', [ 't', 'SELECT', [ [ 'DISTINCT' ] + expr_list,
                                     from_ast, [ 'WHERE' ] + outer_conditions + inner_conditions ] ] ] ]
        translator = monad.translator
        return translator.NumericExprMonad(translator, int, sql_ast)
    def sum(monad):
        translator = monad.translator
        item_type = monad.type.item_type
        if item_type not in translator.numeric_types: throw(TypeError, 
            "Function 'sum' expects query or items of numeric type, got %r in {EXPR}" % type2str(item_type))
        expr_list, from_ast, inner_conditions, outer_conditions = monad._subselect()
        assert len(expr_list) == 1
        expr = expr_list[0]
        sql_ast = [ 'SELECT', [ 'AGGREGATES', [ 'SUM', expr ] ], from_ast, [ 'WHERE' ] + outer_conditions + inner_conditions ]
        return translator.NumericExprMonad(translator, item_type, sql_ast)
    def avg(monad):
        translator = monad.translator
        item_type = monad.type.item_type
        if item_type not in translator.numeric_types: throw(TypeError, 
            "Function 'avg' expects query or items of numeric type, got %r in {EXPR}" % type2str(item_type))
        expr_list, from_ast, inner_conditions, outer_conditions = monad._subselect()
        assert len(expr_list) == 1
        expr = expr_list[0]
        sql_ast = [ 'SELECT', [ 'AGGREGATES', [ 'AVG', expr ] ], from_ast, [ 'WHERE' ] + outer_conditions + inner_conditions ]
        return translator.NumericExprMonad(translator, float, sql_ast)
    def min(monad):
        translator = monad.translator
        item_type = monad.type.item_type
        if item_type not in translator.comparable_types: throw(TypeError, 
            "Function 'min' expects query or items of numeric type, got %r in {EXPR}" % type2str(item_type))
        expr_list, from_ast, inner_conditions, outer_conditions = monad._subselect()
        assert len(expr_list) == 1
        expr = expr_list[0]
        sql_ast = [ 'SELECT', [ 'AGGREGATES', [ 'MIN', expr ] ], from_ast, [ 'WHERE' ] + outer_conditions + inner_conditions ]
        return translator.ExprMonad.new(translator, item_type, sql_ast)
    def max(monad):
        translator = monad.translator
        item_type = monad.type.item_type
        if item_type not in translator.comparable_types: throw(TypeError, 
            "Function 'max' expects query or items of numeric type, got %r in {EXPR}" % type2str(item_type))
        expr_list, from_ast, inner_conditions, outer_conditions = monad._subselect()
        assert len(expr_list) == 1
        expr = expr_list[0]
        if translator.hint_join:
            alias = translator.get_short_alias(None, 't')
            groupby_columns = [ inner_column[:] for cond, outer_column, inner_column in outer_conditions ]
            assert len(set(alias for _, alias, column in groupby_columns)) == 1
            groupby_names = set(column for _, alias, column in groupby_columns)
            while True:            
                expr_name = 'column-%d' % translator.expr_counter()
                if expr_name not in groupby_names: break

            subquery_columns = [ 'ALL' ]
            for column in groupby_columns:
                if column[0] == 'COLUMN': # Workaround for SQLite 3.3.4 bug appeared in vanilla Python 2.5
                    column = [ 'AS', column, column[-1] ]
                subquery_columns.append(column)
            subquery_columns.append([ 'AS', [ 'MAX', expr ], expr_name ])

            subquery_ast = [ subquery_columns, from_ast ]
            if inner_conditions: subquery_ast.append([ 'WHERE' ] + inner_conditions)
            subquery_ast.append([ 'GROUP_BY' ] + groupby_columns)

            for cond in outer_conditions: cond[2][1] = alias

            translator.from_.append([ alias, 'SELECT', subquery_ast, sqland(outer_conditions) ])
            sql_ast = [ 'COLUMN', alias, expr_name ]
        else:
            sql_ast = [ 'SELECT', [ 'AGGREGATES', [ 'MAX', expr ] ],
                                from_ast,
                                [ 'WHERE' ] + outer_conditions + inner_conditions ]
        return translator.ExprMonad.new(monad.translator, item_type, sql_ast)
    def nonzero(monad):
        expr_list, from_ast, inner_conditions, outer_conditions = monad._subselect()
        sql_ast = [ 'EXISTS', from_ast, [ 'WHERE' ] + outer_conditions + inner_conditions ]
        translator = monad.translator
        return translator.BoolExprMonad(translator, sql_ast)
    def negate(monad):
        expr_list, from_ast, inner_conditions, outer_conditions = monad._subselect()
        sql_ast = [ 'NOT_EXISTS', from_ast, [ 'WHERE' ] + outer_conditions + inner_conditions ]
        translator = monad.translator
        return translator.BoolExprMonad(translator, sql_ast)
    def _subselect(monad):
        path = monad.path[:]
        from_ast = [ 'FROM' ]
        tableref = monad.root.tableref
        if not path[-1].reverse:
            nonlink_attr = path.pop()
            assert path
        else: nonlink_attr = None
        for attr in path:
            tableref = JoinedTableRef(monad.translator, None, tableref, attr, from_ast)
        pk_only = not nonlink_attr or nonlink_attr.pk_offset is not None
        alias, columns = tableref.make_join(pk_only)
        inner_conditions = []
        if nonlink_attr:
            if pk_only:
                offset = nonlink_attr.pk_columns_offset
                columns = columns[offset:offset+len(nonlink_attr.columns)]
            else: columns = nonlink_attr.columns
        expr_list = [[ 'COLUMN', alias, column ] for column in columns ]
        if nonlink_attr is not None and not nonlink_attr.is_required:
            inner_conditions = [ [ 'IS_NOT_NULL', expr ] for expr in expr_list ]
        outer_conditions = [ from_ast[1].pop() ]
        return expr_list, from_ast, inner_conditions, outer_conditions
    def getsql(monad):
        throw(TranslationError)

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
        item_type = monad.type.item_type
        if not translator.are_comparable_types(item.type, item_type):
            throw(IncomparableTypesError, item.type, item_type)
        sub = monad.subtranslator
        columns_ast = sub.expr_columns
        conditions = sub.conditions[:]
        if not translator.hint_join:
            if len(columns_ast) == 1 or translator.row_value_syntax:
                select_ast = [ 'ALL' ] + columns_ast
                subquery_ast = [ 'SELECT', select_ast, sub.from_ ]
                subquery_expr = sub.tree.expr.monad
                if isinstance(subquery_expr, AttrMonad) and subquery_expr.attr.is_required: pass
                else: conditions += [ [ 'IS_NOT_NULL', column_ast ] for column_ast in sub.expr_columns ]
                if conditions: subquery_ast.append([ 'WHERE' ] + conditions)
                if len(columns_ast) == 1: expr_ast = item.getsql()[0]
                else: expr_ast = [ 'ROW' ] + item.getsql()
                sql_ast = [ not_in and 'NOT_IN' or 'IN', expr_ast, subquery_ast ]
                return translator.BoolExprMonad(translator, sql_ast)
            else:
                if isinstance(item, ListMonad):
                    item_columns = []
                    for subitem in item.items: item_columns.extend(subitem.getsql())
                else: item_columns = item.getsql()
                conditions += [ [ 'EQ', expr1, expr2 ] for expr1, expr2 in izip(item_columns, columns_ast) ]
                subquery_ast = [ not_in and 'NOT_EXISTS' or 'EXISTS', sub.from_, [ 'WHERE' ] + conditions ]
                return translator.BoolExprMonad(translator, subquery_ast)
        else: throw(NotImplementedError)
    def nonzero(monad):        
        sub = monad.subtranslator
        sql_ast = [ 'EXISTS', sub.from_, [ 'WHERE' ] + sub.conditions ]
        translator = monad.translator
        return translator.BoolExprMonad(translator, sql_ast)
    def negate(monad):
        sub = monad.subtranslator
        sql_ast = [ 'NOT_EXISTS', sub.from_, [ 'WHERE' ] + sub.conditions ]
        translator = monad.translator
        return translator.BoolExprMonad(translator, sql_ast)
    def _subselect(monad, item_type, select_ast):
        sub = monad.subtranslator
        sql_ast = [ 'SELECT', select_ast, sub.from_, [ 'WHERE' ] + sub.conditions ]
        translator = monad.translator
        return translator.ExprMonad.new(translator, item_type, sql_ast)
    def len(monad):
        sub = monad.subtranslator
        expr_type = sub.expr_type
        if isinstance(expr_type, (tuple, EntityMeta)):
            if not sub.distinct:
                select_ast = [ 'AGGREGATES', [ 'COUNT', 'ALL' ] ]
                return monad._subselect(int, select_ast)
            translator = monad.translator
            alias = translator.get_short_alias(None, 't')
            sql_ast = [ 'SELECT', [ 'AGGREGATES', [ 'COUNT', 'ALL' ] ],
                                [ 'FROM', [ alias, 'SELECT', [ [ 'DISTINCT' ] + sub.expr_columns,
                                                 sub.from_, [ 'WHERE' ] + sub.conditions ] ] ] ]
            return translator.ExprMonad.new(translator, int, sql_ast)
        elif len(sub.expr_columns) == 1:
            select_ast = [ 'AGGREGATES', [ 'COUNT', 'DISTINCT', sub.expr_columns[0] ] ]
            return monad._subselect(int, select_ast)
        else: throw(NotImplementedError)
    def sum(monad):
        translator = monad.translator
        sub = monad.subtranslator
        expr_type = sub.expr_type
        if expr_type not in translator.numeric_types: throw(TypeError, 
            "Function 'sum' expects query or items of numeric type, got %r in {EXPR}" % type2str(expr_type))
        assert len(sub.expr_columns) == 1
        select_ast = [ 'AGGREGATES', [ 'SUM', sub.expr_columns[0] ] ]
        return monad._subselect(expr_type, select_ast)
    def avg(monad):
        translator = monad.translator
        sub = monad.subtranslator
        expr_type = sub.expr_type
        if expr_type not in translator.numeric_types: throw(TypeError, 
            "Function 'avg' expects query or items of numeric type, got %r in {EXPR}" % type2str(expr_type))
        assert len(sub.expr_columns) == 1
        select_ast = [ 'AGGREGATES', [ 'AVG', sub.expr_columns[0] ] ]
        return monad._subselect(float, select_ast)
    def min(monad):
        translator = monad.translator
        sub = monad.subtranslator
        expr_type = sub.expr_type
        if expr_type not in translator.comparable_types: throw(TypeError, 
            "Function 'min' cannot be applied to type %r in {EXPR}" % type2str(expr_type))
        assert len(sub.expr_columns) == 1
        select_ast = [ 'AGGREGATES', [ 'MIN', sub.expr_columns[0] ] ]
        return monad._subselect(expr_type, select_ast)
    def max(monad):
        translator = monad.translator
        sub = monad.subtranslator
        expr_type = sub.expr_type
        if expr_type not in translator.comparable_types: throw(TypeError, 
            "Function 'max' cannot be applied to type %r in {EXPR}" % type2str(expr_type))
        assert len(sub.expr_columns) == 1
        select_ast = [ 'AGGREGATES', [ 'MAX', sub.expr_columns[0] ] ]
        return monad._subselect(expr_type, select_ast)

for name, value in globals().items():
    if name.endswith('Monad') or name.endswith('Mixin'):
        setattr(SQLTranslator, name, value)
del name, value
