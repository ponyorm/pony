from __future__ import absolute_import, print_function, division
from pony.py23compat import PY2, items_list, izip, xrange, basestring, unicode, buffer, with_metaclass, int_types

import types, sys, re, itertools, inspect
from decimal import Decimal
from datetime import date, time, datetime, timedelta
from random import random
from copy import deepcopy
from functools import update_wrapper
from uuid import UUID

from pony.thirdparty.compiler import ast

from pony import options, utils
from pony.utils import localbase, is_ident, throw, reraise, copy_ast, between, concat, coalesce
from pony.orm.asttranslation import ASTTranslator, ast2src, TranslationError, create_extractors
from pony.orm.decompiling import decompile, DecompileError
from pony.orm.ormtypes import \
    numeric_types, comparable_types, SetType, FuncType, MethodType, raw_sql, RawSQLType, \
    normalize, normalize_type, coerce_types, are_comparable_types, \
    Json, QueryType, Array, array_types
from pony.orm import core
from pony.orm.core import EntityMeta, Set, JOIN, OptimizationFailed, Attribute, DescWrapper, \
    special_functions, const_functions, extract_vars, Query, UseAnotherTranslator

NoneType = type(None)

def check_comparable(left_monad, right_monad, op='=='):
    t1, t2 = left_monad.type, right_monad.type
    if t1 == 'METHOD': raise_forgot_parentheses(left_monad)
    if t2 == 'METHOD': raise_forgot_parentheses(right_monad)
    if not are_comparable_types(t1, t2, op):
        if op in ('in', 'not in') and isinstance(t2, SetType): t2 = t2.item_type
        throw(IncomparableTypesError, t1, t2)

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

def type2str(t):
    if type(t) is tuple: return 'list'
    if type(t) is SetType: return 'Set of ' + type2str(t.item_type)
    try: return t.__name__
    except: return str(t)

class Local(localbase):
    def __init__(local):
        local.translators = []

    @property
    def translator(self):
        return local.translators[-1]

local = Local()

class SQLTranslator(ASTTranslator):
    dialect = None
    row_value_syntax = True
    json_path_wildcard_syntax = False
    json_values_are_comparable = True
    rowid_support = False

    def __enter__(translator):
        local.translators.append(translator)

    def __exit__(translator, exc_type, exc_val, exc_tb):
        t = local.translators.pop()
        if isinstance(exc_val, UseAnotherTranslator):
            assert t is exc_val.translator
        else:
            assert t is translator

    def default_post(translator, node):
        throw(NotImplementedError)  # pragma: no cover

    def dispatch(translator, node):
        if hasattr(node, 'monad'): return  # monad already assigned somehow
        if not getattr(node, 'external', False) or getattr(node, 'constant', False):
            return ASTTranslator.dispatch(translator, node)  # default route
        translator.call(translator.__class__.dispatch_external, node)

    def dispatch_external(translator, node):
        varkey = translator.filter_num, node.src, translator.code_key
        t = translator.root_translator.vartypes[varkey]
        tt = type(t)
        if t is NoneType:
            monad = ConstMonad.new(None)
        elif tt is SetType:
            if isinstance(t.item_type, EntityMeta):
                monad = EntityMonad(t.item_type)
            else: throw(NotImplementedError)  # pragma: no cover
        elif tt is QueryType:
            prev_translator = deepcopy(t.translator)
            prev_translator.parent = translator
            prev_translator.injected = True
            if translator.database is not prev_translator.database:
                throw(TranslationError, 'Mixing queries from different databases')
            monad = QuerySetMonad(prev_translator)
            if t.limit is not None or t.offset is not None:
                monad = monad.call_limit(t.limit, t.offset)
        elif tt is FuncType:
            func = t.func
            func_monad_class = translator.registered_functions.get(func)
            if func_monad_class is not None:
                monad = func_monad_class(func)
            else:
                monad = HybridFuncMonad(t, func.__name__)
        elif tt is MethodType:
            obj, func = t.obj, t.func
            if isinstance(obj, EntityMeta):
                entity_monad = EntityMonad(obj)
                if obj.__class__.__dict__.get(func.__name__) is not func: throw(NotImplementedError)
                monad = MethodMonad(entity_monad, func.__name__)
            elif node.src == 'random':  # For PyPy
                monad = FuncRandomMonad(t)
            else: throw(NotImplementedError)
        elif isinstance(node, ast.Name) and node.name in ('True', 'False'):
            value = True if node.name == 'True' else False
            monad = ConstMonad.new(value)
        elif tt is tuple:
            params = []
            is_array = False
            if t and translator.database.provider.array_converter_cls is not None:
                types = set(t)
                if len(types) == 1 and unicode in types:
                    item_type = unicode
                    is_array = True
                else:
                    item_type = int
                    for type_ in types:
                        if type_ is float:
                            item_type = float
                        if type_ not in (float, int) or not hasattr(type_, '__index__'):
                            break
                    else:
                        is_array = True

            for i, item_type in enumerate(t):
                if item_type is NoneType:
                    throw(TypeError, 'Expression `%s` should not contain None values' % node.src)
                param = ParamMonad.new(item_type, (varkey, i, None))
                params.append(param)
            monad = ListMonad(params)
            if is_array:
                array_type = array_types.get(item_type, None)
                monad = ArrayParamMonad(array_type, (varkey, None, None), list_monad=monad)
        elif isinstance(t, RawSQLType):
            monad = RawSQLMonad(t, varkey)
        else:
            monad = ParamMonad.new(t, (varkey, None, None))
        node.monad = monad
        monad.node = node
        monad.aggregated = monad.nogroup = False

    def call(translator, method, node):
        try: monad = method(translator, node)
        except Exception:
            exc_class, exc, tb = sys.exc_info()
            try:
                if not exc.args: exc.args = (ast2src(node),)
                else:
                    msg = exc.args[0]
                    if isinstance(msg, basestring) and '{EXPR}' in msg:
                        msg = msg.replace('{EXPR}', ast2src(node))
                        exc.args = (msg,) + exc.args[1:]
                reraise(exc_class, exc, tb)
            finally: del exc, tb
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
            if not hasattr(monad, 'nogroup'):
                for child in node.getChildNodes():
                    m = getattr(child, 'monad', None)
                    if m and getattr(m, 'nogroup', False):
                        monad.nogroup = True
                        break
                else: monad.nogroup = False
            if monad.aggregated:
                translator.aggregated = True
                if monad.nogroup:
                    if isinstance(monad, ListMonad): pass
                    elif isinstance(monad, AndMonad): pass
                    else: throw(TranslationError, 'Too complex aggregation, expressions cannot be combined: %s' % ast2src(node))
            return monad

    def __init__(translator, tree, parent_translator, code_key=None, filter_num=None, extractors=None, vars=None, vartypes=None, left_join=False, optimize=None):
        local.translators.append(translator)
        try:
            translator.init(tree, parent_translator, code_key, filter_num, extractors, vars, vartypes, left_join, optimize)
        except UseAnotherTranslator as e:
            translator = e.translator
            raise
        finally:
            assert local.translators
            t = local.translators.pop()
            assert t is translator

    def init(translator, tree, parent_translator, code_key=None, filter_num=None, extractors=None, vars=None, vartypes=None, left_join=False, optimize=None):
        this = translator
        assert isinstance(tree, ast.GenExprInner), tree
        ASTTranslator.__init__(translator, tree)
        translator.can_be_cached = True
        translator.parent = parent_translator
        translator.injected = False
        if parent_translator is None:
            translator.root_translator = translator
            translator.database = None
            translator.sqlquery = SqlQuery(translator, left_join=left_join)
            assert code_key is not None and filter_num is not None
            translator.code_key = translator.original_code_key = code_key
            translator.filter_num = translator.original_filter_num = filter_num
        else:
            translator.root_translator = parent_translator.root_translator
            translator.database = parent_translator.database
            translator.sqlquery = SqlQuery(translator, parent_translator.sqlquery, left_join=left_join)
            assert code_key is None and filter_num is None
            translator.code_key = parent_translator.code_key
            translator.filter_num = parent_translator.filter_num
            translator.original_code_key = translator.original_filter_num = None
        translator.extractors = extractors
        translator.vars = vars
        translator.vartypes = vartypes
        translator.namespace_stack = [{}] if not parent_translator else [ parent_translator.namespace.copy() ]
        translator.func_extractors_map = {}
        translator.fixed_param_values = {}
        translator.func_vartypes = {}
        translator.left_join = left_join
        translator.optimize = optimize
        translator.from_optimized = False
        translator.optimization_failed = False
        translator.distinct = False
        translator.conditions = translator.sqlquery.conditions
        translator.having_conditions = []
        translator.order = []
        translator.limit = translator.offset = None
        translator.inside_order_by = False
        translator.aggregated = False if not optimize else True
        translator.hint_join = False
        translator.query_result_is_cacheable = True
        translator.aggregated_subquery_paths = set()
        for i, qual in enumerate(tree.quals):
            assign = qual.assign
            if isinstance(assign, ast.AssTuple):
                ass_names = tuple(assign.nodes)
            elif isinstance(assign, ast.AssName):
                ass_names = (assign,)
            else:
                throw(NotImplementedError, ast2src(assign))

            for ass_name in ass_names:
                if not isinstance(ass_name, ast.AssName):
                    throw(NotImplementedError, ast2src(ass_name))
                if ass_name.flags != 'OP_ASSIGN':
                    throw(TypeError, ast2src(ass_name))

            names = tuple(ass_name.name for ass_name in ass_names)
            for name in names:
                if name in translator.namespace and name in translator.sqlquery.tablerefs:
                    throw(TranslationError, 'Duplicate name: %r' % name)
                if name.startswith('__'): throw(TranslationError, 'Illegal name: %r' % name)

            name = names[0] if len(names) == 1 else None

            def check_name_is_single():
                if len(names) > 1: throw(TypeError, 'Single variable name expected. Got: %s' % ast2src(assign))

            database = entity = None

            node = qual.iter
            monad = getattr(node, 'monad', None)

            if monad:  # Lambda was encountered inside generator
                check_name_is_single()
                assert parent_translator and i == 0
                entity = monad.type.item_type
                if isinstance(monad, EntityMonad):
                    tableref = TableRef(translator.sqlquery, name, entity)
                    translator.sqlquery.tablerefs[name] = tableref
                elif isinstance(monad, AttrSetMonad):
                    translator.sqlquery = monad._subselect(translator.sqlquery, extract_outer_conditions=False)
                    tableref = monad.tableref
                else: assert False  # pragma: no cover
                translator.namespace[name] = ObjectIterMonad(tableref, entity)
            elif node.external:
                varkey = translator.filter_num, node.src, translator.code_key
                iterable = translator.root_translator.vartypes[varkey]
                if isinstance(iterable, SetType):
                    check_name_is_single()
                    entity = iterable.item_type
                    if not isinstance(entity, EntityMeta):
                        throw(TranslationError, 'for %s in %s' % (name, ast2src(qual.iter)))
                    if i > 0:
                        if translator.left_join: throw(TranslationError,
                                                       'Collection expected inside left join query. '
                                                       'Got: for %s in %s' % (name, ast2src(qual.iter)))
                        translator.distinct = True
                    tableref = TableRef(translator.sqlquery, name, entity)
                    translator.sqlquery.tablerefs[name] = tableref
                    tableref.make_join()
                    translator.namespace[name] = node.monad = ObjectIterMonad(tableref, entity)
                elif isinstance(iterable, QueryType):
                    prev_translator = deepcopy(iterable.translator)
                    prev_limit = iterable.limit
                    prev_offset = iterable.offset
                    database = prev_translator.database
                    try:
                        translator.process_query_qual(prev_translator, prev_limit, prev_offset,
                                                      names, try_extend_prev_query=not i)
                    except UseAnotherTranslator as e:
                        assert local.translators and local.translators[-1] is translator
                        translator = e.translator
                        local.translators[-1] = translator
                else: throw(TranslationError, 'Inside declarative query, iterator must be entity or query. '
                                              'Got: for %s in %s' % (name, ast2src(qual.iter)))

            else:
                translator.dispatch(node)
                monad = node.monad

                if isinstance(monad, QuerySetMonad):
                    subtranslator = monad.subtranslator
                    database = subtranslator.database
                    try:
                        translator.process_query_qual(subtranslator, monad.limit, monad.offset, names)
                    except UseAnotherTranslator:
                        assert False
                else:
                    check_name_is_single()
                    attr_names = []
                    while isinstance(monad, (AttrMonad, AttrSetMonad)) and monad.parent is not None:
                        attr_names.append(monad.attr.name)
                        monad = monad.parent
                    attr_names.reverse()

                    if not isinstance(monad, ObjectIterMonad):
                        throw(NotImplementedError, 'for %s in %s' % (name, ast2src(qual.iter)))
                    name_path = monad.tableref.alias  # or name_path, it is the same

                    parent_tableref = monad.tableref
                    parent_entity = parent_tableref.entity

                    last_index = len(attr_names) - 1
                    for j, attrname in enumerate(attr_names):
                        attr = parent_entity._adict_.get(attrname)
                        if attr is None: throw(AttributeError, attrname)
                        entity = attr.py_type
                        if not isinstance(entity, EntityMeta):
                            throw(NotImplementedError, 'for %s in %s' % (name, ast2src(qual.iter)))
                        can_affect_distinct = None
                        if attr.is_collection:
                            if not isinstance(attr, Set): throw(NotImplementedError, ast2src(qual.iter))
                            reverse = attr.reverse
                            if reverse.is_collection:
                                if not isinstance(reverse, Set): throw(NotImplementedError, ast2src(qual.iter))
                                translator.distinct = True
                            elif parent_tableref.alias != tree.quals[i-1].assign.name:
                                translator.distinct = True
                            else: can_affect_distinct = True
                        if j == last_index: name_path = name
                        else: name_path += '-' + attr.name
                        tableref = translator.sqlquery.add_tableref(name_path, parent_tableref, attr)
                        tableref.make_join(pk_only=True)
                        if j == last_index:
                            translator.namespace[name] = ObjectIterMonad(tableref, tableref.entity)
                        if can_affect_distinct is not None:
                            tableref.can_affect_distinct = can_affect_distinct
                        parent_tableref = tableref
                        parent_entity = entity

            if database is None:
                assert entity is not None
                database = entity._database_
            assert database.schema is not None
            if translator.database is None: translator.database = database
            elif translator.database is not database: throw(TranslationError,
                'All entities in a query must belong to the same database')

            for if_ in qual.ifs:
                assert isinstance(if_, ast.GenExprIf)
                translator.dispatch(if_)
                if isinstance(if_.monad, AndMonad): cond_monads = if_.monad.operands
                else: cond_monads = [ if_.monad ]
                for m in cond_monads:
                    if not getattr(m, 'aggregated', False): translator.conditions.extend(m.getsql())
                    else: translator.having_conditions.extend(m.getsql())

        translator.dispatch(tree.expr)
        assert not translator.hint_join
        monad = tree.expr.monad
        if isinstance(monad, ParamMonad): throw(TranslationError,
            "External parameter '%s' cannot be used as query result" % ast2src(tree.expr))
        translator.expr_monads = monad.items if isinstance(monad, ListMonad) else [ monad ]
        translator.groupby_monads = None
        expr_type = monad.type
        if isinstance(expr_type, SetType): expr_type = expr_type.item_type
        if isinstance(expr_type, EntityMeta):
            entity = expr_type
            translator.expr_type = entity
            monad.orderby_columns = list(xrange(1, len(entity._pk_columns_)+1))
            if monad.aggregated: throw(TranslationError)
            if isinstance(monad, QuerySetMonad):
                throw(NotImplementedError)
            elif isinstance(monad, ObjectMixin):
                tableref = monad.tableref
            elif isinstance(monad, AttrSetMonad):
                tableref = monad.make_tableref(translator.sqlquery)
            else: assert False  # pragma: no cover
            if translator.aggregated:
                translator.groupby_monads = [ monad ]
            else:
                translator.distinct |= monad.requires_distinct()
            translator.tableref = tableref
            pk_only = parent_translator is not None or translator.aggregated
            alias, pk_columns = tableref.make_join(pk_only=pk_only)
            translator.alias = alias
            translator.expr_columns = [ [ 'COLUMN', alias, column ] for column in pk_columns ]
            translator.row_layout = None
            translator.col_names = [ attr.name for attr in entity._attrs_
                                               if not attr.is_collection and not attr.lazy ]
        else:
            translator.alias = None
            expr_monads = translator.expr_monads
            if len(expr_monads) > 1:
                translator.expr_type = tuple(m.type for m in expr_monads)  # ?????
                expr_columns = []
                for m in expr_monads: expr_columns.extend(m.getsql())
                translator.expr_columns = expr_columns
            else:
                translator.expr_type = monad.type
                translator.expr_columns = monad.getsql()
            if translator.aggregated:
                translator.groupby_monads = [ m for m in expr_monads if not m.aggregated and not m.nogroup ]
            else:
                expr_set = set()
                for m in expr_monads:
                    if isinstance(m, ObjectIterMonad):
                        expr_set.add(m.tableref.name_path)
                    elif isinstance(m, AttrMonad) and isinstance(m.parent, ObjectIterMonad):
                        expr_set.add((m.parent.tableref.name_path, m.attr))
                for tr in translator.sqlquery.tablerefs.values():
                    if tr.entity is None: continue
                    if not tr.can_affect_distinct: continue
                    if tr.name_path in expr_set: continue
                    if any((tr.name_path, attr) not in expr_set for attr in tr.entity._pk_attrs_):
                        translator.distinct = True
                        break
            row_layout = []
            offset = 0
            provider = translator.database.provider
            for m in expr_monads:
                if m.disable_distinct:
                    translator.distinct = False
                expr_type = m.type
                if isinstance(expr_type, SetType): expr_type = expr_type.item_type
                if isinstance(expr_type, EntityMeta):
                    next_offset = offset + len(expr_type._pk_columns_)
                    def func(values, constructor=expr_type._get_by_raw_pkval_):
                        if None in values: return None
                        return constructor(values)
                    row_layout.append((func, slice(offset, next_offset), ast2src(m.node)))
                    m.orderby_columns = list(xrange(offset+1, next_offset+1))
                    offset = next_offset
                else:
                    converter = provider.get_converter_by_py_type(expr_type)
                    def func(value, converter=converter):
                        if value is None: return None
                        value = converter.sql2py(value)
                        value = converter.dbval2val(value)
                        return value
                    row_layout.append((func, offset, ast2src(m.node)))
                    m.orderby_columns = (offset+1,) if not m.disable_ordering else ()
                    offset += 1
            translator.row_layout = row_layout
            translator.col_names = [ src for func, slice_or_offset, src in translator.row_layout ]
        if translator.aggregated:
            translator.distinct = False
        translator.vars = None
        if translator is not this:
            raise UseAnotherTranslator(translator)
    @property
    def namespace(translator):
        return translator.namespace_stack[-1]
    def can_be_optimized(translator):
        if translator.groupby_monads: return False
        if len(translator.aggregated_subquery_paths) != 1: return False
        aggr_path = next(iter(translator.aggregated_subquery_paths))
        for tableref in translator.sqlquery.tablerefs.values():
            if tableref.joined and not aggr_path.startswith(tableref.name_path):
                return False
        return aggr_path
    def process_query_qual(translator, prev_translator, prev_limit, prev_offset, names, try_extend_prev_query=False):
        sqlquery = translator.sqlquery
        tablerefs = sqlquery.tablerefs
        expr_types = prev_translator.expr_type
        if not isinstance(expr_types, tuple): expr_types = (expr_types,)
        expr_count = len(expr_types)

        if expr_count > 1 and len(names) == 1:
            throw(NotImplementedError,
                  'Please unpack a tuple of (%s) in for-loop to individual variables (like: "for x, y in ...")'
                  % (', '.join(ast2src(m.node) for m in prev_translator.expr_monads)))
        elif expr_count > len(names):
            throw(TranslationError,
                  'Not enough values to unpack "for %s in select(%s for ...)" (expected %d, got %d)'
                  % (', '.join(names),
                     ', '.join(ast2src(m.node) for m in prev_translator.expr_monads),
                     len(names), expr_count))
        elif expr_count < len(names):
            throw(TranslationError,
                  'Too many values to unpack "for %s in select(%s for ...)" (expected %d, got %d)'
                  % (', '.join(names),
                     ', '.join(ast2src(m.node) for m in prev_translator.expr_monads),
                     len(names), expr_count))

        if try_extend_prev_query:
            if prev_translator.aggregated: pass
            elif prev_translator.left_join: pass
            else:
                assert translator.parent is None
                assert prev_translator.vars is None
                prev_translator.code_key = translator.code_key
                prev_translator.filter_num = translator.filter_num
                prev_translator.extractors.update(translator.extractors)
                prev_translator.vars = translator.vars
                prev_translator.vartypes.update(translator.vartypes)
                prev_translator.left_join = translator.left_join
                prev_translator.optimize = translator.optimize
                prev_translator.namespace_stack = [
                    {name: expr for name, expr in izip(names, prev_translator.expr_monads)}
                ]
                prev_translator.limit, prev_translator.offset = combine_limit_and_offset(
                    prev_translator.limit, prev_translator.offset, prev_limit, prev_offset)
                raise UseAnotherTranslator(prev_translator)


        if len(names) == 1 and isinstance(prev_translator.expr_type, EntityMeta) \
                and not prev_translator.aggregated and not prev_translator.distinct:
            name = names[0]
            entity = prev_translator.expr_type
            [expr_monad] = prev_translator.expr_monads
            entity_alias = expr_monad.tableref.alias
            subquery_ast = prev_translator.construct_subquery_ast(prev_limit, prev_offset, star=entity_alias)
            tableref = StarTableRef(sqlquery, name, entity, subquery_ast)
            tablerefs[name] = tableref
            tableref.make_join()
            translator.namespace[name] = ObjectIterMonad(tableref, entity)
        else:
            aliases = []
            aliases_dict = {}
            for name, base_expr_monad in izip(names, prev_translator.expr_monads):
                t = base_expr_monad.type
                if isinstance(t, EntityMeta):
                    t_aliases = []
                    for suffix in t._pk_paths_:
                        alias = '%s-%s' % (name, suffix)
                        t_aliases.append(alias)
                    aliases.extend(t_aliases)
                    aliases_dict[base_expr_monad] = t_aliases
                else:
                    aliases.append(name)
                    aliases_dict[base_expr_monad] = name

            subquery_ast = prev_translator.construct_subquery_ast(prev_limit, prev_offset, aliases=aliases)
            tableref = ExprTableRef(sqlquery, 't', subquery_ast, names, aliases)
            for name in names:
                tablerefs[name] = tableref
            tableref.make_join()

            for name, base_expr_monad in izip(names, prev_translator.expr_monads):
                t = base_expr_monad.type
                if isinstance(t, EntityMeta):
                    columns = aliases_dict[base_expr_monad]
                    expr_tableref = ExprJoinedTableRef(sqlquery, tableref, columns, name, t)
                    expr_monad = ObjectIterMonad(expr_tableref, t)
                else:
                    column = aliases_dict[base_expr_monad]
                    expr_ast = ['COLUMN', tableref.alias, column]
                    expr_monad = ExprMonad.new(t, expr_ast, base_expr_monad.nullable)
                assert name not in translator.namespace
                translator.namespace[name] = expr_monad
    def construct_subquery_ast(translator, limit=None, offset=None, aliases=None, star=None,
                               distinct=None, is_not_null_checks=False):
        subquery_ast, attr_offsets = translator.construct_sql_ast(
            limit, offset, distinct, is_not_null_checks=is_not_null_checks)
        assert len(subquery_ast) >= 3 and subquery_ast[0] == 'SELECT'

        select_ast = subquery_ast[1][:]
        assert select_ast[0] in ('ALL', 'DISTINCT', 'AGGREGATES'), select_ast
        if aliases:
            assert not star and len(aliases) == len(select_ast) - 1
            for i, alias in enumerate(aliases):
                expr = select_ast[i+1]
                if expr[0] == 'AS': expr = expr[1]
                select_ast[i+1] = [ 'AS', expr, alias ]
        elif star is not None:
            assert isinstance(star, basestring)
            for section in subquery_ast:
                assert section[0] not in ('GROUP_BY', 'HAVING'), subquery_ast
            select_ast[1:] = [ [ 'STAR', star ] ]

        from_ast = subquery_ast[2][:]
        assert from_ast[0] in ('FROM', 'LEFT_JOIN')

        if len(subquery_ast) == 3:
            where_ast = [ 'WHERE' ]
            other_ast = []
        elif subquery_ast[3][0] != 'WHERE':
            where_ast = [ 'WHERE' ]
            other_ast = subquery_ast[3:]
        else:
            where_ast = subquery_ast[3][:]
            other_ast = subquery_ast[4:]

        if len(from_ast[1]) == 4:
            outer_conditions = from_ast[1][-1]
            from_ast[1] = from_ast[1][:-1]
            if outer_conditions[0] == 'AND': where_ast[1:1] = outer_conditions[1:]
            else: where_ast.insert(1, outer_conditions)

        return [ 'SELECT', select_ast, from_ast, where_ast ] + other_ast
    def construct_sql_ast(translator, limit=None, offset=None, distinct=None,
                          aggr_func_name=None, aggr_func_distinct=None, sep=None,
                          for_update=False, nowait=False, skip_locked=False, is_not_null_checks=False):
        attr_offsets = None
        if distinct is None:
            if not translator.order:
                distinct = translator.distinct
        ast_transformer = lambda ast: ast
        if for_update:
            sql_ast = [ 'SELECT_FOR_UPDATE', nowait, skip_locked ]
            translator.query_result_is_cacheable = False
        else: sql_ast = [ 'SELECT' ]

        select_ast = [ 'DISTINCT' if distinct else 'ALL' ] + translator.expr_columns
        if aggr_func_name:
            expr_type = translator.expr_type
            if isinstance(expr_type, EntityMeta):
                if aggr_func_name == 'GROUP_CONCAT':
                    if expr_type._pk_is_composite_:
                        throw(TypeError, "`group_concat` cannot be used with entity with composite primary key")
                elif aggr_func_name != 'COUNT': throw(TypeError,
                    'Attribute should be specified for %r aggregate function' % aggr_func_name.lower())
            elif isinstance(expr_type, tuple):
                if aggr_func_name != 'COUNT': throw(TypeError,
                    'Single attribute should be specified for %r aggregate function' % aggr_func_name.lower())
            else:
                if aggr_func_name in ('SUM', 'AVG') and expr_type not in numeric_types:
                    throw(TypeError, '%r is valid for numeric attributes only' % aggr_func_name.lower())
                assert len(translator.expr_columns) == 1
            aggr_ast = None
            if translator.groupby_monads or (
                    aggr_func_name == 'COUNT' and distinct
                    and isinstance(translator.expr_type, EntityMeta)
                    and len(translator.expr_columns) > 1):
                outer_alias = 't'
                if aggr_func_name == 'COUNT' and not aggr_func_distinct:
                    outer_aggr_ast = [ 'COUNT', None ]
                else:
                    assert len(translator.expr_columns) == 1
                    expr_ast = translator.expr_columns[0]
                    if expr_ast[0] == 'COLUMN':
                        outer_alias, column_name = expr_ast[1:]
                        outer_aggr_ast = [aggr_func_name, aggr_func_distinct, ['COLUMN', outer_alias, column_name]]
                        if aggr_func_name == 'GROUP_CONCAT' and sep is not None:
                            outer_aggr_ast.append(['VALUE', sep])
                    else:
                        select_ast = [ 'DISTINCT' if distinct else 'ALL' ] + [ [ 'AS', expr_ast, 'expr' ] ]
                        outer_aggr_ast = [ aggr_func_name, aggr_func_distinct, [ 'COLUMN', 't', 'expr' ] ]
                        if aggr_func_name == 'GROUP_CONCAT' and sep is not None:
                            outer_aggr_ast.append(['VALUE', sep])
                def ast_transformer(ast):
                    return [ 'SELECT', [ 'AGGREGATES', outer_aggr_ast ],
                                       [ 'FROM', [ outer_alias, 'SELECT', ast[1:] ] ] ]
            else:
                if aggr_func_name == 'COUNT':
                    if isinstance(expr_type, (tuple, EntityMeta)) and not distinct and not aggr_func_distinct:
                        aggr_ast = [ 'COUNT', aggr_func_distinct ]
                    else:
                        aggr_ast = [ 'COUNT', True if aggr_func_distinct is None else aggr_func_distinct,
                                     translator.expr_columns[0] ]
                else:
                    aggr_ast = [ aggr_func_name, aggr_func_distinct, translator.expr_columns[0] ]
                    if aggr_func_name == 'GROUP_CONCAT' and sep is not None:
                        aggr_ast.append(['VALUE', sep])

            if aggr_ast: select_ast = [ 'AGGREGATES', aggr_ast ]
        elif isinstance(translator.expr_type, EntityMeta) and not translator.parent \
             and not translator.aggregated and not translator.optimize:
            select_ast, attr_offsets = translator.expr_type._construct_select_clause_(
                translator.alias, distinct, translator.tableref.used_attrs)
        sql_ast.append(select_ast)
        sql_ast.append(translator.sqlquery.from_ast)

        conditions = translator.conditions[:]
        having_conditions = translator.having_conditions[:]
        if is_not_null_checks:
            for monad in translator.expr_monads:
                if isinstance(monad, ObjectIterMonad): pass
                elif not monad.nullable: pass
                else:
                    notnull_conditions = [ [ 'IS_NOT_NULL', column_ast ] for column_ast in monad.getsql() ]
                    if monad.aggregated: having_conditions.extend(notnull_conditions)
                    else: conditions.extend(notnull_conditions)
        if conditions:
            sql_ast.append([ 'WHERE' ] + conditions)

        if translator.groupby_monads:
            group_by = [ 'GROUP_BY' ]
            for m in translator.groupby_monads: group_by.extend(m.getsql())
            sql_ast.append(group_by)
        else: group_by = None

        if having_conditions:
            if not group_by: throw(TranslationError,
                'In order to use aggregated functions such as SUM(), COUNT(), etc., '
                'query must have grouping columns (i.e. resulting non-aggregated values)')
            sql_ast.append([ 'HAVING' ] + having_conditions)

        if translator.order and not aggr_func_name: sql_ast.append([ 'ORDER_BY' ] + translator.order)

        limit, offset = combine_limit_and_offset(translator.limit, translator.offset, limit, offset)
        if limit is not None or offset is not None:
            assert not aggr_func_name
            provider = translator.database.provider
            if limit is None:
                if provider.dialect == 'SQLite':
                    limit = -1
                elif provider.dialect == 'MySQL':
                    limit = 18446744073709551615
            limit_section = [ 'LIMIT', limit ]
            if offset: limit_section.append(offset)
            sql_ast.append(limit_section)

        sql_ast = ast_transformer(sql_ast)
        return sql_ast, attr_offsets
    def construct_delete_sql_ast(translator):
        entity = translator.expr_type
        expr_monad = translator.tree.expr.monad
        if not isinstance(entity, EntityMeta): throw(TranslationError,
            'Delete query should be applied to a single entity. Got: %s' % ast2src(translator.tree.expr))
        force_in = False
        if translator.groupby_monads:
            force_in = True
        else:
            assert not translator.having_conditions
        tableref = expr_monad.tableref
        from_ast = translator.sqlquery.from_ast
        if from_ast[0] != 'FROM':
            force_in = True

        if not force_in and len(from_ast) == 2 and not translator.sqlquery.used_from_subquery:
            sql_ast = [ 'DELETE', None, from_ast ]
            if translator.conditions:
                sql_ast.append([ 'WHERE' ] + translator.conditions)
        elif not force_in and translator.dialect == 'MySQL':
            sql_ast = [ 'DELETE', tableref.alias, from_ast ]
            if translator.conditions:
                sql_ast.append([ 'WHERE' ] + translator.conditions)
        else:
            delete_from_ast = [ 'FROM', [ None, 'TABLE', entity._table_ ] ]
            if len(entity._pk_columns_) == 1:
                inner_expr = expr_monad.getsql()
                outer_expr = [ 'COLUMN', None, entity._pk_columns_[0] ]
            elif translator.rowid_support:
                inner_expr = [ [ 'COLUMN', tableref.alias, 'ROWID' ] ]
                outer_expr = [ 'COLUMN', None, 'ROWID' ]
            elif translator.row_value_syntax:
                inner_expr = expr_monad.getsql()
                outer_expr = [ 'ROW' ] + [ [ 'COLUMN', None, column_name ] for column_name in entity._pk_columns_ ]
            else: throw(NotImplementedError)
            subquery_ast = [ 'SELECT', [ 'ALL' ] + inner_expr, from_ast ]
            if translator.conditions:
                subquery_ast.append([ 'WHERE' ] + translator.conditions)
            delete_where_ast = [ 'WHERE', [ 'IN', outer_expr, subquery_ast ] ]
            sql_ast = [ 'DELETE', None, delete_from_ast, delete_where_ast ]
        return sql_ast
    def get_used_attrs(translator):
        if isinstance(translator.expr_type, EntityMeta) and not translator.aggregated and not translator.optimize:
            return translator.tableref.used_attrs
        return ()
    def without_order(translator):
        translator = deepcopy(translator)
        translator.order = []
        return translator
    def order_by_numbers(translator, numbers):
        if 0 in numbers: throw(ValueError, 'Numeric arguments of order_by() method must be non-zero')
        translator = deepcopy(translator)
        order = translator.order = translator.order[:]  # only order will be changed
        expr_monads = translator.expr_monads
        new_order = []
        for i in numbers:
            try: monad = expr_monads[abs(i)-1]
            except IndexError:
                if len(expr_monads) > 1: throw(IndexError,
                    "Invalid index of order_by() method: %d "
                    "(query result is list of tuples with only %d elements in each)" % (i, len(expr_monads)))
                else: throw(IndexError,
                    "Invalid index of order_by() method: %d "
                    "(query result is single list of elements and has only one 'column')" % i)
            for pos in monad.orderby_columns:
                new_order.append(i < 0 and [ 'DESC', [ 'VALUE', pos ] ] or [ 'VALUE', pos ])
        order[:0] = new_order
        return translator
    def order_by_attributes(translator, attrs):
        entity = translator.expr_type
        if not isinstance(entity, EntityMeta): throw(NotImplementedError,
            'Ordering by attributes is limited to queries which return simple list of objects. '
            'Try use other forms of ordering (by tuple element numbers or by full-blown lambda expr).')
        translator = deepcopy(translator)
        order = translator.order = translator.order[:]  # only order will be changed
        alias = translator.alias
        new_order = []
        for x in attrs:
            if isinstance(x, DescWrapper):
                attr = x.attr
                desc_wrapper = lambda column: [ 'DESC', column ]
            elif isinstance(x, Attribute):
                attr = x
                desc_wrapper = lambda column: column
            else: assert False, x  # pragma: no cover
            if entity._adict_.get(attr.name) is not attr: throw(TypeError,
                'Attribute %s does not belong to entity %s' % (attr, entity.__name__))
            if attr.is_collection: throw(TypeError,
                'Collection attribute %s cannot be used for ordering' % attr)
            for column in attr.columns:
                new_order.append(desc_wrapper([ 'COLUMN', alias, column]))
        order[:0] = new_order
        return translator
    def apply_kwfilters(translator, filterattrs, original_names=False):
        translator = deepcopy(translator)
        with translator:
            if original_names:
                object_monad = translator.tree.quals[0].iter.monad
                assert isinstance(object_monad.type, EntityMeta)
            else:
                object_monad = translator.tree.expr.monad
                if not isinstance(object_monad.type, EntityMeta):
                    throw(TypeError, 'Keyword arguments are not allowed when query result is not entity objects')

            monads = []
            none_monad = NoneMonad()
            for attr, id, is_none in filterattrs:
                attr_monad = object_monad.getattr(attr.name)
                if is_none: monads.append(CmpMonad('is', attr_monad, none_monad))
                else:
                    param_monad = ParamMonad.new(attr.py_type, (id, None, None))
                    monads.append(CmpMonad('==', attr_monad, param_monad))
            for m in monads: translator.conditions.extend(m.getsql())
            return translator
    def apply_lambda(translator, func_id, filter_num, order_by, func_ast, argnames, original_names, extractors, vars, vartypes):
        translator = deepcopy(translator)
        func_ast = copy_ast(func_ast)  # func_ast = deepcopy(func_ast)
        translator.code_key = func_id
        translator.filter_num = filter_num
        translator.extractors.update(extractors)
        translator.vars = vars
        translator.vartypes = translator.vartypes.copy()  # make HashableDict mutable again
        translator.vartypes.update(vartypes)

        if not original_names:
            assert argnames
            namespace = {name: monad for name, monad in izip(argnames, translator.expr_monads)}
        elif argnames:
            namespace = {name: translator.namespace[name] for name in argnames}
        else:
            namespace = None
        if namespace is not None:
            translator.namespace_stack.append(namespace)
        try:
            with translator:
                translator.dispatch(func_ast)
                if isinstance(func_ast, ast.Tuple): nodes = func_ast.nodes
                else: nodes = (func_ast,)
                if order_by:
                    translator.inside_order_by = True
                    new_order = []
                    for node in nodes:
                        monad = node.monad.to_single_cell_value()
                        if isinstance(monad, SetMixin):
                            t = monad.type.item_type
                            if isinstance(type(t), type): t = t.__name__
                            throw(TranslationError, 'Set of %s (%s) cannot be used for ordering'
                                                    % (t, ast2src(node)))
                        new_order.extend(node.monad.getsql())
                    translator.order[:0] = new_order
                    translator.inside_order_by = False
                else:
                    for node in nodes:
                        monad = node.monad
                        if isinstance(monad, AndMonad): cond_monads = monad.operands
                        else: cond_monads = [ monad ]
                        for m in cond_monads:
                            if not m.aggregated: translator.conditions.extend(m.getsql())
                            else: translator.having_conditions.extend(m.getsql())
                translator.vars = None
                return translator
        finally:
            if namespace is not None:
                ns = translator.namespace_stack.pop()
                assert ns is namespace
    def preGenExpr(translator, node):
        inner_tree = node.code
        translator_cls = translator.__class__
        try:
            subtranslator = translator_cls(inner_tree, translator)
        except UseAnotherTranslator:
            assert False
        return QuerySetMonad(subtranslator)
    def postGenExprIf(translator, node):
        monad = node.test.monad
        if monad.type is not bool: monad = monad.nonzero()
        return monad
    def preCompare(translator, node):
        monads = []
        ops = node.ops
        left = node.expr
        translator.dispatch(left)
        # op: '<' | '>' | '=' | '>=' | '<=' | '<>' | '!=' | '=='
        #         | 'in' | 'not in' | 'is' | 'is not'
        for op, right in node.ops:
            translator.dispatch(right)
            if op.endswith('in'): monad = right.monad.contains(left.monad, op == 'not in')
            else: monad = left.monad.cmp(op, right.monad)
            if not hasattr(monad, 'aggregated'):
                monad.aggregated = getattr(left.monad, 'aggregated', False) or getattr(right.monad, 'aggregated', False)
            if not hasattr(monad, 'nogroup'):
                monad.nogroup = getattr(left.monad, 'nogroup', False) or getattr(right.monad, 'nogroup', False)
            if monad.aggregated and monad.nogroup: throw(TranslationError,
                'Too complex aggregation, expressions cannot be combined: {EXPR}')
            monads.append(monad)
            left = right
        if len(monads) == 1: return monads[0]
        return AndMonad(monads)
    def postConst(translator, node):
        value = node.value
        if type(value) is frozenset:
            value = tuple(sorted(value))
        return ConstMonad.new(value)
    def postEllipsis(translator, node):
        return ConstMonad.new(Ellipsis)
    def postList(translator, node):
        return ListMonad([ item.monad for item in node.nodes ])
    def postTuple(translator, node):
        return ListMonad([ item.monad for item in node.nodes ])
    def postName(translator, node):
        monad = translator.resolve_name(node.name)
        assert monad is not None
        return monad
    def resolve_name(translator, name):
        if name not in translator.namespace:
            throw(TranslationError, 'Name %s is not found in %s' % (name, translator.namespace))
        monad = translator.namespace[name]
        assert isinstance(monad, Monad)
        if monad.translator is not translator:
            monad.translator.sqlquery.used_from_subquery = True
        return monad
    def postAdd(translator, node):
        return node.left.monad + node.right.monad
    def postSub(translator, node):
        return node.left.monad - node.right.monad
    def postMul(translator, node):
        return node.left.monad * node.right.monad
    def postDiv(translator, node):
        return node.left.monad / node.right.monad
    def postFloorDiv(translator, node):
        return node.left.monad // node.right.monad
    def postMod(translator, node):
        return node.left.monad % node.right.monad
    def postPower(translator, node):
        return node.left.monad ** node.right.monad
    def postUnarySub(translator, node):
        return -node.expr.monad
    def postGetattr(translator, node):
        return node.expr.monad.getattr(node.attrname)
    def postAnd(translator, node):
        return AndMonad([ subnode.monad for subnode in node.nodes ])
    def postOr(translator, node):
        return OrMonad([ subnode.monad for subnode in node.nodes ])
    def postBitor(translator, node):
        left, right = (subnode.monad for subnode in node.nodes)
        return left | right
    def postBitand(translator, node):
        left, right = (subnode.monad for subnode in node.nodes)
        return left & right
    def postBitxor(translator, node):
        left, right = (subnode.monad for subnode in node.nodes)
        return left ^ right
    def postNot(translator, node):
        return node.expr.monad.negate()
    def preCallFunc(translator, node):
        if node.star_args is not None: throw(NotImplementedError, '*%s is not supported' % ast2src(node.star_args))
        if node.dstar_args is not None: throw(NotImplementedError, '**%s is not supported' % ast2src(node.dstar_args))
        func_node = node.node
        if isinstance(func_node, ast.CallFunc):
            if isinstance(func_node.node, ast.Name) and func_node.node.name == 'getattr': return
        if not isinstance(func_node, (ast.Name, ast.Getattr)): throw(NotImplementedError)
        if len(node.args) > 1: return
        if not node.args: return
        arg = node.args[0]
        if isinstance(arg, ast.GenExpr):
            translator.dispatch(func_node)
            func_monad = func_node.monad
            translator.dispatch(arg)
            query_set_monad = arg.monad
            return func_monad(query_set_monad)
        if not isinstance(arg, ast.Lambda): return
        lambda_expr = arg
        translator.dispatch(func_node)
        method_monad = func_node.monad
        if not isinstance(method_monad, MethodMonad): throw(NotImplementedError)
        entity_monad = method_monad.parent
        if not isinstance(entity_monad, (EntityMonad, AttrSetMonad)): throw(NotImplementedError)
        entity = entity_monad.type.item_type
        method_name = method_monad.attrname
        if method_name not in ('select', 'filter', 'exists'): throw(TypeError)
        if len(lambda_expr.argnames) != 1: throw(TypeError)
        if lambda_expr.varargs: throw(TypeError)
        if lambda_expr.kwargs: throw(TypeError)
        if lambda_expr.defaults: throw(TypeError)
        iter_name = lambda_expr.argnames[0]
        cond_expr = lambda_expr.code
        if_expr = ast.GenExprIf(cond_expr)
        name_ast = ast.Name(entity.__name__)
        name_ast.monad = entity_monad
        for_expr = ast.GenExprFor(ast.AssName(iter_name, 'OP_ASSIGN'), name_ast, [ if_expr ])
        inner_expr = ast.GenExprInner(ast.Name(iter_name), [ for_expr ])
        translator_cls = translator.__class__
        try:
            subtranslator = translator_cls(inner_expr, translator)
        except UseAnotherTranslator:
            assert False
        monad = QuerySetMonad(subtranslator)
        if method_name == 'exists':
            monad = monad.nonzero()
        return monad
    def postCallFunc(translator, node):
        args = []
        kwargs = {}
        for arg in node.args:
            if isinstance(arg, ast.Keyword):
                kwargs[arg.name] = arg.expr.monad
            else: args.append(arg.monad)
        func_monad = node.node.monad
        return func_monad(*args, **kwargs)
    def postKeyword(translator, node):
        pass  # this node will be processed by postCallFunc
    def postSubscript(translator, node):
        assert node.flags == 'OP_APPLY'
        assert isinstance(node.subs, list)
        if len(node.subs) > 1:
            for x in node.subs:
                if isinstance(x, ast.Sliceobj): throw(TypeError)
            key = ListMonad([ item.monad for item in node.subs ])
            return node.expr.monad[key]
        sub = node.subs[0]
        if isinstance(sub, ast.Sliceobj):
            start, stop, step = (sub.nodes+[None])[:3]
            if start is not None: start = start.monad
            if isinstance(start, NoneMonad): start = None
            if stop is not None: stop = stop.monad
            if isinstance(stop, NoneMonad): stop = None
            if step is not None: step = step.monad
            if isinstance(step, NoneMonad): step = None
            return node.expr.monad[start:stop:step]
        else: return node.expr.monad[sub.monad]
    def postSlice(translator, node):
        assert node.flags == 'OP_APPLY'
        expr_monad = node.expr.monad
        upper = node.upper
        if upper is not None: upper = upper.monad
        if isinstance(upper, NoneMonad): upper = None
        lower = node.lower
        if lower is not None: lower = lower.monad
        if isinstance(lower, NoneMonad): lower = None
        return expr_monad[lower:upper]
    def postSliceobj(translator, node):
        pass
    def postIfExp(translator, node):
        test_monad, then_monad, else_monad = node.test.monad, node.then.monad, node.else_.monad
        if test_monad.type is not bool: test_monad = test_monad.nonzero()
        result_type = coerce_types(then_monad.type, else_monad.type)
        test_sql, then_sql, else_sql = test_monad.getsql()[0], then_monad.getsql(), else_monad.getsql()
        if len(then_sql) == 1: then_sql, else_sql = then_sql[0], else_sql[0]
        elif not translator.row_value_syntax: throw(NotImplementedError)
        else: then_sql, else_sql = [ 'ROW' ] + then_sql, [ 'ROW' ] + else_sql
        expr = [ 'CASE', None, [ [ test_sql, then_sql ] ], else_sql ]
        result = ExprMonad.new(result_type, expr,
                               nullable=test_monad.nullable or then_monad.nullable or else_monad.nullable)
        result.aggregated = test_monad.aggregated or then_monad.aggregated or else_monad.aggregated
        return result
    def postStr(translator, node):
        val_monad = node.value.monad
        if isinstance(val_monad, StringMixin):
            return val_monad
        sql = ['TO_STR', val_monad.getsql()[0] ]
        return StringExprMonad(unicode, sql, nullable=val_monad.nullable)
    def postJoinedStr(translator, node):
        nullable = False
        for subnode in node.values:
            assert isinstance(subnode.monad, StringMixin), (subnode.monad, subnode)
            if subnode.monad.nullable:
                nullable = True
        sql = [ 'CONCAT' ] + [ value.monad.getsql()[0] for value in node.values ]
        return StringExprMonad(unicode, sql, nullable=nullable)
    def postFormattedValue(translator, node):
        throw(NotImplementedError, 'You cannot set width and precision markers in query')

def combine_limit_and_offset(limit, offset, limit2, offset2):
    assert limit is None or limit >= 0
    assert limit2 is None or limit2 >= 0

    if offset2 is not None:
        if limit is not None:
            limit = max(0, limit - offset2)
        offset = (offset or 0) + offset2

    if limit2 is not None:
        if limit is not None:
            limit = min(limit, limit2)
        else:
            limit = limit2

    if limit == 0:
        offset = None

    return limit, offset

def coerce_monads(m1, m2, for_comparison=False):
    result_type = coerce_types(m1.type, m2.type)
    if result_type in numeric_types and bool in (m1.type, m2.type) and (
                result_type is not bool or not for_comparison):
        translator = m1.translator
        if translator.dialect == 'PostgreSQL':
            if result_type is bool:
                result_type = int
            if m1.type is bool:
                new_m1 = NumericExprMonad(int, [ 'TO_INT', m1.getsql()[0] ], nullable=m1.nullable)
                new_m1.aggregated = m1.aggregated
                m1 = new_m1
            if m2.type is bool:
                new_m2 = NumericExprMonad(int, [ 'TO_INT', m2.getsql()[0] ], nullable=m2.nullable)
                new_m2.aggregated = m2.aggregated
                m2 = new_m2
    return result_type, m1, m2

max_alias_length = 30

class SqlQuery(object):
    def __init__(sqlquery, translator, parent_sqlquery=None, left_join=False):
        sqlquery.translator = translator
        sqlquery.parent_sqlquery = parent_sqlquery
        sqlquery.left_join = left_join
        sqlquery.from_ast = [ 'LEFT_JOIN' if left_join else 'FROM' ]
        sqlquery.conditions = []
        sqlquery.outer_conditions = []
        sqlquery.tablerefs = {}
        if parent_sqlquery is None:
            sqlquery.alias_counters = {}
            sqlquery.expr_counter = itertools.count(1)
        else:
            sqlquery.alias_counters = parent_sqlquery.alias_counters.copy()
            sqlquery.expr_counter = parent_sqlquery.expr_counter
        sqlquery.used_from_subquery = False
    def get_tableref(sqlquery, name_path):
        tableref = sqlquery.tablerefs.get(name_path)
        parent_sqlquery = sqlquery.parent_sqlquery
        if tableref is None and parent_sqlquery:
            tableref = parent_sqlquery.get_tableref(name_path)
            if tableref is not None:
                parent_sqlquery.used_from_subquery = True
        return tableref
    def add_tableref(sqlquery, name_path, parent_tableref, attr):
        assert name_path not in sqlquery.tablerefs
        if parent_tableref.sqlquery is not sqlquery:
            parent_tableref.sqlquery.used_from_subquery = True
        tableref = JoinedTableRef(sqlquery, name_path, parent_tableref, attr)
        sqlquery.tablerefs[name_path] = tableref
        return tableref
    def make_alias(sqlquery, name):
        name = name[:max_alias_length-3].lower()
        i = sqlquery.alias_counters.setdefault(name, 0) + 1
        alias = name if i == 1 and name != 't' else '%s-%d' % (name, i)
        sqlquery.alias_counters[name] = i
        return alias
    def join_table(sqlquery, parent_alias, alias, table_name, join_cond):
        new_item = [alias, 'TABLE', table_name, join_cond]
        from_ast = sqlquery.from_ast
        for i in xrange(1, len(from_ast)):
            if from_ast[i][0] == parent_alias:
                for j in xrange(i+1, len(from_ast)):
                    if len(from_ast[j]) < 4:  # item without join condition
                        from_ast.insert(j, new_item)
                        return
        from_ast.append(new_item)

class TableRef(object):
    def __init__(tableref, sqlquery, name, entity):
        tableref.sqlquery = sqlquery
        tableref.alias = sqlquery.make_alias(name)
        tableref.name_path = tableref.alias
        tableref.entity = entity
        tableref.joined = False
        tableref.can_affect_distinct = True
        tableref.used_attrs = set()
    def make_join(tableref, pk_only=False):
        entity = tableref.entity
        if not tableref.joined:
            sqlquery = tableref.sqlquery
            sqlquery.from_ast.append([ tableref.alias, 'TABLE', entity._table_ ])
            if entity._discriminator_attr_:
                discr_criteria = entity._construct_discriminator_criteria_(tableref.alias)
                assert discr_criteria is not None
                sqlquery.conditions.append(discr_criteria)
            tableref.joined = True
        return tableref.alias, entity._pk_columns_

class ExprTableRef(TableRef):
    def __init__(tableref, sqlquery, name, subquery_ast, expr_names, expr_aliases):
        TableRef.__init__(tableref, sqlquery, name, None)
        tableref.subquery_ast = subquery_ast
        tableref.expr_names = expr_names
        tableref.expr_aliases = expr_aliases
    def make_join(tableref, pk_only=False):
        assert tableref.subquery_ast[0] == 'SELECT'
        if not tableref.joined:
            sqlquery = tableref.sqlquery
            sqlquery.from_ast.append([tableref.alias, 'SELECT', tableref.subquery_ast[1:]])
            tableref.joined = True
        return tableref.alias, None

class StarTableRef(TableRef):
    def __init__(tableref, sqlquery, name, entity, subquery_ast):
        TableRef.__init__(tableref, sqlquery, name, entity)
        tableref.subquery_ast = subquery_ast
    def make_join(tableref, pk_only=False):
        entity = tableref.entity
        assert tableref.subquery_ast[0] == 'SELECT'
        if not tableref.joined:
            sqlquery = tableref.sqlquery
            sqlquery.from_ast.append([ tableref.alias, 'SELECT', tableref.subquery_ast[1:] ])
            if entity._discriminator_attr_:  # ???
                discr_criteria = entity._construct_discriminator_criteria_(tableref.alias)
                assert discr_criteria is not None
                sqlquery.conditions.append(discr_criteria)
            tableref.joined = True
        return tableref.alias, entity._pk_columns_

class ExprJoinedTableRef(object):
    def __init__(tableref, sqlquery, parent_tableref, parent_columns, name, entity):
        tableref.sqlquery = sqlquery
        tableref.parent_tableref = parent_tableref
        tableref.parent_columns = parent_columns
        tableref.name = tableref.name_path = name
        tableref.entity = entity
        tableref.alias = None
        tableref.joined = False
        tableref.can_affect_distinct = False
        tableref.used_attrs = set()
    def make_join(tableref, pk_only=False):
        entity = tableref.entity
        if tableref.joined:
            return tableref.alias, tableref.pk_columns
        sqlquery = tableref.sqlquery
        parent_alias, left_pk_columns = tableref.parent_tableref.make_join()
        if pk_only:
            tableref.alias = parent_alias
            tableref.pk_columns = tableref.parent_columns
            return tableref.alias, tableref.pk_columns
        tableref.alias = sqlquery.make_alias(tableref.name)
        tableref.pk_columns = entity._pk_columns_
        join_cond = join_tables(parent_alias, tableref.alias, tableref.parent_columns, tableref.pk_columns)
        sqlquery.join_table(parent_alias, tableref.alias, entity._table_, join_cond)
        tableref.joined = True
        return tableref.alias, tableref.pk_columns

class JoinedTableRef(object):
    def __init__(tableref, sqlquery, name_path, parent_tableref, attr):
        tableref.sqlquery = sqlquery
        tableref.name_path = name_path
        tableref.var_name = name_path if is_ident(name_path) else None
        tableref.alias = None
        tableref.optimized = None
        tableref.parent_tableref = parent_tableref
        tableref.attr = attr
        tableref.entity = attr.py_type
        assert isinstance(tableref.entity, EntityMeta)
        tableref.joined = False
        tableref.can_affect_distinct = False
        tableref.used_attrs = set()
    def make_join(tableref, pk_only=False):
        entity = tableref.entity
        if tableref.joined:
            if pk_only or not tableref.optimized:
                return tableref.alias, tableref.pk_columns
        sqlquery = tableref.sqlquery
        attr = tableref.attr
        parent_pk_only = attr.pk_offset is not None or attr.is_collection
        parent_alias, left_pk_columns = tableref.parent_tableref.make_join(parent_pk_only)
        left_entity = attr.entity
        pk_columns = entity._pk_columns_
        if not attr.is_collection:
            if not attr.columns:
                # one-to-one relationship with foreign key column on the right side
                reverse = attr.reverse
                assert reverse.columns and not reverse.is_collection
                rentity = reverse.entity
                pk_columns = rentity._pk_columns_
                alias = sqlquery.make_alias(tableref.var_name or rentity.__name__)
                join_cond = join_tables(parent_alias, alias, left_pk_columns, reverse.columns)
            else:
                # one-to-one or many-to-one relationship with foreign key column on the left side
                if attr.pk_offset is not None:
                    offset = attr.pk_columns_offset
                    left_columns = left_pk_columns[offset:offset+len(attr.columns)]
                else: left_columns = attr.columns
                if pk_only:
                    tableref.alias = parent_alias
                    tableref.pk_columns = left_columns
                    tableref.optimized = True
                    # tableref.joined = True
                    return parent_alias, left_columns
                alias = sqlquery.make_alias(tableref.var_name or entity.__name__)
                join_cond = join_tables(parent_alias, alias, left_columns, pk_columns)
        elif not attr.reverse.is_collection:
            # many-to-one relationship
            alias = sqlquery.make_alias(tableref.var_name or entity.__name__)
            join_cond = join_tables(parent_alias, alias, left_pk_columns, attr.reverse.columns)
        else:
            # many-to-many relationship
            right_m2m_columns = attr.reverse_columns if attr.symmetric else attr.columns
            if not tableref.joined:
                m2m_table = attr.table
                m2m_alias = sqlquery.make_alias('t')
                reverse_columns = attr.columns if attr.symmetric else attr.reverse.columns
                m2m_join_cond = join_tables(parent_alias, m2m_alias, left_pk_columns, reverse_columns)
                sqlquery.join_table(parent_alias, m2m_alias, m2m_table, m2m_join_cond)
                if pk_only:
                    tableref.alias = m2m_alias
                    tableref.pk_columns = right_m2m_columns
                    tableref.optimized = True
                    tableref.joined = True
                    return m2m_alias, tableref.pk_columns
            elif tableref.optimized:
                assert not pk_only
                m2m_alias = tableref.alias
            alias = sqlquery.make_alias(tableref.var_name or entity.__name__)
            join_cond = join_tables(m2m_alias, alias, right_m2m_columns, pk_columns)
        if not pk_only and entity._discriminator_attr_:
            discr_criteria = entity._construct_discriminator_criteria_(alias)
            assert discr_criteria is not None
            join_cond.append(discr_criteria)

        translator = tableref.sqlquery.translator.root_translator
        if translator.optimize == tableref.name_path and translator.from_optimized and tableref.sqlquery is translator.sqlquery:
            pass
        else:
            sqlquery.join_table(parent_alias, alias, entity._table_, join_cond)
        tableref.alias = alias
        tableref.pk_columns = pk_columns
        tableref.optimized = False
        tableref.joined = True
        return tableref.alias, pk_columns

def wrap_monad_method(cls_name, func):
    overrider_name = '%s_%s' % (cls_name, func.__name__)
    def wrapper(monad, *args, **kwargs):
        method = getattr(monad.translator, overrider_name, func)
        return method(monad, *args, **kwargs)
    return update_wrapper(wrapper, func)

class MonadMeta(type):
    def __new__(meta, cls_name, bases, cls_dict):
        for name, func in cls_dict.items():
            if not isinstance(func, types.FunctionType): continue
            if name in ('__new__', '__init__'): continue
            cls_dict[name] = wrap_monad_method(cls_name, func)
        return super(MonadMeta, meta).__new__(meta, cls_name, bases, cls_dict)

class MonadMixin(with_metaclass(MonadMeta)):
    pass

class Monad(with_metaclass(MonadMeta)):
    disable_distinct = False
    disable_ordering = False
    def __init__(monad, type, nullable=True):
        monad.node = None
        monad.translator = local.translator
        monad.type = type
        monad.nullable = nullable
        monad.mixin_init()
    def mixin_init(monad):
        pass
    def to_single_cell_value(monad):
        return monad
    def cmp(monad, op, monad2):
        return CmpMonad(op, monad, monad2)
    def contains(monad, item, not_in=False): throw(TypeError)
    def nonzero(monad):
        return CmpMonad('is not', monad, NoneMonad())
    def negate(monad):
        return NotMonad(monad)
    def getattr(monad, attrname):
        try: property_method = getattr(monad, 'attr_' + attrname)
        except AttributeError:
            if not hasattr(monad, 'call_' + attrname):
                throw(AttributeError, '%r object has no attribute %r: {EXPR}' % (type2str(monad.type), attrname))
            return MethodMonad(monad, attrname)
        return property_method()
    def len(monad): throw(TypeError)
    def count(monad, distinct=None):
        distinct = distinct_from_monad(distinct, default=True)
        translator = monad.translator
        if monad.aggregated: throw(TranslationError, 'Aggregated functions cannot be nested. Got: {EXPR}')
        expr = monad.getsql()

        if monad.type is bool:
            expr = [ 'CASE', None, [ [ expr[0], [ 'VALUE', 1 ] ] ], [ 'VALUE', None ] ]
            distinct = None
        elif len(expr) == 1: expr = expr[0]
        elif translator.dialect == 'PostgreSQL':
            row = [ 'ROW' ] + expr
            expr = [ 'CASE', None, [ [ [ 'IS_NULL', row ], [ 'VALUE', None ] ] ], row ]
        # elif translator.dialect == 'PostgreSQL':  # another way
        #     alias, pk_columns = monad.tableref.make_join(pk_only=False)
        #     expr = [ 'COLUMN', alias, 'ctid' ]
        elif translator.dialect in ('SQLite', 'Oracle'):
            alias, pk_columns = monad.tableref.make_join(pk_only=False)
            expr = [ 'COLUMN', alias, 'ROWID' ]
        # elif translator.row_value_syntax == True:  # doesn't work in MySQL
        #     expr = ['ROW'] + expr
        else: throw(NotImplementedError,
                    '%s database provider does not support entities '
                    'with composite primary keys inside aggregate functions. Got: {EXPR}'
                    % translator.dialect)
        result = ExprMonad.new(int, [ 'COUNT', distinct, expr ], nullable=False)
        result.aggregated = True
        return result
    def aggregate(monad, func_name, distinct=None, sep=None):
        distinct = distinct_from_monad(distinct)
        translator = monad.translator
        if monad.aggregated: throw(TranslationError, 'Aggregated functions cannot be nested. Got: {EXPR}')
        expr_type = monad.type
        # if isinstance(expr_type, SetType): expr_type = expr_type.item_type
        if func_name in ('SUM', 'AVG'):
            if expr_type not in numeric_types:
                if expr_type is Json: monad = monad.to_real()
                else: throw(TypeError, "Function '%s' expects argument of numeric type, got %r in {EXPR}"
                                       % (func_name, type2str(expr_type)))
        elif func_name in ('MIN', 'MAX'):
            if expr_type not in comparable_types:
                throw(TypeError, "Function '%s' cannot be applied to type %r in {EXPR}"
                                 % (func_name, type2str(expr_type)))
        elif func_name == 'GROUP_CONCAT':
            if isinstance(expr_type, EntityMeta) and expr_type._pk_is_composite_:
                throw(TypeError, "`group_concat` cannot be used with entity with composite primary key")
        else: assert False  # pragma: no cover
        expr = monad.getsql()
        if len(expr) == 1: expr = expr[0]
        elif translator.row_value_syntax: expr = ['ROW'] + expr
        else: throw(NotImplementedError,
                    '%s database provider does not support entities '
                    'with composite primary keys inside aggregate functions. Got: {EXPR} '
                    '(you can suggest us how to write SQL for this query)'
                    % translator.dialect)
        if func_name == 'AVG':
            result_type = float
        elif func_name == 'GROUP_CONCAT':
            result_type = unicode
        else:
            result_type = expr_type
        if distinct is None:
            distinct = getattr(monad, 'forced_distinct', False) and func_name in ('SUM', 'AVG')
        aggr_ast = [ func_name, distinct, expr ]
        if func_name == 'GROUP_CONCAT':
            if sep is not None:
                aggr_ast.append(['VALUE', sep])
        result = ExprMonad.new(result_type, aggr_ast, nullable=True)
        result.aggregated = True
        return result
    def __call__(monad, *args, **kwargs): throw(TypeError)
    def __getitem__(monad, key): throw(TypeError)
    def __add__(monad, monad2): throw(TypeError)
    def __sub__(monad, monad2): throw(TypeError)
    def __mul__(monad, monad2): throw(TypeError)
    def __truediv__(monad, monad2): throw(TypeError)
    def __floordiv__(monad, monad2): throw(TypeError)
    def __pow__(monad, monad2): throw(TypeError)
    def __neg__(monad): throw(TypeError)
    def __or__(monad): throw(TypeError)
    def __and__(monad): throw(TypeError)
    def __xor__(monad): throw(TypeError)
    def abs(monad): throw(TypeError)
    def cast_from_json(monad, type): assert False, monad
    def to_int(monad):
        return NumericExprMonad(int, [ 'TO_INT', monad.getsql()[0] ], nullable=monad.nullable)
    def to_str(monad):
        return StringExprMonad(unicode, [ 'TO_STR', monad.getsql()[0] ], nullable=monad.nullable)
    def to_real(monad):
        return NumericExprMonad(float, [ 'TO_REAL', monad.getsql()[0] ], nullable=monad.nullable)

def distinct_from_monad(distinct, default=None):
    if distinct is None:
        return default
    if isinstance(distinct, NumericConstMonad) and isinstance(distinct.value, bool):
        return distinct.value
    throw(TypeError, '`distinct` value should be True or False. Got: %s' % ast2src(distinct.node))

class RawSQLMonad(Monad):
    def __init__(monad, rawtype, varkey, nullable=True):
        if rawtype.result_type is None: type = rawtype
        else: type = normalize_type(rawtype.result_type)
        Monad.__init__(monad, type, nullable=nullable)
        monad.rawtype = rawtype
        monad.varkey = varkey
    def contains(monad, item, not_in=False):
        translator = monad.translator
        expr = item.getsql()
        if len(expr) == 1: expr = expr[0]
        elif translator.row_value_syntax == True: expr = ['ROW'] + expr
        else: throw(TranslationError,
                    '%s database provider does not support tuples. Got: {EXPR} ' % translator.dialect)
        op = 'NOT_IN' if not_in else 'IN'
        sql = [ op, expr, monad.getsql() ]
        return BoolExprMonad(sql, nullable=item.nullable)
    def nonzero(monad): return monad
    def getsql(monad, sqlquery=None):
        provider = monad.translator.database.provider
        rawtype = monad.rawtype
        result = []
        types = enumerate(rawtype.types)
        for item in monad.rawtype.items:
            if isinstance(item, basestring):
                result.append(item)
            else:
                expr, code = item
                i, param_type = next(types)
                param_converter = provider.get_converter_by_py_type(param_type)
                result.append(['PARAM', (monad.varkey, i, None), param_converter])
        return [ [ 'RAWSQL', result ] ]

typeerror_re_1 = re.compile(r'\(\) takes (no|(?:exactly|at (?:least|most)))(?: (\d+))? arguments \((\d+) given\)')
typeerror_re_2 = re.compile(r'\(\) takes from (\d+) to (\d+) positional arguments but (\d+) were given')

def reraise_improved_typeerror(exc, func_name, orig_func_name):
    if not exc.args: throw(exc)
    msg = exc.args[0]
    if not msg.startswith(func_name): throw(exc)
    msg = msg[len(func_name):]

    match = typeerror_re_1.match(msg)
    if match:
        what, takes, given = match.groups()
        takes, given = int(takes), int(given)
        if takes: what = '%s %d' % (what, takes-1)
        plural = 's' if takes > 2 else ''
        new_msg = '%s() takes %s argument%s (%d given)' % (orig_func_name, what, plural, given-1)
        exc.args = (new_msg,)
        throw(exc)

    match = typeerror_re_2.match(msg)
    if match:
        start, end, given = match.groups()
        start, end, given = int(start)-1, int(end)-1, int(given)-1
        if not start:
            plural = 's' if end > 1 else ''
            new_msg = '%s() takes at most %d argument%s (%d given)' % (orig_func_name, end, plural, given)
        else:
            new_msg = '%s() takes from %d to %d arguments (%d given)' % (orig_func_name, start, end, given)
        exc.args = (new_msg,)
        throw(exc)

    exc.args = (orig_func_name + msg,)
    throw(exc)

def raise_forgot_parentheses(monad):
    assert monad.type == 'METHOD'
    throw(TranslationError, 'You seems to forgot parentheses after %s' % ast2src(monad.node))

class MethodMonad(Monad):
    def __init__(monad, parent, attrname):
        Monad.__init__(monad, 'METHOD', nullable=False)
        monad.parent = parent
        monad.attrname = attrname
    def getattr(monad, attrname):
        raise_forgot_parentheses(monad)
    def __call__(monad, *args, **kwargs):
        method = getattr(monad.parent, 'call_' + monad.attrname)
        try: return method(*args, **kwargs)
        except TypeError as exc: reraise_improved_typeerror(exc, method.__name__, monad.attrname)

    def contains(monad, item, not_in=False): raise_forgot_parentheses(monad)
    def nonzero(monad): raise_forgot_parentheses(monad)
    def negate(monad): raise_forgot_parentheses(monad)
    def aggregate(monad, func_name, distinct=None, sep=None): raise_forgot_parentheses(monad)
    def __getitem__(monad, key): raise_forgot_parentheses(monad)

    def __add__(monad, monad2): raise_forgot_parentheses(monad)
    def __sub__(monad, monad2): raise_forgot_parentheses(monad)
    def __mul__(monad, monad2): raise_forgot_parentheses(monad)
    def __truediv__(monad, monad2): raise_forgot_parentheses(monad)
    def __floordiv__(monad, monad2): raise_forgot_parentheses(monad)
    def __pow__(monad, monad2): raise_forgot_parentheses(monad)

    def __neg__(monad): raise_forgot_parentheses(monad)
    def abs(monad): raise_forgot_parentheses(monad)

class EntityMonad(Monad):
    def __init__(monad, entity):
        Monad.__init__(monad, SetType(entity))
        translator = monad.translator
        if translator.database is None:
            translator.database = entity._database_
        elif translator.database is not entity._database_:
            throw(TranslationError, 'All entities in a query must belong to the same database')
    def __getitem__(monad, *args):
        throw(NotImplementedError)

class ListMonad(Monad):
    def __init__(monad, items):
        Monad.__init__(monad, tuple(item.type for item in items))
        monad.items = items
    def contains(monad, x, not_in=False):
        if isinstance(x.type, SetType): throw(TypeError,
            "Type of `%s` is '%s'. Expression `{EXPR}` is not supported" % (ast2src(x.node), type2str(x.type)))
        for item in monad.items: check_comparable(x, item)
        left_sql = x.getsql()
        if len(left_sql) == 1:
            if not_in: sql = [ 'NOT_IN', left_sql[0], [ item.getsql()[0] for item in monad.items ] ]
            else: sql = [ 'IN', left_sql[0], [ item.getsql()[0] for item in monad.items ] ]
        elif not_in:
            sql = sqland([ sqlor([ [ 'NE', a, b ]  for a, b in izip(left_sql, item.getsql()) ]) for item in monad.items ])
        else:
            sql = sqlor([ sqland([ [ 'EQ', a, b ]  for a, b in izip(left_sql, item.getsql()) ]) for item in monad.items ])
        return BoolExprMonad(sql, nullable=x.nullable or any(item.nullable for item in monad.items))
    def getsql(monad, sqlquery=None):
        return [ [ 'ROW' ] + [ item.getsql()[0] for item in monad.items ] ]

class BufferMixin(MonadMixin):
    pass

class UuidMixin(MonadMixin):
    pass

_binop_errmsg = 'Unsupported operand types %r and %r for operation %r in expression: {EXPR}'

def make_numeric_binop(op, sqlop):
    def numeric_binop(monad, monad2):
        if isinstance(monad2, (AttrSetMonad, NumericSetExprMonad)):
            return NumericSetExprMonad(op, sqlop, monad, monad2)
        if monad2.type == 'METHOD': raise_forgot_parentheses(monad2)
        result_type, monad, monad2 = coerce_monads(monad, monad2)
        if result_type is None:
            throw(TypeError, _binop_errmsg % (type2str(monad.type), type2str(monad2.type), op))
        left_sql = monad.getsql()[0]
        right_sql = monad2.getsql()[0]
        return NumericExprMonad(result_type, [ sqlop, left_sql, right_sql ])
    numeric_binop.__name__ = sqlop
    return numeric_binop

class NumericMixin(MonadMixin):
    def mixin_init(monad):
        assert monad.type in numeric_types, monad.type
    __add__ = make_numeric_binop('+', 'ADD')
    __sub__ = make_numeric_binop('-', 'SUB')
    __mul__ = make_numeric_binop('*', 'MUL')
    __truediv__ = make_numeric_binop('/', 'DIV')
    __floordiv__ = make_numeric_binop('//', 'FLOORDIV')
    __mod__ = make_numeric_binop('%', 'MOD')
    def __pow__(monad, monad2):
        if not isinstance(monad2, NumericMixin):
            throw(TypeError, _binop_errmsg % (type2str(monad.type), type2str(monad2.type), '**'))
        left_sql = monad.getsql()
        right_sql = monad2.getsql()
        assert len(left_sql) == len(right_sql) == 1
        return NumericExprMonad(float, [ 'POW', left_sql[0], right_sql[0] ],
                                nullable=monad.nullable or monad2.nullable)
    def __neg__(monad):
        sql = monad.getsql()[0]
        return NumericExprMonad(monad.type, [ 'NEG', sql ], nullable=monad.nullable)
    def abs(monad):
        sql = monad.getsql()[0]
        return NumericExprMonad(monad.type, [ 'ABS', sql ], nullable=monad.nullable)
    def nonzero(monad):
        translator = monad.translator
        sql = monad.getsql()[0]
        if not (translator.dialect == 'PostgreSQL' and monad.type is bool):
            sql = [ 'NE', sql, [ 'VALUE', 0 ] ]
        return BoolExprMonad(sql, nullable=False)
    def negate(monad):
        sql = monad.getsql()[0]
        translator = monad.translator
        pg_bool = translator.dialect == 'PostgreSQL' and monad.type is bool
        result_sql = [ 'NOT', sql ] if pg_bool else [ 'EQ', sql, [ 'VALUE', 0 ] ]
        if monad.nullable:
            if isinstance(monad, AttrMonad):
                result_sql = [ 'OR', result_sql, [ 'IS_NULL', sql ] ]
            elif pg_bool:
                result_sql = [ 'NOT', [ 'COALESCE', sql, [ 'VALUE', True ] ] ]
            else:
                result_sql = [ 'EQ', [ 'COALESCE', sql, [ 'VALUE', 0 ] ], [ 'VALUE', 0 ] ]
        return BoolExprMonad(result_sql, nullable=False)

def numeric_attr_factory(name):
    def attr_func(monad):
        sql = [ name, monad.getsql()[0] ]
        return NumericExprMonad(int, sql, nullable=monad.nullable)
    attr_func.__name__ = name.lower()
    return attr_func

def make_datetime_binop(op, sqlop):
    def datetime_binop(monad, monad2):
        if monad2.type != timedelta: throw(TypeError,
            _binop_errmsg % (type2str(monad.type), type2str(monad2.type), op))
        expr_monad_cls = DateExprMonad if monad.type is date else DatetimeExprMonad
        return expr_monad_cls(monad.type, [ sqlop, monad.getsql()[0], monad2.getsql()[0] ],
                              nullable=monad.nullable or monad2.nullable)
    datetime_binop.__name__ = sqlop
    return datetime_binop

class DateMixin(MonadMixin):
    def mixin_init(monad):
        assert monad.type is date

    attr_year = numeric_attr_factory('YEAR')
    attr_month = numeric_attr_factory('MONTH')
    attr_day = numeric_attr_factory('DAY')

    def __add__(monad, other):
        if other.type != timedelta:
            throw(TypeError, _binop_errmsg % (type2str(monad.type), type2str(other.type), '+'))
        return DateExprMonad(monad.type, [ 'DATE_ADD', monad.getsql()[0], other.getsql()[0] ],
                             nullable=monad.nullable or other.nullable)

    def __sub__(monad, other):
        if other.type == timedelta:
            return DateExprMonad(monad.type, [ 'DATE_SUB', monad.getsql()[0], other.getsql()[0] ],
                                 nullable=monad.nullable or other.nullable)
        elif other.type == date:
            return TimedeltaExprMonad(timedelta, [ 'DATE_DIFF', monad.getsql()[0], other.getsql()[0] ],
                                      nullable=monad.nullable or other.nullable)
        throw(TypeError, _binop_errmsg % (type2str(monad.type), type2str(other.type), '-'))


class TimeMixin(MonadMixin):
    def mixin_init(monad):
        assert monad.type is time
    attr_hour = numeric_attr_factory('HOUR')
    attr_minute = numeric_attr_factory('MINUTE')
    attr_second = numeric_attr_factory('SECOND')

class TimedeltaMixin(MonadMixin):
    def mixin_init(monad):
        assert monad.type is timedelta

class DatetimeMixin(DateMixin):
    def mixin_init(monad):
        assert monad.type is datetime

    def call_date(monad):
        sql = [ 'DATE', monad.getsql()[0] ]
        return ExprMonad.new(date, sql, nullable=monad.nullable)

    attr_hour = numeric_attr_factory('HOUR')
    attr_minute = numeric_attr_factory('MINUTE')
    attr_second = numeric_attr_factory('SECOND')

    def __add__(monad, other):
        if other.type != timedelta:
            throw(TypeError, _binop_errmsg % (type2str(monad.type), type2str(other.type), '+'))
        return DatetimeExprMonad(monad.type, [ 'DATETIME_ADD', monad.getsql()[0], other.getsql()[0] ],
                             nullable=monad.nullable or other.nullable)

    def __sub__(monad, other):
        if other.type == timedelta:
            return DatetimeExprMonad(monad.type, [ 'DATETIME_SUB', monad.getsql()[0], other.getsql()[0] ],
                                     nullable=monad.nullable or other.nullable)
        elif other.type == datetime:
            return TimedeltaExprMonad(timedelta, [ 'DATETIME_DIFF', monad.getsql()[0], other.getsql()[0] ],
                                      nullable=monad.nullable or other.nullable)
        throw(TypeError, _binop_errmsg % (type2str(monad.type), type2str(other.type), '-'))

def make_string_binop(op, sqlop):
    def string_binop(monad, monad2):
        if not are_comparable_types(monad.type, monad2.type, sqlop):
            if monad2.type == 'METHOD': raise_forgot_parentheses(monad2)
            throw(TypeError, _binop_errmsg % (type2str(monad.type), type2str(monad2.type), op))
        left_sql = monad.getsql()
        right_sql = monad2.getsql()
        assert len(left_sql) == len(right_sql) == 1
        return StringExprMonad(monad.type, [ sqlop, left_sql[0], right_sql[0] ],
                               nullable=monad.nullable or monad2.nullable)
    string_binop.__name__ = sqlop
    return string_binop

def make_string_func(sqlop):
    def func(monad):
        sql = monad.getsql()
        assert len(sql) == 1
        return StringExprMonad(monad.type, [ sqlop, sql[0] ], nullable=monad.nullable)
    func.__name__ = sqlop
    return func

class StringMixin(MonadMixin):
    def mixin_init(monad):
        assert issubclass(monad.type, basestring), monad.type
    __add__ = make_string_binop('+', 'CONCAT')
    def __getitem__(monad, index):
        root_translator = monad.translator.root_translator
        dialect = root_translator.database.provider.dialect

        def param_to_const(monad, is_start=True):
            if isinstance(monad, ParamMonad):
                key = monad.paramkey[0]
                if key in root_translator.fixed_param_values:
                    index_value = root_translator.fixed_param_values[key]
                else:
                    index_value = root_translator.vars[key]
                    if index_value is None:
                        index_value = 0 if is_start else -1
                    root_translator.fixed_param_values[key] = index_value
                return ConstMonad.new(index_value)
            return monad

        if isinstance(index, ListMonad): throw(TypeError, "String index must be of 'int' type. Got 'tuple' in {EXPR}")
        elif isinstance(index, slice):
            if index.step is not None: throw(TypeError, 'Step is not supported in {EXPR}')
            start, stop = index.start, index.stop
            start = param_to_const(start, is_start=True)
            stop = param_to_const(stop, is_start=False)
            start_value = stop_value = None
            if start is None: start_value = 0
            if stop_value is None: stop_value = -1
            if isinstance(start, ConstMonad): start_value = start.value
            if isinstance(stop, ConstMonad): stop_value = stop.value
            if start_value == 0 and stop_value == -1:
                return monad
            if isinstance(monad, StringConstMonad) and start_value is not None and stop_value is not None:
                return ConstMonad.new(monad.value[start_value:stop_value])

            if start is not None and start.type is not int:
                throw(TypeError, "Invalid type of start index (expected 'int', got %r) in string slice {EXPR}" % type2str(start.type))
            if stop is not None and stop.type is not int:
                throw(TypeError, "Invalid type of stop index (expected 'int', got %r) in string slice {EXPR}" % type2str(stop.type))
            expr_sql = monad.getsql()[0]

            start_sql = None if start is None else start.getsql()[0]
            stop_sql = None if stop is None else stop.getsql()[0]
            sql = [ 'STRING_SLICE', expr_sql, start_sql, stop_sql ]
            return StringExprMonad(monad.type, sql, nullable=
                monad.nullable or start is not None and start.nullable or stop is not None and stop.nullable)

        index = param_to_const(index)
        if isinstance(monad, StringConstMonad) and isinstance(index, NumericConstMonad):
            return ConstMonad.new(monad.value[index.value])
        if index.type is not int: throw(TypeError,
            'String indices must be integers. Got %r in expression {EXPR}' % type2str(index.type))
        expr_sql = monad.getsql()[0]

        if isinstance(index, NumericConstMonad):
            value = index.value
            if dialect == 'PostgreSQL' and value < 0:
                index_sql = [ 'LENGTH', expr_sql ]
                if value < -1:
                    index_sql = [ 'SUB', index_sql, [ 'VALUE', -(value + 1) ] ]
            else:
                if value >= 0: value += 1
                index_sql = [ 'VALUE', value ]
        else:
            inner_sql = index.getsql()[0]
            then = ['ADD', inner_sql, ['VALUE', 1]]
            else_ = [ 'ADD', ['LENGTH', expr_sql], then ] if dialect == 'PostgreSQL' else inner_sql
            index_sql = [ 'IF', [ 'GE', inner_sql, [ 'VALUE', 0 ] ], then, else_ ]

        sql = [ 'SUBSTR', expr_sql, index_sql, [ 'VALUE', 1 ] ]
        return StringExprMonad(monad.type, sql, nullable=monad.nullable)
    def negate(monad):
        sql = monad.getsql()[0]
        translator = monad.translator
        if translator.dialect == 'Oracle':
            result_sql = [ 'IS_NULL', sql ]
        else:
            result_sql = [ 'EQ', sql, [ 'VALUE', '' ] ]
            if monad.nullable:
                if isinstance(monad, AttrMonad):
                    result_sql = [ 'OR', result_sql, [ 'IS_NULL', sql ] ]
                else:
                    result_sql = [ 'EQ', [ 'COALESCE', sql, [ 'VALUE', '' ] ], [ 'VALUE', '' ]]
        result = BoolExprMonad(result_sql, nullable=False)
        result.aggregated = monad.aggregated
        return result
    def nonzero(monad):
        sql = monad.getsql()[0]
        translator = monad.translator
        if translator.dialect == 'Oracle':
            result_sql = [ 'IS_NOT_NULL', sql ]
        else:
            result_sql = [ 'NE', sql, [ 'VALUE', '' ] ]
        result = BoolExprMonad(result_sql, nullable=False)
        result.aggregated = monad.aggregated
        return result
    def len(monad):
        sql = monad.getsql()[0]
        return NumericExprMonad(int, [ 'LENGTH', sql ])
    def contains(monad, item, not_in=False):
        check_comparable(item, monad, 'LIKE')
        return monad._like(item, before='%', after='%', not_like=not_in)
    call_upper = make_string_func('UPPER')
    call_lower = make_string_func('LOWER')
    def call_startswith(monad, arg):
        if not are_comparable_types(monad.type, arg.type, None):
            if arg.type == 'METHOD': raise_forgot_parentheses(arg)
            throw(TypeError, 'Expected %r argument but got %r in expression {EXPR}'
                            % (type2str(monad.type), type2str(arg.type)))
        return monad._like(arg, after='%')
    def call_endswith(monad, arg):
        if not are_comparable_types(monad.type, arg.type, None):
            if arg.type == 'METHOD': raise_forgot_parentheses(arg)
            throw(TypeError, 'Expected %r argument but got %r in expression {EXPR}'
                            % (type2str(monad.type), type2str(arg.type)))
        return monad._like(arg, before='%')
    def _like(monad, item, before=None, after=None, not_like=False):
        escape = False
        translator = monad.translator
        if isinstance(item, StringConstMonad):
            value = item.value
            if '%' in value or '_' in value:
                escape = True
                value = value.replace('!', '!!').replace('%', '!%').replace('_', '!_')
            if before: value = before + value
            if after: value = value + after
            item_sql = [ 'VALUE', value ]
        else:
            escape = True
            item_sql = item.getsql()[0]
            item_sql = [ 'REPLACE', item_sql, [ 'VALUE', '!' ], [ 'VALUE', '!!' ] ]
            item_sql = [ 'REPLACE', item_sql, [ 'VALUE', '%' ], [ 'VALUE', '!%' ] ]
            item_sql = [ 'REPLACE', item_sql, [ 'VALUE', '_' ], [ 'VALUE', '!_' ] ]
            if before and after: item_sql = [ 'CONCAT', [ 'VALUE', before ], item_sql, [ 'VALUE', after ] ]
            elif before: item_sql = [ 'CONCAT', [ 'VALUE', before ], item_sql ]
            elif after: item_sql = [ 'CONCAT', item_sql, [ 'VALUE', after ] ]
        sql = monad.getsql()[0]
        if not_like and monad.nullable and not isinstance(monad, AttrMonad) and translator.dialect != 'Oracle':
            sql = [ 'COALESCE', sql, [ 'VALUE', '' ] ]
        result_sql = [ 'NOT_LIKE' if not_like else 'LIKE', sql, item_sql ]
        if escape: result_sql.append([ 'VALUE', '!' ])
        if not_like and monad.nullable and (isinstance(monad, AttrMonad) or translator.dialect == 'Oracle'):
            result_sql = [ 'OR', result_sql, [ 'IS_NULL', sql ] ]
        return BoolExprMonad(result_sql, nullable=not_like)
    def strip(monad, chars, strip_type):
        if chars is not None and not are_comparable_types(monad.type, chars.type, None):
            if chars.type == 'METHOD': raise_forgot_parentheses(chars)
            throw(TypeError, "'chars' argument must be of %r type in {EXPR}, got: %r"
                            % (type2str(monad.type), type2str(chars.type)))
        parent_sql = monad.getsql()[0]
        sql = [ strip_type, parent_sql ]
        if chars is not None: sql.append(chars.getsql()[0])
        return StringExprMonad(monad.type, sql, nullable=monad.nullable)
    def call_strip(monad, chars=None):
        return monad.strip(chars, 'TRIM')
    def call_lstrip(monad, chars=None):
        return monad.strip(chars, 'LTRIM')
    def call_rstrip(monad, chars=None):
        return monad.strip(chars, 'RTRIM')

class JsonMixin(object):
    disable_distinct = True  # at least in Oracle we cannot use DISTINCT with JSON column
    disable_ordering = True  # at least in Oracle we cannot use ORDER BY with JSON column

    def mixin_init(monad):
        assert monad.type is Json, monad.type
    def get_path(monad):
        return monad, []
    def __getitem__(monad, key):
        return JsonItemMonad(monad, key)
    def contains(monad, key, not_in=False):
        translator = monad.translator
        if isinstance(key, ParamMonad):
            if translator.dialect == 'Oracle': throw(TypeError,
                'For `key in JSON` operation %s supports literal key values only, '
                'parameters are not allowed: {EXPR}' % translator.dialect)
        elif not isinstance(key, StringConstMonad): raise NotImplementedError
        base_monad, path = monad.get_path()
        base_sql = base_monad.getsql()[0]
        key_sql = key.getsql()[0]
        sql = [ 'JSON_CONTAINS', base_sql, path, key_sql ]
        if not_in: sql = [ 'NOT', sql ]
        return BoolExprMonad(sql)
    def __or__(monad, other):
        if not isinstance(other, JsonMixin):
            raise TypeError('Should be JSON: %s' % ast2src(other.node))
        left_sql = monad.getsql()[0]
        right_sql = other.getsql()[0]
        sql = [ 'JSON_CONCAT', left_sql, right_sql ]
        return JsonExprMonad(Json, sql)
    def len(monad):
        sql = [ 'JSON_ARRAY_LENGTH', monad.getsql()[0] ]
        return NumericExprMonad(int, sql)
    def cast_from_json(monad, type):
        if type in (Json, NoneType): return monad
        throw(TypeError, 'Cannot compare whole JSON value, you need to select specific sub-item: {EXPR}')
    def nonzero(monad):
        return BoolExprMonad([ 'JSON_NONZERO', monad.getsql()[0] ])

class ArrayMixin(MonadMixin):
    def contains(monad, key, not_in=False):
        if key.type is monad.type.item_type:
            sql = 'ARRAY_CONTAINS', key.getsql()[0], not_in, monad.getsql()[0]
            return BoolExprMonad(sql)
        if isinstance(key, ListMonad):
            if not key.items:
                if not_in:
                    return BoolExprMonad(['EQ', ['VALUE', 0], ['VALUE', 1]], nullable=False)
                else:
                    return BoolExprMonad(['EQ', ['VALUE', 1], ['VALUE', 1]], nullable=False)
            sql = [ 'MAKE_ARRAY' ]
            sql.extend(item.getsql()[0] for item in key.items)
            sql = 'ARRAY_SUBSET', sql, not_in, monad.getsql()[0]
            return BoolExprMonad(sql)
        elif isinstance(key, ArrayParamMonad):
            sql = 'ARRAY_SUBSET', key.getsql()[0], not_in, monad.getsql()[0]
            return BoolExprMonad(sql)
        throw(TypeError, 'Cannot search for %s in %s: {EXPR}' %
              (type2str(key.type), type2str(monad.type)))

    def len(monad):
        sql = ['ARRAY_LENGTH', monad.getsql()[0]]
        return NumericExprMonad(int, sql)

    def nonzero(monad):
        return BoolExprMonad(['GT', ['ARRAY_LENGTH', monad.getsql()[0]], ['VALUE', 0]])

    def _index(monad, index, from_one, plus_one):
        if isinstance(index, NumericConstMonad):
            expr_sql = monad.getsql()[0]
            index_sql = index.getsql()[0]
            value = index_sql[1]
            if value >= 0:
                index_sql = ['VALUE', value + int(from_one and plus_one)]
            else:
                index_sql = ['SUB', ['ARRAY_LENGTH', expr_sql], ['VALUE', abs(value + int(from_one and plus_one))]]
            return index_sql
        elif isinstance(index, NumericMixin):
            expr_sql = monad.getsql()[0]
            index0 = index.getsql()[0]
            index1 = ['ADD', index0, ['VALUE', 1]] if from_one and plus_one else index0
            index_sql = ['CASE', None, [[['GE', index0, ['VALUE', 0]], index1]],
                     ['ADD', ['ARRAY_LENGTH', expr_sql], index1]]
            return index_sql

    def __getitem__(monad, index):
        dialect = monad.translator.database.provider.dialect
        expr_sql = monad.getsql()[0]
        from_one = dialect != 'SQLite'
        if isinstance(index, NumericMixin):
            index_sql = monad._index(index, from_one, plus_one=True)
            sql = ['ARRAY_INDEX', expr_sql, index_sql]
            return ExprMonad.new(monad.type.item_type, sql)
        elif isinstance(index, slice):
            if index.step is not None: throw(TypeError, 'Step is not supported in {EXPR}')
            start_sql = monad._index(index.start, from_one, plus_one=True)
            stop_sql = monad._index(index.stop, from_one, plus_one=False)
            sql = ['ARRAY_SLICE', expr_sql, start_sql, stop_sql]
            return ExprMonad.new(monad.type, sql)


class ObjectMixin(MonadMixin):
    def mixin_init(monad):
        assert isinstance(monad.type, EntityMeta)
    def negate(monad):
        return CmpMonad('is', monad, NoneMonad())
    def nonzero(monad):
        return CmpMonad('is not', monad, NoneMonad())
    def getattr(monad, attrname):
        entity = monad.type
        attr = entity._adict_.get(attrname) or entity._subclass_adict_.get(attrname)
        if attr is None:
            if hasattr(entity, attrname):
                attr = getattr(entity, attrname, None)
                if isinstance(attr, property):
                    new_monad = HybridMethodMonad(monad, attrname, attr.fget)
                    return new_monad()
                if callable(attr):
                    func = getattr(attr, '__func__') if PY2 else attr
                    if func is not None: return HybridMethodMonad(monad, attrname, func)
                throw(NotImplementedError, '{EXPR} cannot be translated to SQL')
            throw(AttributeError, 'Entity %s does not have attribute %s: {EXPR}' % (entity.__name__, attrname))
        if hasattr(monad, 'tableref'): monad.tableref.used_attrs.add(attr)
        if not attr.is_collection:
            return AttrMonad.new(monad, attr)
        else:
            return AttrSetMonad(monad, attr)
    def requires_distinct(monad, joined=False):
        return monad.attr.reverse.is_collection or monad.parent.requires_distinct(joined)  # parent ???

class ObjectIterMonad(ObjectMixin, Monad):
    def __init__(monad, tableref, entity):
        Monad.__init__(monad, entity)
        monad.tableref = tableref
    def getsql(monad, sqlquery=None):
        entity = monad.type
        alias, pk_columns = monad.tableref.make_join(pk_only=True)
        return [ [ 'COLUMN', alias, column ] for column in pk_columns ]
    def requires_distinct(monad, joined=False):
        return monad.tableref.name_path != monad.translator.tree.quals[-1].assign.name

class AttrMonad(Monad):
    @staticmethod
    def new(parent, attr, *args, **kwargs):
        t = normalize_type(attr.py_type)
        if t in numeric_types: cls = NumericAttrMonad
        elif t is unicode: cls = StringAttrMonad
        elif t is date: cls = DateAttrMonad
        elif t is time: cls = TimeAttrMonad
        elif t is timedelta: cls = TimedeltaAttrMonad
        elif t is datetime: cls = DatetimeAttrMonad
        elif t is buffer: cls = BufferAttrMonad
        elif t is UUID: cls = UuidAttrMonad
        elif t is Json: cls = JsonAttrMonad
        elif isinstance(t, EntityMeta): cls = ObjectAttrMonad
        elif isinstance(t, type) and issubclass(t, Array): cls = ArrayAttrMonad
        else: throw(NotImplementedError, t)  # pragma: no cover
        return cls(parent, attr, *args, **kwargs)
    def __new__(cls, *args):
        if cls is AttrMonad: assert False, 'Abstract class'  # pragma: no cover
        return Monad.__new__(cls)
    def __init__(monad, parent, attr):
        assert monad.__class__ is not AttrMonad
        attr_type = normalize_type(attr.py_type)
        Monad.__init__(monad, attr_type)
        monad.parent = parent
        monad.attr = attr
        monad.nullable = attr.nullable
    def getsql(monad, sqlquery=None):
        parent = monad.parent
        attr = monad.attr
        entity = attr.entity
        pk_only = attr.pk_offset is not None
        alias, parent_columns = monad.parent.tableref.make_join(pk_only)
        if pk_only:
            if entity._pk_is_composite_:
                offset = attr.pk_columns_offset
                columns = parent_columns[offset:offset+len(attr.columns)]
            else: columns = parent_columns
        elif not attr.columns:
            assert isinstance(monad, ObjectAttrMonad)
            sqlquery = monad.translator.sqlquery
            monad.translator.left_join = sqlquery.left_join = True
            sqlquery.from_ast[0] = 'LEFT_JOIN'
            alias, columns = monad.tableref.make_join()
        else: columns = attr.columns
        return [ [ 'COLUMN', alias, column ] for column in columns ]

class ObjectAttrMonad(ObjectMixin, AttrMonad):
    def __init__(monad, parent, attr):
        AttrMonad.__init__(monad, parent, attr)
        translator = monad.translator
        parent_monad = monad.parent
        entity = monad.type
        name_path = '-'.join((parent_monad.tableref.name_path, attr.name))
        monad.tableref = translator.sqlquery.get_tableref(name_path)
        if monad.tableref is None:
            parent_sqlquery = parent_monad.tableref.sqlquery
            monad.tableref = parent_sqlquery.add_tableref(name_path, parent_monad.tableref, attr)

class StringAttrMonad(StringMixin, AttrMonad): pass
class NumericAttrMonad(NumericMixin, AttrMonad): pass
class DateAttrMonad(DateMixin, AttrMonad): pass
class TimeAttrMonad(TimeMixin, AttrMonad): pass
class TimedeltaAttrMonad(TimedeltaMixin, AttrMonad): pass
class DatetimeAttrMonad(DatetimeMixin, AttrMonad): pass
class BufferAttrMonad(BufferMixin, AttrMonad): pass
class UuidAttrMonad(UuidMixin, AttrMonad): pass
class JsonAttrMonad(JsonMixin, AttrMonad): pass
class ArrayAttrMonad(ArrayMixin, AttrMonad): pass

class ParamMonad(Monad):
    @staticmethod
    def new(t, paramkey):
        t = normalize_type(t)
        if t in numeric_types: cls = NumericParamMonad
        elif t is unicode: cls = StringParamMonad
        elif t is date: cls = DateParamMonad
        elif t is time: cls = TimeParamMonad
        elif t is timedelta: cls = TimedeltaParamMonad
        elif t is datetime: cls = DatetimeParamMonad
        elif t is buffer: cls = BufferParamMonad
        elif t is UUID: cls = UuidParamMonad
        elif t is Json: cls = JsonParamMonad
        elif isinstance(t, type) and issubclass(t, Array): cls = ArrayParamMonad
        elif isinstance(t, EntityMeta): cls = ObjectParamMonad
        else: throw(NotImplementedError, 'Parameter {EXPR} has unsupported type %r' % (t,))
        result = cls(t, paramkey)
        result.aggregated = False
        return result
    def __new__(cls, *args, **kwargs):
        if cls is ParamMonad: assert False, 'Abstract class'  # pragma: no cover
        return Monad.__new__(cls)
    def __init__(monad, t, paramkey):
        t = normalize_type(t)
        Monad.__init__(monad, t, nullable=False)
        monad.paramkey = paramkey
        if not isinstance(t, EntityMeta):
            provider = monad.translator.database.provider
            monad.converter = provider.get_converter_by_py_type(t)
        else: monad.converter = None
    def getsql(monad, sqlquery=None):
        return [ [ 'PARAM', monad.paramkey, monad.converter ] ]

class ObjectParamMonad(ObjectMixin, ParamMonad):
    def __init__(monad, entity, paramkey):
        ParamMonad.__init__(monad, entity, paramkey)
        if monad.translator.database is not entity._database_:
            assert monad.translator.database is entity._database_, (paramkey, monad.translator.database, entity._database_)
        varkey, i, j = paramkey
        assert j is None
        monad.params = tuple((varkey, i, j) for j in xrange(len(entity._pk_converters_)))
    def getsql(monad, sqlquery=None):
        entity = monad.type
        assert len(monad.params) == len(entity._pk_converters_)
        return [ [ 'PARAM', param, converter ] for param, converter in izip(monad.params, entity._pk_converters_) ]
    def requires_distinct(monad, joined=False):
        assert False  # pragma: no cover

class StringParamMonad(StringMixin, ParamMonad): pass
class NumericParamMonad(NumericMixin, ParamMonad): pass
class DateParamMonad(DateMixin, ParamMonad): pass
class TimeParamMonad(TimeMixin, ParamMonad): pass
class TimedeltaParamMonad(TimedeltaMixin, ParamMonad): pass
class DatetimeParamMonad(DatetimeMixin, ParamMonad): pass
class BufferParamMonad(BufferMixin, ParamMonad): pass
class UuidParamMonad(UuidMixin, ParamMonad): pass

class ArrayParamMonad(ArrayMixin, ParamMonad):
    def __init__(monad, t, paramkey, list_monad=None):
        ParamMonad.__init__(monad, t, paramkey)
        monad.list_monad = list_monad
    def contains(monad, key, not_in=False):
        if key.type is monad.type.item_type:
            return monad.list_monad.contains(key, not_in)
        return ArrayMixin.contains(monad, key, not_in)

class JsonParamMonad(JsonMixin, ParamMonad):
    def getsql(monad, sqlquery=None):
        return [ [ 'JSON_PARAM', ParamMonad.getsql(monad)[0] ] ]

class ExprMonad(Monad):
    @staticmethod
    def new(t, sql, nullable=True):
        if t in numeric_types: cls = NumericExprMonad
        elif t is unicode: cls = StringExprMonad
        elif t is date: cls = DateExprMonad
        elif t is time: cls = TimeExprMonad
        elif t is timedelta: cls = TimedeltaExprMonad
        elif t is datetime: cls = DatetimeExprMonad
        elif t is Json: cls = JsonExprMonad
        elif isinstance(t, EntityMeta): cls = ObjectExprMonad
        elif isinstance(t, type) and issubclass(t, Array): cls = ArrayExprMonad
        else: throw(NotImplementedError, t)  # pragma: no cover
        return cls(t, sql, nullable=nullable)
    def __new__(cls, *args, **kwargs):
        if cls is ExprMonad: assert False, 'Abstract class'  # pragma: no cover
        return Monad.__new__(cls)
    def __init__(monad, type, sql, nullable=True):
        Monad.__init__(monad, type, nullable=nullable)
        monad.sql = sql
    def getsql(monad, sqlquery=None):
        return [ monad.sql ]

class ObjectExprMonad(ObjectMixin, ExprMonad):
    def getsql(monad, sqlquery=None):
        return monad.sql

class StringExprMonad(StringMixin, ExprMonad): pass
class NumericExprMonad(NumericMixin, ExprMonad): pass
class DateExprMonad(DateMixin, ExprMonad): pass
class TimeExprMonad(TimeMixin, ExprMonad): pass
class TimedeltaExprMonad(TimedeltaMixin, ExprMonad): pass
class DatetimeExprMonad(DatetimeMixin, ExprMonad): pass
class JsonExprMonad(JsonMixin, ExprMonad): pass
class ArrayExprMonad(ArrayMixin, ExprMonad): pass

class JsonItemMonad(JsonMixin, Monad):
    def __init__(monad, parent, key):
        assert isinstance(parent, JsonMixin), parent
        Monad.__init__(monad, Json)
        monad.parent = parent
        if isinstance(key, slice):
            if key != slice(None, None, None): throw(NotImplementedError)
            monad.key_ast = [ 'VALUE', key ]
        elif isinstance(key, (ParamMonad, StringConstMonad, NumericConstMonad, EllipsisMonad)):
            monad.key_ast = key.getsql()[0]
        else: throw(TypeError, 'Invalid JSON path item: %s' % ast2src(key.node))
        translator = monad.translator
        if isinstance(key, (slice, EllipsisMonad)) and not translator.json_path_wildcard_syntax:
            throw(TranslationError, '%s does not support wildcards in JSON path: {EXPR}' % translator.dialect)
    def get_path(monad):
        path = []
        while isinstance(monad, JsonItemMonad):
            path.append(monad.key_ast)
            monad = monad.parent
        path.reverse()
        return monad, path
    def to_int(monad):
        return monad.cast_from_json(int)
    def to_str(monad):
        return monad.cast_from_json(unicode)
    def to_real(monad):
        return monad.cast_from_json(float)
    def cast_from_json(monad, type):
        translator = monad.translator
        if issubclass(type, Json):
            if not translator.json_values_are_comparable: throw(TranslationError,
                '%s does not support comparison of json structures: {EXPR}' % translator.dialect)
            return monad
        base_monad, path = monad.get_path()
        sql = [ 'JSON_VALUE', base_monad.getsql()[0], path, type ]
        return ExprMonad.new(Json if type is NoneType else type, sql)
    def getsql(monad):
        base_monad, path = monad.get_path()
        base_sql = base_monad.getsql()[0]
        translator = monad.translator
        if translator.inside_order_by and translator.dialect == 'SQLite':
            return [ [ 'JSON_VALUE', base_sql, path, None ] ]
        return [ [ 'JSON_QUERY', base_sql, path ] ]

class ConstMonad(Monad):
    @staticmethod
    def new(value):
        value_type, value = normalize(value)
        if isinstance(value_type, tuple):
            return ListMonad([ConstMonad.new(item) for item in value])
        elif value_type in numeric_types: cls = NumericConstMonad
        elif value_type is unicode: cls = StringConstMonad
        elif value_type is date: cls = DateConstMonad
        elif value_type is time: cls = TimeConstMonad
        elif value_type is timedelta: cls = TimedeltaConstMonad
        elif value_type is datetime: cls = DatetimeConstMonad
        elif value_type is NoneType: cls = NoneMonad
        elif value_type is buffer: cls = BufferConstMonad
        elif value_type is Json: cls = JsonConstMonad
        elif issubclass(value_type, type(Ellipsis)): cls = EllipsisMonad
        else: throw(NotImplementedError, value_type)  # pragma: no cover
        result = cls(value)
        result.aggregated = False
        return result
    def __new__(cls, *args):
        if cls is ConstMonad: assert False, 'Abstract class'  # pragma: no cover
        return Monad.__new__(cls)
    def __init__(monad, value):
        value_type, value = normalize(value)
        Monad.__init__(monad, value_type, nullable=value_type is NoneType)
        monad.value = value
    def getsql(monad, sqlquery=None):
        return [ [ 'VALUE', monad.value ] ]

class NoneMonad(ConstMonad):
    type = NoneType
    def __init__(monad, value=None):
        assert value is None
        ConstMonad.__init__(monad, value)

class EllipsisMonad(ConstMonad):
    pass

class StringConstMonad(StringMixin, ConstMonad):
    def len(monad):
        return ConstMonad.new(len(monad.value))

class JsonConstMonad(JsonMixin, ConstMonad): pass
class BufferConstMonad(BufferMixin, ConstMonad): pass
class NumericConstMonad(NumericMixin, ConstMonad): pass
class DateConstMonad(DateMixin, ConstMonad): pass
class TimeConstMonad(TimeMixin, ConstMonad): pass
class TimedeltaConstMonad(TimedeltaMixin, ConstMonad): pass
class DatetimeConstMonad(DatetimeMixin, ConstMonad): pass

class BoolMonad(Monad):
    def __init__(monad, nullable=True):
        Monad.__init__(monad, bool, nullable=nullable)
    def nonzero(monad):
        return monad

sql_negation = { 'IN' : 'NOT_IN', 'EXISTS' : 'NOT_EXISTS', 'LIKE' : 'NOT_LIKE', 'BETWEEN' : 'NOT_BETWEEN', 'IS_NULL' : 'IS_NOT_NULL' }
sql_negation.update((value, key) for key, value in items_list(sql_negation))

class BoolExprMonad(BoolMonad):
    def __init__(monad, sql, nullable=True):
        BoolMonad.__init__(monad, nullable=nullable)
        monad.sql = sql
    def getsql(monad, sqlquery=None):
        return [ monad.sql ]
    def negate(monad):
        sql = monad.sql
        sqlop = sql[0]
        negated_op = sql_negation.get(sqlop)
        if negated_op is not None:
            negated_sql = [ negated_op ] + sql[1:]
        elif negated_op == 'NOT':
            assert len(sql) == 2
            negated_sql = sql[1]
        else: return NotMonad(monad)
        return BoolExprMonad(negated_sql, nullable=monad.nullable)

cmp_ops = { '>=' : 'GE', '>' : 'GT', '<=' : 'LE', '<' : 'LT' }

cmp_negate = { '<' : '>=', '<=' : '>', '==' : '!=', 'is' : 'is not' }
cmp_negate.update((b, a) for a, b in items_list(cmp_negate))

class CmpMonad(BoolMonad):
    EQ = 'EQ'
    NE = 'NE'
    def __init__(monad, op, left, right):
        if op == '<>': op = '!='
        if left.type is NoneType:
            assert right.type is not NoneType
            left, right = right, left
        if right.type is NoneType:
            if op == '==': op = 'is'
            elif op == '!=': op = 'is not'
        elif op == 'is': op = '=='
        elif op == 'is not': op = '!='
        check_comparable(left, right, op)
        result_type, left, right = coerce_monads(left, right, for_comparison=True)
        BoolMonad.__init__(monad, nullable=left.nullable or right.nullable)
        monad.op = op
        monad.aggregated = getattr(left, 'aggregated', False) or getattr(right, 'aggregated', False)

        if isinstance(left, JsonMixin):
            left = left.cast_from_json(right.type)
        if isinstance(right, JsonMixin):
            right = right.cast_from_json(left.type)

        monad.left = left
        monad.right = right
    def negate(monad):
        return CmpMonad(cmp_negate[monad.op], monad.left, monad.right)
    def getsql(monad, sqlquery=None):
        op = monad.op
        left_sql = monad.left.getsql()
        if op == 'is':
            return [ sqland([ [ 'IS_NULL', item ] for item in left_sql ]) ]
        if op == 'is not':
            return [ sqland([ [ 'IS_NOT_NULL', item ] for item in left_sql ]) ]
        right_sql = monad.right.getsql()
        if len(left_sql) == 1 and left_sql[0][0] == 'ROW':
            left_sql = left_sql[0][1:]
        if len(right_sql) == 1 and right_sql[0][0] == 'ROW':
            right_sql = right_sql[0][1:]
        assert len(left_sql) == len(right_sql)
        size = len(left_sql)
        if op in ('<', '<=', '>', '>='):
            if size == 1:
                return [ [ cmp_ops[op], left_sql[0], right_sql[0] ] ]
            if monad.translator.row_value_syntax:
                return [ [ cmp_ops[op], [ 'ROW' ] + left_sql, [ 'ROW' ] + right_sql ] ]
            clauses = []
            for i in xrange(size):
                clause = [ [ monad.EQ, left_sql[j], right_sql[j] ] for j in range(i) ]
                clause.append([ cmp_ops[op], left_sql[i], right_sql[i] ])
                clauses.append(sqland(clause))
            return [ sqlor(clauses) ]
        if op == '==':
            return [ sqland([ [ monad.EQ, a, b ] for a, b in izip(left_sql, right_sql) ]) ]
        if op == '!=':
            return [ sqlor([ [ monad.NE, a, b ] for a, b in izip(left_sql, right_sql) ]) ]
        assert False, op  # pragma: no cover

class LogicalBinOpMonad(BoolMonad):
    def __init__(monad, operands):
        assert len(operands) >= 2
        items = []
        for operand in operands:
            if operand.type is not bool: items.append(operand.nonzero())
            elif isinstance(operand, LogicalBinOpMonad) and monad.binop == operand.binop:
                items.extend(operand.operands)
            else: items.append(operand)
        nullable = any(item.nullable for item in items)
        BoolMonad.__init__(monad, nullable=nullable)
        monad.operands = items
    def getsql(monad, sqlquery=None):
        result = [ monad.binop ]
        for operand in monad.operands:
            operand_sql = operand.getsql()
            assert len(operand_sql) == 1
            result.extend(operand_sql)
        return [ result ]

class AndMonad(LogicalBinOpMonad):
    binop = 'AND'

class OrMonad(LogicalBinOpMonad):
    binop = 'OR'

class NotMonad(BoolMonad):
    def __init__(monad, operand):
        if operand.type is not bool: operand = operand.nonzero()
        BoolMonad.__init__(monad, nullable=operand.nullable)
        monad.operand = operand
    def negate(monad):
        return monad.operand
    def getsql(monad, sqlquery=None):
        return [ [ 'NOT', monad.operand.getsql()[0] ] ]

class HybridFuncMonad(Monad):
    def __init__(monad, func_type, func_name, *params):
        Monad.__init__(monad, func_type)
        monad.func = func_type.func
        monad.func_name = func_name
        monad.params = params
    def __call__(monad, *args, **kwargs):
        translator = monad.translator
        name_mapping = inspect.getcallargs(monad.func, *(monad.params + args), **kwargs)

        func = monad.func
        if PY2 and isinstance(func, types.UnboundMethodType):
            func = func.im_func
        func_id = id(func)
        try:
            func_ast, external_names, cells = decompile(func)
        except DecompileError:
            throw(TranslationError, '%s(...) is too complex to decompile' % ast2src(monad.node))

        func_ast, func_extractors = create_extractors(
            func_id, func_ast, func.__globals__, {}, special_functions, const_functions, outer_names=name_mapping)

        root_translator = translator.root_translator
        if func not in root_translator.func_extractors_map:
            func_vars, func_vartypes = extract_vars(func_id, translator.filter_num, func_extractors, func.__globals__, {}, cells)
            translator.database.provider.normalize_vars(func_vars, func_vartypes)
            if func.__closure__:
                translator.can_be_cached = False
            if func_extractors:
                root_translator.func_extractors_map[func] = func_extractors
                root_translator.func_vartypes.update(func_vartypes)
                root_translator.vartypes.update(func_vartypes)
                root_translator.vars.update(func_vars)

        func_ast = copy_ast(func_ast)
        stack = translator.namespace_stack
        stack.append(name_mapping)
        try:
            prev_code_key = translator.code_key
            translator.code_key = func_id
            try:
                translator.dispatch(func_ast)
            finally:
                translator.code_key = prev_code_key
        except Exception as e:
            if len(e.args) == 1 and isinstance(e.args[0], basestring):
                msg = e.args[0] + ' (inside %s)' % (monad.func_name)
                e.args = (msg,)
            raise
        finally:
            stack.pop()
        return func_ast.monad

class HybridMethodMonad(HybridFuncMonad):
    def __init__(monad, parent, attrname, func):
        entity = parent.type
        assert isinstance(entity, EntityMeta)
        func_name = '%s.%s' % (entity.__name__, attrname)
        HybridFuncMonad.__init__(monad, FuncType(func), func_name, parent)

registered_functions = SQLTranslator.registered_functions = {}

class FuncMonadMeta(MonadMeta):
    def __new__(meta, cls_name, bases, cls_dict):
        func = cls_dict.get('func')
        monad_cls = super(FuncMonadMeta, meta).__new__(meta, cls_name, bases, cls_dict)
        if func:
            if type(func) is tuple: functions = func
            else: functions = (func,)
            for func in functions: registered_functions[func] = monad_cls
        return monad_cls

class FuncMonad(with_metaclass(FuncMonadMeta, Monad)):
    def __call__(monad, *args, **kwargs):
        for arg in args:
            assert isinstance(arg, Monad)
        for value in kwargs.values():
            assert isinstance(value, Monad)
        try: return monad.call(*args, **kwargs)
        except TypeError as exc:
            reraise_improved_typeerror(exc, 'call', monad.type.__name__)

def get_classes(classinfo):
    if isinstance(classinfo, EntityMonad):
        yield classinfo.type.item_type
    elif isinstance(classinfo, ListMonad):
        for item in classinfo.items:
            for type in get_classes(item):
                yield type
    else: throw(TypeError, ast2src(classinfo.node))

class FuncIsinstanceMonad(FuncMonad):
    func = isinstance
    def call(monad, obj, classinfo):
        if not isinstance(obj, ObjectMixin): throw(ValueError,
            'Inside a query, isinstance first argument should be of entity type. Got: %s' % ast2src(obj.node))
        entity = obj.type
        classes = list(get_classes(classinfo))
        subclasses = set()
        for cls in classes:
            if entity._root_ is cls._root_:
                subclasses.add(cls)
                subclasses.update(cls._subclasses_)
        if entity in subclasses:
            return BoolExprMonad(['EQ', ['VALUE', 1], ['VALUE', 1]], nullable=False)

        subclasses.intersection_update(entity._subclasses_)
        if not subclasses:
            return BoolExprMonad(['EQ', ['VALUE', 0], ['VALUE', 1]], nullable=False)

        discr_attr = entity._discriminator_attr_
        assert discr_attr is not None
        discr_values = [ [ 'VALUE', cls._discriminator_ ] for cls in subclasses ]
        alias, pk_columns = obj.tableref.make_join(pk_only=True)
        sql = [ 'IN', [ 'COLUMN', alias, discr_attr.column ], discr_values ]
        return BoolExprMonad(sql, nullable=False)


class FuncBufferMonad(FuncMonad):
    func = buffer
    def call(monad, source, encoding=None, errors=None):
        if not isinstance(source, StringConstMonad): throw(TypeError)
        source = source.value
        if encoding is not None:
            if not isinstance(encoding, StringConstMonad): throw(TypeError)
            encoding = encoding.value
        if errors is not None:
            if not isinstance(errors, StringConstMonad): throw(TypeError)
            errors = errors.value
        if PY2:
            if encoding and errors: source = source.encode(encoding, errors)
            elif encoding: source = source.encode(encoding)
            return ConstMonad.new(buffer(source))
        else:
            if encoding and errors: value = buffer(source, encoding, errors)
            elif encoding: value = buffer(source, encoding)
            else: value = buffer(source)
            return ConstMonad.new(value)

class FuncBoolMonad(FuncMonad):
    func = bool
    def call(monad, x):
        return x.nonzero()

class FuncIntMonad(FuncMonad):
    func = int
    def call(monad, x):
        return x.to_int()

class FuncStrMonad(FuncMonad):
    func = str
    def call(monad, x):
        return x.to_str()

class FuncFloatMonad(FuncMonad):
    func = float
    def call(monad, x):
        return x.to_real()

class FuncDecimalMonad(FuncMonad):
    func = Decimal
    def call(monad, x):
        if not isinstance(x, StringConstMonad): throw(TypeError)
        return ConstMonad.new(Decimal(x.value))

class FuncDateMonad(FuncMonad):
    func = date
    def call(monad, year, month, day):
        for arg, name in izip((year, month, day), ('year', 'month', 'day')):
            if not isinstance(arg, NumericMixin) or arg.type is not int: throw(TypeError,
                "'%s' argument of date(year, month, day) function must be of 'int' type. "
                "Got: %r" % (name, type2str(arg.type)))
            if not isinstance(arg, ConstMonad): throw(NotImplementedError)
        return ConstMonad.new(date(year.value, month.value, day.value))
    def call_today(monad):
        return DateExprMonad(date, [ 'TODAY' ], nullable=monad.nullable)

class FuncTimeMonad(FuncMonad):
    func = time
    def call(monad, *args):
        for arg, name in izip(args, ('hour', 'minute', 'second', 'microsecond')):
            if not isinstance(arg, NumericMixin) or arg.type is not int: throw(TypeError,
                "'%s' argument of time(...) function must be of 'int' type. Got: %r" % (name, type2str(arg.type)))
            if not isinstance(arg, ConstMonad): throw(NotImplementedError)
        return ConstMonad.new(time(*tuple(arg.value for arg in args)))

class FuncTimedeltaMonad(FuncMonad):
    func = timedelta
    def call(monad, days=None, seconds=None, microseconds=None, milliseconds=None, minutes=None, hours=None, weeks=None):
        args = days, seconds, microseconds, milliseconds, minutes, hours, weeks
        for arg, name in izip(args, ('days', 'seconds', 'microseconds', 'milliseconds', 'minutes', 'hours', 'weeks')):
            if arg is None: continue
            if not isinstance(arg, NumericMixin) or arg.type is not int: throw(TypeError,
                "'%s' argument of timedelta(...) function must be of 'int' type. Got: %r" % (name, type2str(arg.type)))
            if not isinstance(arg, ConstMonad): throw(NotImplementedError)
        value = timedelta(*(arg.value if arg is not None else 0 for arg in args))
        return ConstMonad.new(value)

class FuncDatetimeMonad(FuncDateMonad):
    func = datetime
    def call(monad, year, month, day, hour=None, minute=None, second=None, microsecond=None):
        args = year, month, day, hour, minute, second, microsecond
        for arg, name in izip(args, ('year', 'month', 'day', 'hour', 'minute', 'second', 'microsecond')):
            if arg is None: continue
            if not isinstance(arg, NumericMixin) or arg.type is not int: throw(TypeError,
                "'%s' argument of datetime(...) function must be of 'int' type. Got: %r" % (name, type2str(arg.type)))
            if not isinstance(arg, ConstMonad): throw(NotImplementedError)
        value = datetime(*(arg.value if arg is not None else 0 for arg in args))
        return ConstMonad.new(value)
    def call_now(monad):
        return DatetimeExprMonad(datetime, [ 'NOW' ], nullable=monad.nullable)

class FuncBetweenMonad(FuncMonad):
    func = between
    def call(monad, x, a, b):
        check_comparable(x, a, '<')
        check_comparable(x, b, '<')
        if isinstance(x.type, EntityMeta): throw(TypeError,
            '%s instance cannot be argument of between() function: {EXPR}' % x.type.__name__)
        sql = [ 'BETWEEN', x.getsql()[0], a.getsql()[0], b.getsql()[0] ]
        return BoolExprMonad(sql, nullable=x.nullable or a.nullable or b.nullable)

class FuncConcatMonad(FuncMonad):
    func = concat
    def call(monad, *args):
        if len(args) < 2: throw(TranslationError, 'concat() function requires at least two arguments')
        result_ast = [ 'CONCAT' ]
        translator = monad.translator
        for arg in args:
            t = arg.type
            if isinstance(t, EntityMeta) or type(t) in (tuple, SetType):
                throw(TranslationError, 'Invalid argument of concat() function: %s' % ast2src(arg.node))
            if translator.database.provider_name == 'cockroach' and not isinstance(arg, StringMixin):
                arg = arg.to_str()
            result_ast.extend(arg.getsql())
        return ExprMonad.new(unicode, result_ast, nullable=any(arg.nullable for arg in args))

class FuncLenMonad(FuncMonad):
    func = len
    def call(monad, x):
        return x.len()

class FuncGetattrMonad(FuncMonad):
    func = getattr
    def call(monad, obj_monad, name_monad):
        if isinstance(name_monad, ConstMonad):
            attrname = name_monad.value
        elif isinstance(name_monad, ParamMonad):
            translator = monad.translator.root_translator
            key = name_monad.paramkey[0]
            if key in translator.fixed_param_values:
                attrname = translator.fixed_param_values[key]
            else:
                attrname = translator.vars[key]
                translator.fixed_param_values[key] = attrname
        else: throw(TranslationError, 'Expression `{EXPR}` cannot be translated into SQL '
                                      'because %s will be different for each row' % ast2src(name_monad.node))
        if not isinstance(attrname, basestring):
            throw(TypeError, 'In `{EXPR}` second argument should be a string. Got: %r' % attrname)
        return obj_monad.getattr(attrname)

class FuncRawSQLMonad(FuncMonad):
    func = raw_sql
    def call(monad, *args):
        throw(TranslationError, 'Expression `{EXPR}` cannot be translated into SQL '
                                'because raw SQL fragment will be different for each row')

class FuncCountMonad(FuncMonad):
    func = itertools.count, utils.count, core.count
    def call(monad, x=None, distinct=None):
        if isinstance(x, StringConstMonad) and x.value == '*': x = None
        if x is not None: return x.count(distinct)
        result = ExprMonad.new(int, [ 'COUNT', None ], nullable=False)
        result.aggregated = True
        return result

class FuncAbsMonad(FuncMonad):
    func = abs
    def call(monad, x):
        return x.abs()

class FuncSumMonad(FuncMonad):
    func = sum, core.sum
    def call(monad, x, distinct=None):
        return x.aggregate('SUM', distinct)

class FuncAvgMonad(FuncMonad):
    func = utils.avg, core.avg
    def call(monad, x, distinct=None):
        return x.aggregate('AVG', distinct)

class FuncGroupConcatMonad(FuncMonad):
    func = utils.group_concat, core.group_concat
    def call(monad, x, sep=None, distinct=None):
        if sep is not None:
            if distinct and monad.translator.database.provider.dialect == 'SQLite':
                throw(TypeError, 'SQLite does not allow to specify distinct and separator in group_concat at the same time: {EXPR}')
            if not(isinstance(sep, StringConstMonad) and isinstance(sep.value, basestring)):
                throw(TypeError, '`sep` option of `group_concat` should be type of str. Got: %s' % ast2src(sep.node))
            sep = sep.value
        return x.aggregate('GROUP_CONCAT', distinct=distinct, sep=sep)

class FuncCoalesceMonad(FuncMonad):
    func = coalesce
    def call(monad, *args):
        if len(args) < 2: throw(TranslationError, 'coalesce() function requires at least two arguments')
        arg = args[0].to_single_cell_value()
        t = arg.type
        result = [ [ sql ] for sql in arg.getsql() ]
        for arg in args[1:]:
            arg = arg.to_single_cell_value()
            if arg.type is not t:
                t2 = coerce_types(t, arg.type)
                if t2 is None:
                    throw(TypeError, 'All arguments of coalesce() function should have the same type')
                t = t2
            for i, sql in enumerate(arg.getsql()):
                result[i].append(sql)
        sql = [ [ 'COALESCE' ] + coalesce_args for coalesce_args in result ]
        if not isinstance(t, EntityMeta): sql = sql[0]
        return ExprMonad.new(t, sql, nullable=all(arg.nullable for arg in args))

class FuncDistinctMonad(FuncMonad):
    func = utils.distinct, core.distinct
    def call(monad, x):
        if isinstance(x, SetMixin): return x.call_distinct()
        if not isinstance(x, NumericMixin): throw(TypeError)
        result = object.__new__(x.__class__)
        result.__dict__.update(x.__dict__)
        result.forced_distinct = True
        return result

class FuncMinMonad(FuncMonad):
    func = min, core.min
    def call(monad, *args):
        if not args: throw(TypeError, 'min() function expected at least one argument')
        if len(args) == 1: return args[0].aggregate('MIN')
        return minmax(monad, 'MIN', *args)

class FuncMaxMonad(FuncMonad):
    func = max, core.max
    def call(monad, *args):
        if not args: throw(TypeError, 'max() function expected at least one argument')
        if len(args) == 1: return args[0].aggregate('MAX')
        return minmax(monad, 'MAX', *args)

def minmax(monad, sqlop, *args):
    assert len(args) > 1
    translator = monad.translator
    t = args[0].type
    if t == 'METHOD': raise_forgot_parentheses(args[0])
    if t not in comparable_types: throw(TypeError,
        "Value of type %r is not valid as argument of %r function in expression {EXPR}"
        % (type2str(t), sqlop.lower()))
    for arg in args[1:]:
        t2 = arg.type
        if t2 == 'METHOD': raise_forgot_parentheses(arg)
        t3 = coerce_types(t, t2)
        if t3 is None: throw(IncomparableTypesError, t, t2)
        t = t3
    if t3 in numeric_types and translator.dialect == 'PostgreSQL':
        args = list(args)
        for i, arg in enumerate(args):
            if arg.type is bool:
                args[i] = NumericExprMonad(int, [ 'TO_INT', arg.getsql()[0] ], nullable=arg.nullable)
    sql = [ sqlop, None ] + [ arg.getsql()[0] for arg in args ]
    return ExprMonad.new(t, sql, nullable=any(arg.nullable for arg in args))

class FuncSelectMonad(FuncMonad):
    func = core.select
    def call(monad, queryset):
        if not isinstance(queryset, QuerySetMonad): throw(TypeError,
            "'select' function expects generator expression, got: {EXPR}")
        return queryset

class FuncExistsMonad(FuncMonad):
    func = core.exists
    def call(monad, arg):
        if not isinstance(arg, SetMixin): throw(TypeError,
            "'exists' function expects generator expression or collection, got: {EXPR}")
        return arg.nonzero()

class FuncDescMonad(FuncMonad):
    func = core.desc
    def call(monad, expr):
        return DescMonad(expr)

class DescMonad(Monad):
    def __init__(monad, expr):
        Monad.__init__(monad, expr.type, nullable=expr.nullable)
        monad.expr = expr
    def getsql(monad):
        return [ [ 'DESC', item ] for item in monad.expr.getsql() ]

class JoinMonad(Monad):
    def __init__(monad, type):
        Monad.__init__(monad, type)
        translator = monad.translator
        monad.hint_join_prev = translator.hint_join
        translator.hint_join = True
    def __call__(monad, x):
        monad.translator.hint_join = monad.hint_join_prev
        return x
registered_functions[JOIN] = JoinMonad

class FuncRandomMonad(FuncMonad):
    func = random
    def __init__(monad, type):
        FuncMonad.__init__(monad, type)
        monad.translator.query_result_is_cacheable = False
    def __call__(monad):
        return NumericExprMonad(float, [ 'RANDOM' ], nullable=False)

class SetMixin(MonadMixin):
    forced_distinct = False
    def call_distinct(monad):
        new_monad = object.__new__(monad.__class__)
        new_monad.__dict__.update(monad.__dict__)
        new_monad.forced_distinct = True
        return new_monad

def make_attrset_binop(op, sqlop):
    def attrset_binop(monad, monad2):
        return NumericSetExprMonad(op, sqlop, monad, monad2)
    return attrset_binop

class AttrSetMonad(SetMixin, Monad):
    def __init__(monad, parent, attr):
        item_type = normalize_type(attr.py_type)
        Monad.__init__(monad, SetType(item_type))
        monad.parent = parent
        monad.attr = attr
        monad.sqlquery = None
        monad.tableref = None
    def cmp(monad, op, monad2):
        if type(monad2.type) is SetType \
           and are_comparable_types(monad.type.item_type, monad2.type.item_type): pass
        elif monad.type != monad2.type: check_comparable(monad, monad2)
        throw(NotImplementedError)
    def contains(monad, item, not_in=False):
        translator = monad.translator
        check_comparable(item, monad, 'in')
        if not translator.hint_join:
            sqlop = 'NOT_IN' if not_in else 'IN'
            sqlquery = monad._subselect()
            expr_list = sqlquery.expr_list
            from_ast = sqlquery.from_ast
            conditions = sqlquery.outer_conditions + sqlquery.conditions
            if len(expr_list) == 1:
                subquery_ast = [ 'SELECT', [ 'ALL' ] + expr_list, from_ast, [ 'WHERE' ] + conditions ]
                sql_ast = [ sqlop, item.getsql()[0], subquery_ast ]
            elif translator.row_value_syntax:
                subquery_ast = [ 'SELECT', [ 'ALL' ] + expr_list, from_ast, [ 'WHERE' ] + conditions ]
                sql_ast = [ sqlop, [ 'ROW' ] + item.getsql(), subquery_ast ]
            else:
                conditions += [ [ 'EQ', expr1, expr2 ] for expr1, expr2 in izip(item.getsql(), expr_list) ]
                sql_ast = [ 'NOT_EXISTS' if not_in else 'EXISTS', from_ast, [ 'WHERE' ] + conditions ]
            result = BoolExprMonad(sql_ast, nullable=False)
            result.nogroup = True
            return result
        elif not not_in:
            translator.distinct = True
            tableref = monad.make_tableref(translator.sqlquery)
            expr_list = monad.make_expr_list()
            expr_ast = sqland([ [ 'EQ', expr1, expr2 ]  for expr1, expr2 in izip(expr_list, item.getsql()) ])
            return BoolExprMonad(expr_ast, nullable=False)
        else:
            sqlquery = SqlQuery(translator, translator.sqlquery)
            tableref = monad.make_tableref(sqlquery)
            attr = monad.attr
            alias, columns = tableref.make_join(pk_only=attr.reverse)
            expr_list = monad.make_expr_list()
            if not attr.reverse: columns = attr.columns
            from_ast = translator.sqlquery.from_ast
            from_ast[0] = 'LEFT_JOIN'
            from_ast.extend(sqlquery.from_ast[1:])
            conditions = [ [ 'EQ', [ 'COLUMN', alias, column ], expr ]  for column, expr in izip(columns, item.getsql()) ]
            conditions.extend(sqlquery.conditions)
            from_ast[-1][-1] = sqland([ from_ast[-1][-1] ] + conditions)
            expr_ast = sqland([ [ 'IS_NULL', expr ] for expr in expr_list ])
            return BoolExprMonad(expr_ast, nullable=False)
    def getattr(monad, name):
        try: return Monad.getattr(monad, name)
        except AttributeError: pass
        entity = monad.type.item_type
        if not isinstance(entity, EntityMeta): throw(AttributeError)
        attr = entity._adict_.get(name)
        if attr is None: throw(AttributeError)
        return AttrSetMonad(monad, attr)
    def call_select(monad):
        # calling with lambda argument processed in preCallFunc
        return monad
    call_filter = call_select
    def call_exists(monad):
        return monad
    def requires_distinct(monad, joined=False, for_count=False):
        if monad.parent.requires_distinct(joined): return True
        reverse = monad.attr.reverse
        if not reverse: return True
        if reverse.is_collection:
            translator = monad.translator
            if not for_count and not translator.hint_join: return True
            if isinstance(monad.parent, AttrSetMonad): return True
        return False
    def count(monad, distinct=None):
        translator = monad.translator
        distinct = distinct_from_monad(distinct, monad.requires_distinct(joined=translator.hint_join, for_count=True))

        sqlquery = monad._subselect()
        expr_list = sqlquery.expr_list
        from_ast = sqlquery.from_ast
        inner_conditions = sqlquery.conditions
        outer_conditions = sqlquery.outer_conditions

        sql_ast = make_aggr = None
        extra_grouping = False
        if not distinct and monad.tableref.name_path != translator.optimize:
            make_aggr = lambda expr_list: [ 'COUNT', None ]
        elif len(expr_list) == 1:
            make_aggr = lambda expr_list: [ 'COUNT', True ] + expr_list
        elif translator.dialect == 'Oracle':
            if monad.tableref.name_path == translator.optimize:
                alias, pk_columns = monad.tableref.make_join(pk_only=True)
                make_aggr = lambda expr_list: [ 'COUNT', distinct, [ 'COLUMN', alias, 'ROWID' ] ]
            else:
                extra_grouping = True
                if translator.hint_join: make_aggr = lambda expr_list: [ 'COUNT', None ]
                else: make_aggr = lambda expr_list: [ 'COUNT', None, [ 'COUNT', None ] ]
        elif translator.dialect == 'PostgreSQL':
            row = [ 'ROW' ] + expr_list
            cond = [ 'IS_NULL', row ]
            if translator.database.provider_name == 'cockroach':
                cond = [ 'OR' ] + [ [ 'IS_NULL', expr ] for expr in expr_list ]
            expr = [ 'CASE', None, [ [ cond, [ 'VALUE', None ] ] ], row ]
            make_aggr = lambda expr_list: [ 'COUNT', True, expr ]
        elif translator.row_value_syntax:
            make_aggr = lambda expr_list: [ 'COUNT', True ] + expr_list
        elif translator.dialect == 'SQLite':
            if not distinct:
                alias, pk_columns = monad.tableref.make_join(pk_only=True)
                make_aggr = lambda expr_list: [ 'COUNT', None, [ 'COLUMN', alias, 'ROWID' ] ]
            elif translator.hint_join:  # Same join as in Oracle
                extra_grouping = True
                make_aggr = lambda expr_list: [ 'COUNT', None ]
            elif translator.sqlite_version < (3, 6, 21):
                alias, pk_columns = monad.tableref.make_join(pk_only=False)
                make_aggr = lambda expr_list: [ 'COUNT', True, [ 'COLUMN', alias, 'ROWID' ] ]
            else:
                sql_ast = [ 'SELECT', [ 'AGGREGATES', [ 'COUNT', None ] ],
                          [ 'FROM', [ 't', 'SELECT', [
                              [ 'DISTINCT' ] + expr_list, from_ast,
                              [ 'WHERE' ] + outer_conditions + inner_conditions ] ] ] ]
        else: throw(NotImplementedError)  # pragma: no cover
        if sql_ast: optimized = False
        elif translator.hint_join:
            sql_ast, optimized = monad._joined_subselect(make_aggr, extra_grouping, coalesce_to_zero=True)
        else:
            sql_ast, optimized = monad._aggregated_scalar_subselect(make_aggr, extra_grouping)
        translator.aggregated_subquery_paths.add(monad.tableref.name_path)
        result = ExprMonad.new(int, sql_ast, nullable=False)
        if optimized: result.aggregated = True
        else: result.nogroup = True
        return result
    len = count
    def aggregate(monad, func_name, distinct=None, sep=None):
        distinct = distinct_from_monad(distinct, default=monad.forced_distinct and func_name in ('SUM', 'AVG'))
        translator = monad.translator
        item_type = monad.type.item_type

        if func_name in ('SUM', 'AVG'):
            if item_type not in numeric_types: throw(TypeError,
                "Function %s() expects query or items of numeric type, got %r in {EXPR}"
                % (func_name.lower(), type2str(item_type)))
        elif func_name in ('MIN', 'MAX'):
            if item_type not in comparable_types: throw(TypeError,
                "Function %s() expects query or items of comparable type, got %r in {EXPR}"
                % (func_name.lower(), type2str(item_type)))
        elif func_name == 'GROUP_CONCAT':
            if isinstance(item_type, EntityMeta) and item_type._pk_is_composite_:
                throw(TypeError, "`group_concat` cannot be used with entity with composite primary key")
        else: assert False  # pragma: no cover

        def make_aggr(expr_list):
            result = [ func_name, distinct ] + expr_list
            if sep is not None:
                assert func_name == 'GROUP_CONCAT'
                result.append(['VALUE', sep])
            return result

        # make_aggr = lambda expr_list: [ func_name, distinct ] + expr_list

        if translator.hint_join:
            sql_ast, optimized = monad._joined_subselect(make_aggr, coalesce_to_zero=(func_name=='SUM'))
        else:
            sql_ast, optimized = monad._aggregated_scalar_subselect(make_aggr)

        if func_name == 'AVG':
            result_type = float
        elif func_name == 'GROUP_CONCAT':
            result_type = unicode
        else:
            result_type = item_type
        translator.aggregated_subquery_paths.add(monad.tableref.name_path)
        result = ExprMonad.new(result_type, sql_ast, nullable=func_name != 'SUM')
        if optimized: result.aggregated = True
        else: result.nogroup = True
        return result
    def nonzero(monad):
        sqlquery = monad._subselect()
        sql_ast = [ 'EXISTS', sqlquery.from_ast,
                    [ 'WHERE' ] + sqlquery.outer_conditions + sqlquery.conditions ]
        return BoolExprMonad(sql_ast, nullable=False)
    def negate(monad):
        sqlquery = monad._subselect()
        sql_ast = [ 'NOT_EXISTS', sqlquery.from_ast,
                    [ 'WHERE' ] + sqlquery.outer_conditions + sqlquery.conditions ]
        return BoolExprMonad(sql_ast, nullable=False)
    call_is_empty = negate
    def make_tableref(monad, sqlquery):
        parent = monad.parent
        attr = monad.attr
        if isinstance(parent, ObjectMixin): parent_tableref = parent.tableref
        elif isinstance(parent, AttrSetMonad): parent_tableref = parent.make_tableref(sqlquery)
        else: assert False  # pragma: no cover
        if attr.reverse:
            name_path = parent_tableref.name_path + '-' + attr.name
            monad.tableref = sqlquery.get_tableref(name_path) \
                             or sqlquery.add_tableref(name_path, parent_tableref, attr)
        else: monad.tableref = parent_tableref
        monad.tableref.can_affect_distinct = True
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
        sqlquery = monad._subselect()
        optimized = False
        if translator.optimize == monad.tableref.name_path:
            sql_ast = make_aggr(sqlquery.expr_list)
            optimized = True
            if not translator.from_optimized:
                from_ast = monad.sqlquery.from_ast[1:]
                assert sqlquery.outer_conditions
                from_ast[0] = from_ast[0] + [ sqland(sqlquery.outer_conditions) ]
                translator.sqlquery.from_ast.extend(from_ast)
                translator.from_optimized = True
        else: sql_ast = [ 'SELECT', [ 'AGGREGATES', make_aggr(sqlquery.expr_list) ],
                          sqlquery.from_ast,
                          [ 'WHERE' ] + sqlquery.outer_conditions + sqlquery.conditions ]
        if extra_grouping:  # This is for Oracle only, with COUNT(COUNT(*))
            sql_ast.append([ 'GROUP_BY' ] + sqlquery.expr_list)
        return sql_ast, optimized
    def _joined_subselect(monad, make_aggr, extra_grouping=False, coalesce_to_zero=False):
        translator = monad.translator
        sqlquery = monad._subselect()
        expr_list = sqlquery.expr_list
        from_ast = sqlquery.from_ast
        inner_conditions = sqlquery.conditions
        outer_conditions = sqlquery.outer_conditions

        groupby_columns = [ inner_column[:] for cond, outer_column, inner_column in outer_conditions ]
        assert len({alias for _, alias, column in groupby_columns}) == 1

        if extra_grouping:
            inner_alias = translator.sqlquery.make_alias('t')
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
                    new_name = 'expr-%d' % next(translator.sqlquery.expr_counter)
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

        subselect_columns = [ 'ALL' ]
        for column_ast in groupby_columns:
            assert column_ast[0] == 'COLUMN'
            subselect_columns.append([ 'AS', column_ast, column_ast[2] ])
        expr_name = 'expr-%d' % next(translator.sqlquery.expr_counter)
        subselect_columns.append([ 'AS', make_aggr(expr_list), expr_name ])
        subquery_ast = [ subselect_columns, from_ast ]
        if inner_conditions and not extra_grouping:
            subquery_ast.append([ 'WHERE' ] + inner_conditions)
        subquery_ast.append([ 'GROUP_BY' ] + groupby_columns)

        alias = translator.sqlquery.make_alias('t')
        for cond in outer_conditions: cond[2][1] = alias
        translator.sqlquery.from_ast.append([ alias, 'SELECT', subquery_ast, sqland(outer_conditions) ])
        expr_ast = [ 'COLUMN', alias, expr_name ]
        if coalesce_to_zero: expr_ast = [ 'COALESCE', expr_ast, [ 'VALUE', 0 ] ]
        return expr_ast, False
    def _subselect(monad, sqlquery=None, extract_outer_conditions=True):
        if monad.sqlquery is not None: return monad.sqlquery
        attr = monad.attr
        translator = monad.translator
        if sqlquery is None:
            sqlquery = SqlQuery(translator, translator.sqlquery)
        monad.make_tableref(sqlquery)
        sqlquery.expr_list = monad.make_expr_list()
        if not attr.reverse and not attr.is_required:
            sqlquery.conditions.extend([ 'IS_NOT_NULL', expr ] for expr in sqlquery.expr_list)
        if sqlquery is not translator.sqlquery and extract_outer_conditions:
            outer_cond = sqlquery.from_ast[1].pop()
            if outer_cond[0] == 'AND': sqlquery.outer_conditions = outer_cond[1:]
            else: sqlquery.outer_conditions = [ outer_cond ]
        monad.sqlquery = sqlquery
        return sqlquery
    def getsql(monad, sqlquery=None):
        if sqlquery is None: sqlquery = monad.translator.sqlquery
        monad.make_tableref(sqlquery)
        return monad.make_expr_list()
    __add__ = make_attrset_binop('+', 'ADD')
    __sub__ = make_attrset_binop('-', 'SUB')
    __mul__ = make_attrset_binop('*', 'MUL')
    __truediv__ = make_attrset_binop('/', 'DIV')
    __floordiv__ = make_attrset_binop('//', 'FLOORDIV')

def make_numericset_binop(op, sqlop):
    def numericset_binop(monad, monad2):
        return NumericSetExprMonad(op, sqlop, monad, monad2)
    return numericset_binop

class NumericSetExprMonad(SetMixin, Monad):
    def __init__(monad, op, sqlop, left, right):
        result_type, left, right = coerce_monads(left, right)
        assert type(result_type) is SetType
        if result_type.item_type not in numeric_types:
            throw(TypeError, _binop_errmsg % (type2str(left.type), type2str(right.type), op))
        Monad.__init__(monad, result_type)
        monad.op = op
        monad.sqlop = sqlop
        monad.left = left
        monad.right = right
    def aggregate(monad, func_name, distinct=None, sep=None):
        distinct = distinct_from_monad(distinct, default=monad.forced_distinct and func_name in ('SUM', 'AVG'))
        translator = monad.translator
        sqlquery = SqlQuery(translator, translator.sqlquery)
        expr = monad.getsql(sqlquery)[0]
        translator.aggregated_subquery_paths.add(monad.tableref.name_path)
        outer_cond = sqlquery.from_ast[1].pop()
        if outer_cond[0] == 'AND': sqlquery.outer_conditions = outer_cond[1:]
        else: sqlquery.outer_conditions = [ outer_cond ]
        if func_name == 'AVG':
            result_type = float
        elif func_name == 'GROUP_CONCAT':
            result_type = unicode
        else:
            result_type = monad.type.item_type
        aggr_ast = [ func_name, distinct, expr ]
        if func_name == 'GROUP_CONCAT':
            if sep is not None:
                aggr_ast.append(['VALUE', sep])
        if translator.optimize != monad.tableref.name_path:
            sql_ast = [ 'SELECT', [ 'AGGREGATES', aggr_ast ],
                        sqlquery.from_ast,
                        [ 'WHERE' ] + sqlquery.outer_conditions + sqlquery.conditions ]
            result = ExprMonad.new(result_type, sql_ast, nullable=func_name != 'SUM')
            result.nogroup = True
        else:
            if not translator.from_optimized:
                from_ast = sqlquery.from_ast[1:]
                assert sqlquery.outer_conditions
                from_ast[0] = from_ast[0] + [ sqland(sqlquery.outer_conditions) ]
                translator.sqlquery.from_ast.extend(from_ast)
                translator.from_optimized = True
            sql_ast = aggr_ast
            result = ExprMonad.new(result_type, sql_ast, nullable=func_name != 'SUM')
            result.aggregated = True
        return result
    def getsql(monad, sqlquery=None):
        if sqlquery is None: sqlquery = monad.translator.sqlquery
        left, right = monad.left, monad.right
        left_expr = left.getsql(sqlquery)[0]
        right_expr = right.getsql(sqlquery)[0]
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
    __truediv__ = make_numericset_binop('/', 'DIV')
    __floordiv__ = make_numericset_binop('//', 'FLOORDIV')

class QuerySetMonad(SetMixin, Monad):
    nogroup = True
    def __init__(monad, subtranslator):
        item_type = subtranslator.expr_type
        monad_type = SetType(item_type)
        Monad.__init__(monad, monad_type)
        monad.subtranslator = subtranslator
        monad.item_type = item_type
        monad.limit = monad.offset = None
    def to_single_cell_value(monad):
        return ExprMonad.new(monad.item_type, monad.getsql()[0])
    def requires_distinct(monad, joined=False):
        assert False
    def call_limit(monad, limit=None, offset=None):
        if limit is not None and not isinstance(limit, int_types):
            if not isinstance(limit, (NoneMonad, NumericConstMonad)):
                throw(TypeError, '`limit` parameter should be of int type')
            limit = limit.value
        if offset is not None and not isinstance(offset, int_types):
            if not isinstance(offset, (NoneMonad, NumericConstMonad)):
                throw(TypeError, '`offset` parameter should be of int type')
            offset = offset.value
        monad.limit = limit
        monad.offset = offset
        return monad
    def contains(monad, item, not_in=False):
        translator = monad.translator
        check_comparable(item, monad, 'in')
        if isinstance(item, ListMonad):
            item_columns = []
            for subitem in item.items: item_columns.extend(subitem.getsql())
        else: item_columns = item.getsql()

        sub = monad.subtranslator
        if translator.hint_join and len(sub.sqlquery.from_ast[1]) == 3:
            subquery_ast = sub.construct_subquery_ast(monad.limit, monad.offset, distinct=False)
            select_ast, from_ast, where_ast = subquery_ast[1:4]
            sqlquery = translator.sqlquery
            if not not_in:
                translator.distinct = True
                if sqlquery.from_ast[0] == 'FROM':
                    sqlquery.from_ast[0] = 'INNER_JOIN'
            else:
                sqlquery.left_join = True
                sqlquery.from_ast[0] = 'LEFT_JOIN'
            col_names = set()
            new_names = []
            exprs = []

            for i, column_ast in enumerate(select_ast):
                if not i: continue  # 'ALL'
                if column_ast[0] == 'COLUMN':
                    tab_name, col_name = column_ast[1:]
                    if col_name not in col_names:
                        col_names.add(col_name)
                        new_names.append(col_name)
                        select_ast[i] = [ 'AS', column_ast, col_name ]
                        continue
                new_name = 'expr-%d' % next(sqlquery.expr_counter)
                new_names.append(new_name)
                select_ast[i] = [ 'AS', column_ast, new_name ]

            alias = sqlquery.make_alias('t')
            outer_conditions = [ [ 'EQ', item_column, [ 'COLUMN', alias, new_name ] ]
                                    for item_column, new_name in izip(item_columns, new_names) ]
            sqlquery.from_ast.append([ alias, 'SELECT', subquery_ast[1:], sqland(outer_conditions) ])
            if not_in: sql_ast = sqland([ [ 'IS_NULL', [ 'COLUMN', alias, new_name ] ]
                                              for new_name in new_names ])
            else: sql_ast = [ 'EQ', [ 'VALUE', 1 ], [ 'VALUE', 1 ] ]
        else:
            if len(item_columns) == 1:
                subquery_ast = sub.construct_subquery_ast(monad.limit, monad.offset, distinct=False, is_not_null_checks=not_in)
                sql_ast = [ 'NOT_IN' if not_in else 'IN', item_columns[0], subquery_ast ]
            elif translator.row_value_syntax:
                subquery_ast = sub.construct_subquery_ast(monad.limit, monad.offset, distinct=False, is_not_null_checks=not_in)
                sql_ast = [ 'NOT_IN' if not_in else 'IN', [ 'ROW' ] + item_columns, subquery_ast ]
            else:
                ambiguous_names = set()
                if sub.injected:
                    for name in translator.sqlquery.tablerefs:
                        if name in sub.sqlquery.tablerefs:
                            ambiguous_names.add(name)
                subquery_ast = sub.construct_subquery_ast(monad.limit, monad.offset, distinct=False)
                if ambiguous_names:
                    select_ast = subquery_ast[1]
                    expr_aliases = []
                    for i, expr_ast in enumerate(select_ast):
                        if i > 0:
                            if expr_ast[0] == 'AS':
                                expr_ast = expr_ast[1]
                            expr_alias = 'expr-%d' % i
                            expr_aliases.append(expr_alias)
                            expr_ast = [ 'AS', expr_ast, expr_alias ]
                            select_ast[i] = expr_ast

                    new_table_alias = translator.sqlquery.make_alias('t')
                    new_select_ast = [ 'ALL' ]
                    for expr_alias in expr_aliases:
                        new_select_ast.append([ 'COLUMN', new_table_alias, expr_alias ])
                    new_from_ast = [ 'FROM', [ new_table_alias, 'SELECT', subquery_ast[1:] ] ]
                    new_where_ast = [ 'WHERE' ]
                    subquery_ast = [ 'SELECT', new_select_ast, new_from_ast, new_where_ast ]
                select_ast, from_ast, where_ast = subquery_ast[1:4]
                in_conditions = [ [ 'EQ', expr1, expr2 ] for expr1, expr2 in izip(item_columns, select_ast[1:]) ]
                if not ambiguous_names and sub.aggregated:
                    having_ast = find_or_create_having_ast(subquery_ast)
                    having_ast += in_conditions
                else: where_ast += in_conditions
                sql_ast = [ 'NOT_EXISTS' if not_in else 'EXISTS' ] + subquery_ast[2:]
        return BoolExprMonad(sql_ast, nullable=False)
    def nonzero(monad):
        subquery_ast = monad.subtranslator.construct_subquery_ast(distinct=False)
        expr_monads = monad.subtranslator.expr_monads
        if len(expr_monads) > 1:
            throw(NotImplementedError)
        expr_monad = expr_monads[0]
        if not isinstance(expr_monad, ObjectIterMonad):
            sql = expr_monad.nonzero().getsql()
            assert subquery_ast[3][0] == 'WHERE'
            subquery_ast[3].append(sql[0])
        subquery_ast = [ 'EXISTS' ] + subquery_ast[2:]
        return BoolExprMonad(subquery_ast, nullable=False)
    def negate(monad):
        sql = monad.nonzero().sql
        assert sql[0] == 'EXISTS'
        return BoolExprMonad([ 'NOT_EXISTS' ] + sql[1:], nullable=False)
    def count(monad, distinct=None):
        distinct = distinct_from_monad(distinct)
        translator = monad.translator
        sub = monad.subtranslator

        if sub.aggregated: throw(TranslationError, 'Too complex aggregation in {EXPR}')
        subquery_ast = sub.construct_subquery_ast(distinct=False)
        from_ast, where_ast = subquery_ast[2:4]
        sql_ast = None

        expr_type = sub.expr_type
        if isinstance(expr_type, (tuple, EntityMeta)):
            if not sub.distinct and not distinct:
                select_ast = [ 'AGGREGATES', [ 'COUNT', None ] ]
            elif len(sub.expr_columns) == 1:
                select_ast = [ 'AGGREGATES', [ 'COUNT', True if distinct is None else distinct ] + sub.expr_columns ]
            elif translator.dialect == 'Oracle':
                sql_ast = [ 'SELECT', [ 'AGGREGATES', [ 'COUNT', None, [ 'COUNT', None ] ] ],
                            from_ast, where_ast, [ 'GROUP_BY' ] + sub.expr_columns ]
            elif translator.row_value_syntax:
                select_ast = [ 'AGGREGATES', [ 'COUNT', True if distinct is None else distinct ] + sub.expr_columns ]
            elif translator.dialect == 'SQLite':
                if translator.sqlite_version < (3, 6, 21):
                    if sub.aggregated: throw(TranslationError)
                    alias, pk_columns = sub.tableref.make_join(pk_only=False)
                    subquery_ast = sub.construct_subquery_ast(distinct=False)
                    from_ast, where_ast = subquery_ast[2:4]
                    sql_ast = [ 'SELECT',
                        [ 'AGGREGATES', [ 'COUNT', True if distinct is None else distinct, [ 'COLUMN', alias, 'ROWID' ] ] ],
                        from_ast, where_ast ]
                else:
                    alias = translator.sqlquery.make_alias('t')
                    sql_ast = [ 'SELECT', [ 'AGGREGATES', [ 'COUNT', None ] ],
                                [ 'FROM', [ alias, 'SELECT', [ [ 'DISTINCT' if distinct is not False else 'ALL' ]
                                                               + sub.expr_columns, from_ast, where_ast ] ] ] ]
            else: assert False  # pragma: no cover
        elif len(sub.expr_columns) == 1:
            select_ast = [ 'AGGREGATES', [ 'COUNT', True if distinct is None else distinct, sub.expr_columns[0] ] ]
        else: throw(NotImplementedError)  # pragma: no cover

        if sql_ast is None: sql_ast = [ 'SELECT', select_ast, from_ast, where_ast ]
        return ExprMonad.new(int, sql_ast, nullable=False)
    len = count
    def aggregate(monad, func_name, distinct=None, sep=None):
        distinct = distinct_from_monad(distinct, default=monad.forced_distinct and func_name in ('SUM', 'AVG'))
        sub = monad.subtranslator
        if sub.aggregated: throw(TranslationError, 'Too complex aggregation in {EXPR}')
        subquery_ast = sub.construct_subquery_ast(distinct=False)
        from_ast, where_ast = subquery_ast[2:4]
        expr_type = sub.expr_type
        if func_name in ('SUM', 'AVG'):
            if expr_type not in numeric_types: throw(TypeError,
                "Function %s() expects query or items of numeric type, got %r in {EXPR}"
                % (func_name.lower(), type2str(expr_type)))
        elif func_name in ('MIN', 'MAX'):
            if expr_type not in comparable_types: throw(TypeError,
                "Function %s() cannot be applied to type %r in {EXPR}"
                % (func_name.lower(), type2str(expr_type)))
        elif func_name == 'GROUP_CONCAT':
            if isinstance(expr_type, EntityMeta) and expr_type._pk_is_composite_:
                throw(TypeError, "`group_concat` cannot be used with entity with composite primary key")
        else: assert False  # pragma: no cover
        assert len(sub.expr_columns) == 1
        aggr_ast = [ func_name, distinct, sub.expr_columns[0] ]
        if func_name == 'GROUP_CONCAT':
            if sep is not None:
                aggr_ast.append(['VALUE', sep])
        select_ast = [ 'AGGREGATES', aggr_ast ]
        sql_ast = [ 'SELECT', select_ast, from_ast, where_ast ]
        if func_name == 'AVG':
            result_type = float
        elif func_name == 'GROUP_CONCAT':
            result_type = unicode
        else:
            result_type = expr_type
        return ExprMonad.new(result_type, sql_ast, func_name != 'SUM')
    def call_count(monad, distinct=None):
        return monad.count(distinct=distinct)
    def call_sum(monad, distinct=None):
        return monad.aggregate('SUM', distinct)
    def call_min(monad):
        return monad.aggregate('MIN')
    def call_max(monad):
        return monad.aggregate('MAX')
    def call_avg(monad, distinct=None):
        return monad.aggregate('AVG', distinct)
    def call_group_concat(monad, sep=None, distinct=None):
        if sep is not None:
            if not isinstance(sep, basestring):
                throw(TypeError, '`sep` option of `group_concat` should be type of str. Got: %s' % type(sep).__name__)
        return monad.aggregate('GROUP_CONCAT', distinct, sep=sep)
    def getsql(monad):
        return [ monad.subtranslator.construct_subquery_ast(monad.limit, monad.offset) ]

def find_or_create_having_ast(sections):
    groupby_offset = None
    for i, section in enumerate(sections):
        section_name = section[0]
        if section_name == 'GROUP_BY':
            groupby_offset = i
        elif section_name == 'HAVING':
            return section
    having_ast = [ 'HAVING' ]
    sections.insert(groupby_offset + 1, having_ast)
    return having_ast
