from __future__ import absolute_import, print_function, division
from pony.py23compat import PY2, items_list, izip, xrange, basestring, unicode, buffer, with_metaclass

import types, sys, re, itertools
from decimal import Decimal
from datetime import date, time, datetime, timedelta
from random import random
from copy import deepcopy
from functools import update_wrapper
from uuid import UUID

from pony.thirdparty.compiler import ast

from pony import options, utils
from pony.utils import is_ident, throw, reraise, copy_ast, between, concat, coalesce
from pony.orm.asttranslation import ASTTranslator, ast2src, TranslationError
from pony.orm.ormtypes import \
    numeric_types, comparable_types, SetType, FuncType, MethodType, RawSQLType, \
    get_normalized_type_of, normalize_type, coerce_types, are_comparable_types, \
    Json
from pony.orm import core
from pony.orm.core import EntityMeta, Set, JOIN, OptimizationFailed, Attribute, DescWrapper

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

class SQLTranslator(ASTTranslator):
    dialect = None
    row_value_syntax = True
    json_path_wildcard_syntax = False
    json_values_are_comparable = True
    rowid_support = False

    def default_post(translator, node):
        throw(NotImplementedError)  # pragma: no cover

    def dispatch(translator, node):
        if hasattr(node, 'monad'): return  # monad already assigned somehow
        if not getattr(node, 'external', False) or getattr(node, 'constant', False):
            return ASTTranslator.dispatch(translator, node)  # default route
        translator.call(translator.dispatch_external, node)

    def dispatch_external(translator, node):
        varkey = translator.filter_num, node.src
        t = translator.vartypes[varkey]
        tt = type(t)
        if t is NoneType:
            monad = translator.ConstMonad.new(translator, None)
        elif tt is SetType:
            if isinstance(t.item_type, EntityMeta):
                monad = translator.EntityMonad(translator, t.item_type)
            else: throw(NotImplementedError)  # pragma: no cover
        elif tt is FuncType:
            func = t.func
            func_monad_class = translator.registered_functions.get(func, translator.ErrorSpecialFuncMonad)
            monad = func_monad_class(translator, func)
        elif tt is MethodType:
            obj, func = t.obj, t.func
            if not isinstance(obj, EntityMeta): throw(NotImplementedError)
            entity_monad = translator.EntityMonad(translator, obj)
            if obj.__class__.__dict__.get(func.__name__) is not func: throw(NotImplementedError)
            monad = translator.MethodMonad(entity_monad, func.__name__)
        elif isinstance(node, ast.Name) and node.name in ('True', 'False'):
            value = True if node.name == 'True' else False
            monad = translator.ConstMonad.new(translator, value)
        elif tt is tuple:
            params = []
            for i, item_type in enumerate(t):
                if item_type is NoneType:
                    throw(TypeError, 'Expression `%s` should not contain None values' % node.src)
                param = translator.ParamMonad.new(translator, item_type, (varkey, i, None))
                params.append(param)
            monad = translator.ListMonad(translator, params)
        elif isinstance(t, RawSQLType):
            monad = translator.RawSQLMonad(translator, t, varkey)
        else:
            monad = translator.ParamMonad.new(translator, t, (varkey, None, None))
        node.monad = monad
        monad.node = node
        monad.aggregated = monad.nogroup = False

    def call(translator, method, node):
        try: monad = method(node)
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

    def __init__(translator, tree, extractors, vartypes, parent_translator=None, left_join=False, optimize=None):
        assert isinstance(tree, ast.GenExprInner), tree
        ASTTranslator.__init__(translator, tree)
        translator.database = None
        translator.lambda_argnames = None
        translator.filter_num = parent_translator.filter_num if parent_translator is not None else 0
        translator.extractors = extractors
        translator.vartypes = vartypes.copy()
        translator.parent = parent_translator
        translator.left_join = left_join
        translator.optimize = optimize
        translator.from_optimized = False
        translator.optimization_failed = False
        if not parent_translator: subquery = Subquery(left_join=left_join)
        else: subquery = Subquery(parent_translator.subquery, left_join=left_join)
        translator.subquery = subquery
        tablerefs = subquery.tablerefs
        translator.distinct = False
        translator.conditions = subquery.conditions
        translator.having_conditions = []
        translator.order = []
        translator.inside_order_by = False
        translator.aggregated = False if not optimize else True
        translator.hint_join = False
        translator.query_result_is_cacheable = True
        translator.aggregated_subquery_paths = set()
        for i, qual in enumerate(tree.quals):
            assign = qual.assign
            if not isinstance(assign, ast.AssName): throw(NotImplementedError, ast2src(assign))
            if assign.flags != 'OP_ASSIGN': throw(TypeError, ast2src(assign))

            name = assign.name
            if name in tablerefs: throw(TranslationError, 'Duplicate name: %r' % name)
            if name.startswith('__'): throw(TranslationError, 'Illegal name: %r' % name)

            node = qual.iter
            monad = getattr(node, 'monad', None)
            src = getattr(node, 'src', None)
            if monad:  # Lambda was encountered inside generator
                assert isinstance(monad, EntityMonad)
                entity = monad.type.item_type
                tablerefs[name] = TableRef(subquery, name, entity)
            elif src:
                iterable = translator.vartypes[translator.filter_num, src]
                if not isinstance(iterable, SetType): throw(TranslationError,
                    'Inside declarative query, iterator must be entity. '
                    'Got: for %s in %s' % (name, ast2src(qual.iter)))
                entity = iterable.item_type
                if not isinstance(entity, EntityMeta):
                    throw(TranslationError, 'for %s in %s' % (name, ast2src(qual.iter)))
                if i > 0:
                    if translator.left_join: throw(TranslationError,
                        'Collection expected inside left join query. '
                        'Got: for %s in %s' % (name, ast2src(qual.iter)))
                    translator.distinct = True
                tableref = TableRef(subquery, name, entity)
                tablerefs[name] = tableref
                tableref.make_join()
                node.monad = translator.ObjectIterMonad(translator, tableref, entity)
            else:
                attr_names = []
                while isinstance(node, ast.Getattr):
                    attr_names.append(node.attrname)
                    node = node.expr
                if not isinstance(node, ast.Name) or not attr_names:
                    throw(TranslationError, 'for %s in %s' % (name, ast2src(qual.iter)))
                node_name = node.name
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
                    tableref = JoinedTableRef(subquery, name_path, parent_tableref, attr)
                    if can_affect_distinct is not None:
                        tableref.can_affect_distinct = can_affect_distinct
                    tablerefs[name_path] = tableref
                    parent_tableref = tableref
                    parent_entity = entity

            database = entity._database_
            assert database.schema is not None
            if translator.database is None:
                translator.database = database
                translator.dialect = database.provider.dialect
            elif translator.database is not database: throw(TranslationError,
                'All entities in a query must belong to the same database')

            for if_ in qual.ifs:
                assert isinstance(if_, ast.GenExprIf)
                translator.dispatch(if_)
                if isinstance(if_.monad, translator.AndMonad): cond_monads = if_.monad.operands
                else: cond_monads = [ if_.monad ]
                for m in cond_monads:
                    if not m.aggregated: translator.conditions.extend(m.getsql())
                    else: translator.having_conditions.extend(m.getsql())

        translator.dispatch(tree.expr)
        assert not translator.hint_join
        monad = tree.expr.monad
        if isinstance(monad, translator.ParamMonad): throw(TranslationError,
            "External parameter '%s' cannot be used as query result" % ast2src(tree.expr))
        translator.expr_monads = monad.items if isinstance(monad, translator.ListMonad) else [ monad ]
        translator.groupby_monads = None
        expr_type = monad.type
        if isinstance(expr_type, SetType): expr_type = expr_type.item_type
        if isinstance(expr_type, EntityMeta):
            monad.orderby_columns = list(xrange(1, len(expr_type._pk_columns_)+1))
            if monad.aggregated: throw(TranslationError)
            if translator.aggregated: translator.groupby_monads = [ monad ]
            else: translator.distinct |= monad.requires_distinct()
            if isinstance(monad, translator.ObjectMixin):
                entity = monad.type
                tableref = monad.tableref
            elif isinstance(monad, translator.AttrSetMonad):
                entity = monad.type.item_type
                tableref = monad.make_tableref(translator.subquery)
            else: assert False  # pragma: no cover
            translator.tableref = tableref
            pk_only = parent_translator is not None or translator.aggregated
            alias, pk_columns = tableref.make_join(pk_only=pk_only)
            translator.alias = alias
            translator.expr_type = entity
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
                for tr in tablerefs.values():
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
    def shallow_copy_of_subquery_ast(translator, move_outer_conditions=True, is_not_null_checks=False):
        subquery_ast, attr_offsets = translator.construct_sql_ast(distinct=False, is_not_null_checks=is_not_null_checks)
        assert attr_offsets is None
        assert len(subquery_ast) >= 3 and subquery_ast[0] == 'SELECT'

        select_ast = subquery_ast[1][:]
        assert select_ast[0] == 'ALL'

        from_ast = subquery_ast[2][:]
        assert from_ast[0] == 'FROM'

        if len(subquery_ast) == 3:
            where_ast = [ 'WHERE' ]
            other_ast = []
        elif subquery_ast[3][0] != 'WHERE':
            where_ast = [ 'WHERE' ]
            other_ast = subquery_ast[3:]
        else:
            where_ast = subquery_ast[3][:]
            other_ast = subquery_ast[4:]

        if move_outer_conditions and len(from_ast[1]) == 4:
            outer_conditions = from_ast[1][-1]
            from_ast[1] = from_ast[1][:-1]
            if outer_conditions[0] == 'AND': where_ast[1:1] = outer_conditions[1:]
            else: where_ast.insert(1, outer_conditions)

        return [ 'SELECT', select_ast, from_ast, where_ast ] + other_ast
    def can_be_optimized(translator):
        if translator.groupby_monads: return False
        if len(translator.aggregated_subquery_paths) != 1: return False
        return next(iter(translator.aggregated_subquery_paths))
    def construct_sql_ast(translator, range=None, distinct=None, aggr_func_name=None, for_update=False, nowait=False,
                          attrs_to_prefetch=(), is_not_null_checks=False):
        attr_offsets = None
        if distinct is None: distinct = translator.distinct
        ast_transformer = lambda ast: ast
        if for_update:
            sql_ast = [ 'SELECT_FOR_UPDATE', nowait ]
            translator.query_result_is_cacheable = False
        else: sql_ast = [ 'SELECT' ]

        groupby_monads = translator.groupby_monads
        if distinct and translator.aggregated and not groupby_monads:
            distinct = False
            groupby_monads = translator.expr_monads

        select_ast = [ 'DISTINCT' if distinct else 'ALL' ] + translator.expr_columns
        if aggr_func_name:
            expr_type = translator.expr_type
            if isinstance(expr_type, EntityMeta):
                if aggr_func_name is not 'COUNT': throw(TypeError,
                    'Attribute should be specified for %r aggregate function' % aggr_func_name.lower())
            elif isinstance(expr_type, tuple):
                if aggr_func_name is not 'COUNT': throw(TypeError,
                    'Single attribute should be specified for %r aggregate function' % aggr_func_name.lower())
            else:
                if aggr_func_name in ('SUM', 'AVG') and expr_type not in numeric_types:
                    throw(TypeError, '%r is valid for numeric attributes only' % aggr_func_name.lower())
                assert len(translator.expr_columns) == 1
            aggr_ast = None
            if groupby_monads or (aggr_func_name == 'COUNT' and distinct
                                             and isinstance(translator.expr_type, EntityMeta)
                                             and len(translator.expr_columns) > 1):
                outer_alias = 't'
                if aggr_func_name == 'COUNT':
                    outer_aggr_ast = [ 'COUNT', 'ALL' ]
                else:
                    assert len(translator.expr_columns) == 1
                    expr_ast = translator.expr_columns[0]
                    if expr_ast[0] == 'COLUMN':
                        outer_alias, column_name = expr_ast[1:]
                        outer_aggr_ast = [ aggr_func_name, [ 'COLUMN', outer_alias, column_name ] ]
                    else:
                        select_ast = [ 'DISTINCT' if distinct else 'ALL' ] + [ [ 'AS', expr_ast, 'expr' ] ]
                        outer_aggr_ast = [ aggr_func_name, [ 'COLUMN', 't', 'expr' ] ]
                def ast_transformer(ast):
                    return [ 'SELECT', [ 'AGGREGATES', outer_aggr_ast ],
                                       [ 'FROM', [ outer_alias, 'SELECT', ast[1:] ] ] ]
            else:
                if aggr_func_name == 'COUNT':
                    if isinstance(expr_type, (tuple, EntityMeta)) and not distinct: aggr_ast = [ 'COUNT', 'ALL' ]
                    else: aggr_ast = [ 'COUNT', 'DISTINCT', translator.expr_columns[0] ]
                else: aggr_ast = [ aggr_func_name, translator.expr_columns[0] ]
            if aggr_ast: select_ast = [ 'AGGREGATES', aggr_ast ]
        elif isinstance(translator.expr_type, EntityMeta) and not translator.parent \
             and not translator.aggregated and not translator.optimize:
            select_ast, attr_offsets = translator.expr_type._construct_select_clause_(
                translator.alias, distinct, translator.tableref.used_attrs, attrs_to_prefetch)
        sql_ast.append(select_ast)
        sql_ast.append(translator.subquery.from_ast)

        conditions = translator.conditions[:]
        having_conditions = translator.having_conditions[:]
        if is_not_null_checks:
            for monad in translator.expr_monads:
                if isinstance(monad, translator.ObjectIterMonad): pass
                elif isinstance(monad, translator.AttrMonad) and not monad.attr.nullable: pass
                else:
                    notnull_conditions = [ [ 'IS_NOT_NULL', column_ast ] for column_ast in monad.getsql() ]
                    if monad.aggregated: having_conditions.extend(notnull_conditions)
                    else: conditions.extend(notnull_conditions)
        if conditions:
            sql_ast.append([ 'WHERE' ] + conditions)

        if groupby_monads:
            group_by = [ 'GROUP_BY' ]
            for m in groupby_monads: group_by.extend(m.getsql())
            sql_ast.append(group_by)
        else: group_by = None

        if having_conditions:
            if not group_by: throw(TranslationError,
                'In order to use aggregated functions such as SUM(), COUNT(), etc., '
                'query must have grouping columns (i.e. resulting non-aggregated values)')
            sql_ast.append([ 'HAVING' ] + having_conditions)

        if translator.order and not aggr_func_name: sql_ast.append([ 'ORDER_BY' ] + translator.order)

        if range:
            assert not aggr_func_name
            start, stop = range
            limit = stop - start
            offset = start
            assert limit is not None
            limit_section = [ 'LIMIT', [ 'VALUE', limit ]]
            if offset: limit_section.append([ 'VALUE', offset ])
            sql_ast = sql_ast + [ limit_section ]

        sql_ast = ast_transformer(sql_ast)
        return sql_ast, attr_offsets
    def construct_delete_sql_ast(translator):
        entity = translator.expr_type
        expr_monad = translator.tree.expr.monad
        if not isinstance(entity, EntityMeta): throw(TranslationError,
            'Delete query should be applied to a single entity. Got: %s' % ast2src(translator.tree.expr))
        if translator.groupby_monads: throw(TranslationError,
            'Delete query cannot contains GROUP BY section or aggregate functions')
        assert not translator.having_conditions
        tableref = expr_monad.tableref
        from_ast = translator.subquery.from_ast
        assert from_ast[0] == 'FROM'
        if len(from_ast) == 2 and not translator.subquery.used_from_subquery:
            sql_ast = [ 'DELETE', None, from_ast ]
            if translator.conditions:
                sql_ast.append([ 'WHERE' ] + translator.conditions)
        elif translator.dialect == 'MySQL':
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
        if original_names:
            object_monad = translator.tree.quals[0].iter.monad
            assert isinstance(object_monad.type, EntityMeta)
        else:
            object_monad = translator.tree.expr.monad
            if not isinstance(object_monad.type, EntityMeta):
                throw(TypeError, 'Keyword arguments are not allowed when query result is not entity objects')

        monads = []
        none_monad = translator.NoneMonad(translator)
        for attr, id, is_none in filterattrs:
            attr_monad = object_monad.getattr(attr.name)
            if is_none: monads.append(CmpMonad('is', attr_monad, none_monad))
            else:
                param_monad = translator.ParamMonad.new(translator, attr.py_type, (id, None, None))
                monads.append(CmpMonad('==', attr_monad, param_monad))
        for m in monads: translator.conditions.extend(m.getsql())
        return translator
    def apply_lambda(translator, filter_num, order_by, func_ast, argnames, original_names, extractors, vartypes):
        translator = deepcopy(translator)
        func_ast = copy_ast(func_ast)  # func_ast = deepcopy(func_ast)
        translator.filter_num = filter_num
        translator.extractors.update(extractors)
        translator.vartypes.update(vartypes)
        translator.lambda_argnames = list(argnames)
        translator.original_names = original_names
        translator.dispatch(func_ast)
        if isinstance(func_ast, ast.Tuple): nodes = func_ast.nodes
        else: nodes = (func_ast,)
        if order_by:
            translator.inside_order_by = True
            new_order = []
            for node in nodes:
                if isinstance(node.monad, translator.SetMixin):
                    t = node.monad.type.item_type
                    if isinstance(type(t), type): t = t.__name__
                    throw(TranslationError, 'Set of %s (%s) cannot be used for ordering'
                                            % (t, ast2src(node)))
                new_order.extend(node.monad.getsql())
            translator.order[:0] = new_order
            translator.inside_order_by = False
        else:
            for node in nodes:
                monad = node.monad
                if isinstance(monad, translator.AndMonad): cond_monads = monad.operands
                else: cond_monads = [ monad ]
                for m in cond_monads:
                    if not m.aggregated: translator.conditions.extend(m.getsql())
                    else: translator.having_conditions.extend(m.getsql())
        return translator
    def preGenExpr(translator, node):
        inner_tree = node.code
        subtranslator = translator.__class__(inner_tree, translator.extractors, translator.vartypes, translator)
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
        return translator.AndMonad(monads)
    def postConst(translator, node):
        value = node.value
        if type(value) is frozenset:
            value = tuple(sorted(value))
        if type(value) is not tuple:
            return translator.ConstMonad.new(translator, value)
        else:
            return translator.ListMonad(translator, [ translator.ConstMonad.new(translator, item) for item in value ])
    def postEllipsis(translator, node):
        return translator.ConstMonad.new(translator, Ellipsis)
    def postList(translator, node):
        return translator.ListMonad(translator, [ item.monad for item in node.nodes ])
    def postTuple(translator, node):
        return translator.ListMonad(translator, [ item.monad for item in node.nodes ])
    def postName(translator, node):
        name = node.name
        t = translator
        while t is not None:
            argnames = t.lambda_argnames
            if argnames is not None and not t.original_names and name in argnames:
                i = argnames.index(name)
                return t.expr_monads[i]
            t = t.parent
        tableref = translator.subquery.get_tableref(name)
        if tableref is not None:
            return translator.ObjectIterMonad(translator, tableref, tableref.entity)
        else: assert False, name  # pragma: no cover
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
        return translator.AndMonad([ subnode.monad for subnode in node.nodes ])
    def postOr(translator, node):
        return translator.OrMonad([ subnode.monad for subnode in node.nodes ])
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
        if not isinstance(entity_monad, EntityMonad): throw(NotImplementedError)
        entity = entity_monad.type.item_type
        if method_monad.attrname != 'select': throw(TypeError)
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
        subtranslator = translator.__class__(inner_expr, translator.extractors, translator.vartypes, translator)
        return translator.QuerySetMonad(translator, subtranslator)
    def postCallFunc(translator, node):
        args = []
        kwargs = {}
        for arg in node.args:
            if isinstance(arg, ast.Keyword):
                kwargs[arg.name] = arg.expr.monad
            else: args.append(arg.monad)
        func_monad = node.node.monad
        if isinstance(func_monad, ErrorSpecialFuncMonad): throw(TypeError,
            'Function %r cannot be used this way: %s' % (func_monad.func.__name__, ast2src(node)))
        return func_monad(*args, **kwargs)
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
        result = translator.ExprMonad.new(translator, result_type, expr)
        result.aggregated = test_monad.aggregated or then_monad.aggregated or else_monad.aggregated
        return result

def coerce_monads(m1, m2):
    result_type = coerce_types(m1.type, m2.type)
    if result_type in numeric_types and bool in (m1.type, m2.type) and result_type is not bool:
        translator = m1.translator
        if translator.dialect == 'PostgreSQL':
            if m1.type is bool:
                new_m1 = NumericExprMonad(translator, int, [ 'TO_INT', m1.getsql()[0] ])
                new_m1.aggregated = m1.aggregated
                m1 = new_m1
            if m2.type is bool:
                new_m2 = NumericExprMonad(translator, int, [ 'TO_INT', m2.getsql()[0] ])
                new_m2.aggregated = m2.aggregated
                m2 = new_m2
    return result_type, m1, m2

max_alias_length = 30

class Subquery(object):
    def __init__(subquery, parent_subquery=None, left_join=False):
        subquery.parent_subquery = parent_subquery
        subquery.left_join = left_join
        subquery.from_ast = [ 'LEFT_JOIN' if left_join else 'FROM' ]
        subquery.conditions = []
        subquery.tablerefs = {}
        if parent_subquery is None:
            subquery.alias_counters = {}
            subquery.expr_counter = itertools.count(1)
        else:
            subquery.alias_counters = parent_subquery.alias_counters.copy()
            subquery.expr_counter = parent_subquery.expr_counter
        subquery.used_from_subquery = False
    def get_tableref(subquery, name_path, from_subquery=False):
        tableref = subquery.tablerefs.get(name_path)
        if tableref is not None:
            if from_subquery and subquery.parent_subquery is None:
                subquery.used_from_subquery = True
            return tableref
        if subquery.parent_subquery:
            return subquery.parent_subquery.get_tableref(name_path, from_subquery=True)
        return None
    __contains__ = get_tableref
    def add_tableref(subquery, name_path, parent_tableref, attr):
        tablerefs = subquery.tablerefs
        assert name_path not in tablerefs
        tableref = JoinedTableRef(subquery, name_path, parent_tableref, attr)
        tablerefs[name_path] = tableref
        return tableref
    def make_alias(subquery, name):
        name = name[:max_alias_length-3].lower()
        i = subquery.alias_counters.setdefault(name, 0) + 1
        alias = name if i == 1 and name != 't' else '%s-%d' % (name, i)
        subquery.alias_counters[name] = i
        return alias
    def join_table(subquery, parent_alias, alias, table_name, join_cond):
        new_item = [alias, 'TABLE', table_name, join_cond]
        from_ast = subquery.from_ast
        for i in xrange(1, len(from_ast)):
            if from_ast[i][0] == parent_alias:
                for j in xrange(i+1, len(from_ast)):
                    if len(from_ast[j]) < 4:  # item without join condition
                        from_ast.insert(j, new_item)
                        return
        from_ast.append(new_item)

class TableRef(object):
    def __init__(tableref, subquery, name, entity):
        tableref.subquery = subquery
        tableref.alias = subquery.make_alias(name)
        tableref.name_path = tableref.alias
        tableref.entity = entity
        tableref.joined = False
        tableref.can_affect_distinct = True
        tableref.used_attrs = set()
    def make_join(tableref, pk_only=False):
        entity = tableref.entity
        if not tableref.joined:
            subquery = tableref.subquery
            subquery.from_ast.append([ tableref.alias, 'TABLE', entity._table_ ])
            if entity._discriminator_attr_:
                discr_criteria = entity._construct_discriminator_criteria_(tableref.alias)
                assert discr_criteria is not None
                subquery.conditions.append(discr_criteria)
            tableref.joined = True
        return tableref.alias, entity._pk_columns_

class JoinedTableRef(object):
    def __init__(tableref, subquery, name_path, parent_tableref, attr):
        tableref.subquery = subquery
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
        subquery = tableref.subquery
        attr = tableref.attr
        parent_pk_only = attr.pk_offset is not None or attr.is_collection
        parent_alias, left_pk_columns = tableref.parent_tableref.make_join(parent_pk_only)
        left_entity = attr.entity
        pk_columns = entity._pk_columns_
        if not attr.is_collection:
            if not attr.columns:
                reverse = attr.reverse
                assert reverse.columns and not reverse.is_collection
                rentity = reverse.entity
                pk_columns = rentity._pk_columns_
                alias = subquery.make_alias(tableref.var_name or rentity.__name__)
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
                alias = subquery.make_alias(tableref.var_name or entity.__name__)
                join_cond = join_tables(parent_alias, alias, left_columns, pk_columns)
        elif not attr.reverse.is_collection:
            alias = subquery.make_alias(tableref.var_name or entity.__name__)
            join_cond = join_tables(parent_alias, alias, left_pk_columns, attr.reverse.columns)
        else:
            right_m2m_columns = attr.reverse_columns if attr.symmetric else attr.columns
            if not tableref.joined:
                m2m_table = attr.table
                m2m_alias = subquery.make_alias('t')
                reverse_columns = attr.columns if attr.symmetric else attr.reverse.columns
                m2m_join_cond = join_tables(parent_alias, m2m_alias, left_pk_columns, reverse_columns)
                subquery.join_table(parent_alias, m2m_alias, m2m_table, m2m_join_cond)
                if pk_only:
                    tableref.alias = m2m_alias
                    tableref.pk_columns = right_m2m_columns
                    tableref.optimized = True
                    tableref.joined = True
                    return m2m_alias, tableref.pk_columns
            elif tableref.optimized:
                assert not pk_only
                m2m_alias = tableref.alias
            alias = subquery.make_alias(tableref.var_name or entity.__name__)
            join_cond = join_tables(m2m_alias, alias, right_m2m_columns, pk_columns)
        if not pk_only and entity._discriminator_attr_:
            discr_criteria = entity._construct_discriminator_criteria_(alias)
            assert discr_criteria is not None
            join_cond.append(discr_criteria)
        subquery.join_table(parent_alias, alias, entity._table_, join_cond)
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
    def __init__(monad, translator, type):
        monad.node = None
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
                throw(AttributeError, '%r object has no attribute %r: {EXPR}' % (type2str(monad.type), attrname))
            translator = monad.translator
            return translator.MethodMonad(monad, attrname)
        return property_method()
    def len(monad): throw(TypeError)
    def count(monad):
        translator = monad.translator
        if monad.aggregated: throw(TranslationError, 'Aggregated functions cannot be nested. Got: {EXPR}')
        expr = monad.getsql()
        count_kind = 'DISTINCT'
        if monad.type is bool:
            expr = [ 'CASE', None, [ [ expr[0], [ 'VALUE', 1 ] ] ], [ 'VALUE', None ] ]
            count_kind = 'ALL'
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
        result = translator.ExprMonad.new(translator, int, [ 'COUNT', count_kind, expr ])
        result.aggregated = True
        return result
    def aggregate(monad, func_name):
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
        else: assert False  # pragma: no cover
        expr = monad.getsql()
        if len(expr) == 1: expr = expr[0]
        elif translator.row_value_syntax == True: expr = ['ROW'] + expr
        else: throw(NotImplementedError,
                    '%s database provider does not support entities '
                    'with composite primary keys inside aggregate functions. Got: {EXPR} '
                    '(you can suggest us how to write SQL for this query)'
                    % translator.dialect)
        if func_name == 'AVG': result_type = float
        else: result_type = expr_type
        aggr_ast = [ func_name, expr ]
        if getattr(monad, 'forced_distinct', False) and func_name in ('SUM', 'AVG'):
            aggr_ast.append(True)
        result = translator.ExprMonad.new(translator, result_type, aggr_ast)
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
        return NumericExprMonad(monad.translator, int, [ 'TO_INT', monad.getsql()[0] ])
    def to_real(monad):
        return NumericExprMonad(monad.translator, float, [ 'TO_REAL', monad.getsql()[0] ])

class RawSQLMonad(Monad):
    def __init__(monad, translator, rawtype, varkey):
        if rawtype.result_type is None: type = rawtype
        else: type = normalize_type(rawtype.result_type)
        Monad.__init__(monad, translator, type)
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
        return translator.BoolExprMonad(translator, sql)
    def nonzero(monad): return monad
    def getsql(monad, subquery=None):
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
        Monad.__init__(monad, parent.translator, 'METHOD')
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
    def aggregate(monad, func_name): raise_forgot_parentheses(monad)
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
    def __init__(monad, translator, entity):
        Monad.__init__(monad, translator, SetType(entity))
        if translator.database is None:
            translator.database = entity._database_
        elif translator.database is not entity._database_:
            throw(TranslationError, 'All entities in a query must belong to the same database')
    def __getitem__(monad, *args):
        throw(NotImplementedError)

class ListMonad(Monad):
    def __init__(monad, translator, items):
        Monad.__init__(monad, translator, tuple(item.type for item in items))
        monad.items = items
    def contains(monad, x, not_in=False):
        translator = monad.translator
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
        return translator.BoolExprMonad(translator, sql)
    def getsql(monad, subquery=None):
        return [ [ 'ROW' ] + [ item.getsql()[0] for item in monad.items ] ]

class BufferMixin(MonadMixin):
    pass

class UuidMixin(MonadMixin):
    pass

_binop_errmsg = 'Unsupported operand types %r and %r for operation %r in expression: {EXPR}'

def make_numeric_binop(op, sqlop):
    def numeric_binop(monad, monad2):
        translator = monad.translator
        if isinstance(monad2, (translator.AttrSetMonad, translator.NumericSetExprMonad)):
            return translator.NumericSetExprMonad(op, sqlop, monad, monad2)
        if monad2.type == 'METHOD': raise_forgot_parentheses(monad2)
        result_type, monad, monad2 = coerce_monads(monad, monad2)
        if result_type is None:
            throw(TypeError, _binop_errmsg % (type2str(monad.type), type2str(monad2.type), op))
        left_sql = monad.getsql()[0]
        right_sql = monad2.getsql()[0]
        return translator.NumericExprMonad(translator, result_type, [ sqlop, left_sql, right_sql ])
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
        result = translator.CmpMonad('==', monad, translator.ConstMonad.new(translator, 0))
        if isinstance(monad, translator.AttrMonad) and not monad.attr.nullable:
            return result
        sql = [ 'OR', result.getsql()[0], [ 'IS_NULL', monad.getsql()[0] ] ]
        return translator.BoolExprMonad(translator, sql)

def numeric_attr_factory(name):
    def attr_func(monad):
        sql = [ name, monad.getsql()[0] ]
        translator = monad.translator
        return translator.NumericExprMonad(translator, int, sql)
    attr_func.__name__ = name.lower()
    return attr_func

def make_datetime_binop(op, sqlop):
    def datetime_binop(monad, monad2):
        translator = monad.translator
        if monad2.type != timedelta: throw(TypeError,
            _binop_errmsg % (type2str(monad.type), type2str(monad2.type), op))
        expr_monad_cls = translator.DateExprMonad if monad.type is date else translator.DatetimeExprMonad
        delta = monad2.value if isinstance(monad2, TimedeltaConstMonad) else monad2.getsql()[0]
        return expr_monad_cls(translator, monad.type, [ sqlop, monad.getsql()[0], delta ])
    datetime_binop.__name__ = sqlop
    return datetime_binop

class DateMixin(MonadMixin):
    def mixin_init(monad):
        assert monad.type is date
    attr_year = numeric_attr_factory('YEAR')
    attr_month = numeric_attr_factory('MONTH')
    attr_day = numeric_attr_factory('DAY')
    __add__ = make_datetime_binop('+', 'DATE_ADD')
    __sub__ = make_datetime_binop('-', 'DATE_SUB')

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
        translator = monad.translator
        sql = [ 'DATE', monad.getsql()[0] ]
        return translator.ExprMonad.new(translator, date, sql)
    attr_hour = numeric_attr_factory('HOUR')
    attr_minute = numeric_attr_factory('MINUTE')
    attr_second = numeric_attr_factory('SECOND')
    __add__ = make_datetime_binop('+', 'DATETIME_ADD')
    __sub__ = make_datetime_binop('-', 'DATETIME_SUB')

def make_string_binop(op, sqlop):
    def string_binop(monad, monad2):
        translator = monad.translator
        if not are_comparable_types(monad.type, monad2.type, sqlop):
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
    def negate(monad):
        sql = monad.getsql()[0]
        translator = monad.translator
        result = translator.BoolExprMonad(translator, [ 'EQ', [ 'LENGTH', sql ], [ 'VALUE', 0 ]])
        result.aggregated = monad.aggregated
        return result
    def nonzero(monad):
        sql = monad.getsql()[0]
        translator = monad.translator
        result = translator.BoolExprMonad(translator, [ 'GT', [ 'LENGTH', sql ], [ 'VALUE', 0 ]])
        result.aggregated = monad.aggregated
        return result
    def len(monad):
        sql = monad.getsql()[0]
        translator = monad.translator
        return translator.NumericExprMonad(translator, int, [ 'LENGTH', sql ])
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
        if isinstance(item, translator.StringConstMonad):
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
        sql = [ 'NOT_LIKE' if not_like else 'LIKE', monad.getsql()[0], item_sql ]
        if escape: sql.append([ 'VALUE', '!' ])
        return translator.BoolExprMonad(translator, sql)
    def strip(monad, chars, strip_type):
        translator = monad.translator
        if chars is not None and not are_comparable_types(monad.type, chars.type, None):
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

class JsonMixin(object):
    disable_distinct = True  # at least in Oracle we cannot use DISTINCT with JSON column
    disable_ordering = True  # at least in Oracle we cannot use ORDER BY with JSON column

    def mixin_init(monad):
        assert monad.type is Json, monad.type
    def get_path(monad):
        return monad, []
    def __getitem__(monad, key):
        return monad.translator.JsonItemMonad(monad, key)
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
        return translator.BoolExprMonad(translator, sql)
    def __or__(monad, other):
        translator = monad.translator
        if not isinstance(other, translator.JsonMixin):
            raise TypeError('Should be JSON: %s' % ast2src(other.node))
        left_sql = monad.getsql()[0]
        right_sql = other.getsql()[0]
        sql = [ 'JSON_CONCAT', left_sql, right_sql ]
        return translator.JsonExprMonad(translator, Json, sql)
    def len(monad):
        translator = monad.translator
        sql = [ 'JSON_ARRAY_LENGTH', monad.getsql()[0] ]
        return translator.NumericExprMonad(translator, int, sql)
    def cast_from_json(monad, type):
        if type in (Json, NoneType): return monad
        throw(TypeError, 'Cannot compare whole JSON value, you need to select specific sub-item: {EXPR}')
    def nonzero(monad):
        translator = monad.translator
        return translator.BoolExprMonad(translator, [ 'JSON_NONZERO', monad.getsql()[0] ])

class ObjectMixin(MonadMixin):
    def mixin_init(monad):
        assert isinstance(monad.type, EntityMeta)
    def negate(monad):
        translator = monad.translator
        return translator.CmpMonad('is', monad, translator.NoneMonad(translator))
    def nonzero(monad):
        translator = monad.translator
        return translator.CmpMonad('is not', monad, translator.NoneMonad(translator))
    def getattr(monad, name):
        translator = monad.translator
        entity = monad.type
        attr = entity._adict_.get(name) or entity._subclass_adict_.get(name)
        if attr is None: throw(AttributeError,
            'Entity %s does not have attribute %s: {EXPR}' % (entity.__name__, name))
        if hasattr(monad, 'tableref'): monad.tableref.used_attrs.add(attr)
        if not attr.is_collection:
            return translator.AttrMonad.new(monad, attr)
        else:
            return translator.AttrSetMonad(monad, attr)
    def requires_distinct(monad, joined=False):
        return monad.attr.reverse.is_collection or monad.parent.requires_distinct(joined)  # parent ???

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
    def new(parent, attr, *args, **kwargs):
        translator = parent.translator
        type = normalize_type(attr.py_type)
        if type in numeric_types: cls = translator.NumericAttrMonad
        elif type is unicode: cls = translator.StringAttrMonad
        elif type is date: cls = translator.DateAttrMonad
        elif type is time: cls = translator.TimeAttrMonad
        elif type is timedelta: cls = translator.TimedeltaAttrMonad
        elif type is datetime: cls = translator.DatetimeAttrMonad
        elif type is buffer: cls = translator.BufferAttrMonad
        elif type is UUID: cls = translator.UuidAttrMonad
        elif type is Json: cls = translator.JsonAttrMonad
        elif isinstance(type, EntityMeta): cls = translator.ObjectAttrMonad
        else: throw(NotImplementedError, type)  # pragma: no cover
        return cls(parent, attr, *args, **kwargs)
    def __new__(cls, *args):
        if cls is AttrMonad: assert False, 'Abstract class'  # pragma: no cover
        return Monad.__new__(cls)
    def __init__(monad, parent, attr):
        assert monad.__class__ is not AttrMonad
        translator = parent.translator
        attr_type = normalize_type(attr.py_type)
        Monad.__init__(monad, parent.translator, attr_type)
        monad.parent = parent
        monad.attr = attr
    def getsql(monad, subquery=None):
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
            subquery = monad.translator.subquery
            monad.translator.left_join = subquery.left_join = True
            subquery.from_ast[0] = 'LEFT_JOIN'
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
        monad.tableref = translator.subquery.get_tableref(name_path)
        if monad.tableref is None:
            parent_subquery = parent_monad.tableref.subquery
            monad.tableref = parent_subquery.add_tableref(name_path, parent_monad.tableref, attr)

class StringAttrMonad(StringMixin, AttrMonad):
    def negate(monad):
        sql = monad.getsql()[0]
        translator = monad.translator
        result_sql = [ 'EQ', [ 'LENGTH', sql ], [ 'VALUE', 0 ] ]
        if monad.attr.nullable:
            result_sql = [ 'OR', result_sql, [ 'IS_NULL', sql ] ]
        result = translator.BoolExprMonad(translator, result_sql)
        result.aggregated = monad.aggregated
        return result
    def nonzero(monad):
        sql = monad.getsql()[0]
        translator = monad.translator
        result_sql = [ 'GT', [ 'LENGTH', sql ], [ 'VALUE', 0 ] ]
        result = translator.BoolExprMonad(translator,  result_sql)
        result.aggregated = monad.aggregated
        return result

class NumericAttrMonad(NumericMixin, AttrMonad): pass
class DateAttrMonad(DateMixin, AttrMonad): pass
class TimeAttrMonad(TimeMixin, AttrMonad): pass
class TimedeltaAttrMonad(TimedeltaMixin, AttrMonad): pass
class DatetimeAttrMonad(DatetimeMixin, AttrMonad): pass
class BufferAttrMonad(BufferMixin, AttrMonad): pass
class UuidAttrMonad(UuidMixin, AttrMonad): pass
class JsonAttrMonad(JsonMixin, AttrMonad): pass

class ParamMonad(Monad):
    @staticmethod
    def new(translator, type, paramkey):
        type = normalize_type(type)
        if type in numeric_types: cls = translator.NumericParamMonad
        elif type is unicode: cls = translator.StringParamMonad
        elif type is date: cls = translator.DateParamMonad
        elif type is time: cls = translator.TimeParamMonad
        elif type is timedelta: cls = translator.TimedeltaParamMonad
        elif type is datetime: cls = translator.DatetimeParamMonad
        elif type is buffer: cls = translator.BufferParamMonad
        elif type is UUID: cls = translator.UuidParamMonad
        elif type is Json: cls = translator.JsonParamMonad
        elif isinstance(type, EntityMeta): cls = translator.ObjectParamMonad
        else: throw(NotImplementedError, 'Parameter {EXPR} has unsupported type %r' % (type))
        result = cls(translator, type, paramkey)
        result.aggregated = False
        return result
    def __new__(cls, *args):
        if cls is ParamMonad: assert False, 'Abstract class'  # pragma: no cover
        return Monad.__new__(cls)
    def __init__(monad, translator, type, paramkey):
        type = normalize_type(type)
        Monad.__init__(monad, translator, type)
        monad.paramkey = paramkey
        if not isinstance(type, EntityMeta):
            provider = translator.database.provider
            monad.converter = provider.get_converter_by_py_type(type)
        else: monad.converter = None
    def getsql(monad, subquery=None):
        return [ [ 'PARAM', monad.paramkey, monad.converter ] ]

class ObjectParamMonad(ObjectMixin, ParamMonad):
    def __init__(monad, translator, entity, paramkey):
        assert translator.database is entity._database_
        ParamMonad.__init__(monad, translator, entity, paramkey)
        varkey, i, j = paramkey
        assert j is None
        monad.params = tuple((varkey, i, j) for j in xrange(len(entity._pk_converters_)))
    def getsql(monad, subquery=None):
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

class JsonParamMonad(JsonMixin, ParamMonad):
    def getsql(monad, subquery=None):
        return [ [ 'JSON_PARAM', ParamMonad.getsql(monad)[0] ] ]

class ExprMonad(Monad):
    @staticmethod
    def new(translator, type, sql):
        if type in numeric_types: cls = translator.NumericExprMonad
        elif type is unicode: cls = translator.StringExprMonad
        elif type is date: cls = translator.DateExprMonad
        elif type is time: cls = translator.TimeExprMonad
        elif type is timedelta: cls = translator.TimedeltaExprMonad
        elif type is datetime: cls = translator.DatetimeExprMonad
        elif type is Json: cls = translator.JsonExprMonad
        elif isinstance(type, EntityMeta): cls = translator.ObjectExprMonad
        else: throw(NotImplementedError, type)  # pragma: no cover
        return cls(translator, type, sql)
    def __new__(cls, *args):
        if cls is ExprMonad: assert False, 'Abstract class'  # pragma: no cover
        return Monad.__new__(cls)
    def __init__(monad, translator, type, sql):
        Monad.__init__(monad, translator, type)
        monad.sql = sql
    def getsql(monad, subquery=None):
        return [ monad.sql ]

class ObjectExprMonad(ObjectMixin, ExprMonad):
    def getsql(monad, subquery=None):
        return monad.sql

class StringExprMonad(StringMixin, ExprMonad): pass
class NumericExprMonad(NumericMixin, ExprMonad): pass
class DateExprMonad(DateMixin, ExprMonad): pass
class TimeExprMonad(TimeMixin, ExprMonad): pass
class TimedeltaExprMonad(TimedeltaMixin, ExprMonad): pass
class DatetimeExprMonad(DatetimeMixin, ExprMonad): pass
class JsonExprMonad(JsonMixin, ExprMonad): pass

class JsonItemMonad(JsonMixin, Monad):
    def __init__(monad, parent, key):
        assert isinstance(parent, JsonMixin), parent
        translator = parent.translator
        Monad.__init__(monad, translator, Json)
        monad.parent = parent
        if isinstance(key, slice):
            if key != slice(None, None, None): throw(NotImplementedError)
            monad.key_ast = [ 'VALUE', key ]
        elif isinstance(key, (ParamMonad, StringConstMonad, NumericConstMonad, EllipsisMonad)):
            monad.key_ast = key.getsql()[0]
        else: throw(TypeError, 'Invalid JSON path item: %s' % ast2src(key.node))
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
    def cast_from_json(monad, type):
        translator = monad.translator
        if issubclass(type, Json):
            if not translator.json_values_are_comparable: throw(TranslationError,
                '%s does not support comparison of json structures: {EXPR}' % translator.dialect)
            return monad
        base_monad, path = monad.get_path()
        sql = [ 'JSON_VALUE', base_monad.getsql()[0], path, type ]
        return translator.ExprMonad.new(translator, Json if type is NoneType else type, sql)
    def getsql(monad):
        base_monad, path = monad.get_path()
        base_sql = base_monad.getsql()[0]
        translator = monad.translator
        if translator.inside_order_by and translator.dialect == 'SQLite':
            return [ [ 'JSON_VALUE', base_sql, path, None ] ]
        return [ [ 'JSON_QUERY', base_sql, path ] ]

class ConstMonad(Monad):
    @staticmethod
    def new(translator, value):
        value_type = get_normalized_type_of(value)
        if value_type in numeric_types: cls = translator.NumericConstMonad
        elif value_type is unicode: cls = translator.StringConstMonad
        elif value_type is date: cls = translator.DateConstMonad
        elif value_type is time: cls = translator.TimeConstMonad
        elif value_type is timedelta: cls = translator.TimedeltaConstMonad
        elif value_type is datetime: cls = translator.DatetimeConstMonad
        elif value_type is NoneType: cls = translator.NoneMonad
        elif value_type is buffer: cls = translator.BufferConstMonad
        elif value_type is Json: cls = translator.JsonConstMonad
        elif issubclass(value_type, type(Ellipsis)): cls = translator.EllipsisMonad
        else: throw(NotImplementedError, value_type)  # pragma: no cover
        result = cls(translator, value)
        result.aggregated = False
        return result
    def __new__(cls, *args):
        if cls is ConstMonad: assert False, 'Abstract class'  # pragma: no cover
        return Monad.__new__(cls)
    def __init__(monad, translator, value):
        value_type = get_normalized_type_of(value)
        Monad.__init__(monad, translator, value_type)
        monad.value = value
    def getsql(monad, subquery=None):
        return [ [ 'VALUE', monad.value ] ]

class NoneMonad(ConstMonad):
    type = NoneType
    def __init__(monad, translator, value=None):
        assert value is None
        ConstMonad.__init__(monad, translator, value)

class EllipsisMonad(ConstMonad):
    pass

class StringConstMonad(StringMixin, ConstMonad):
    def len(monad):
        return monad.translator.ConstMonad.new(monad.translator, len(monad.value))

class JsonConstMonad(JsonMixin, ConstMonad): pass
class BufferConstMonad(BufferMixin, ConstMonad): pass
class NumericConstMonad(NumericMixin, ConstMonad): pass
class DateConstMonad(DateMixin, ConstMonad): pass
class TimeConstMonad(TimeMixin, ConstMonad): pass
class TimedeltaConstMonad(TimedeltaMixin, ConstMonad): pass
class DatetimeConstMonad(DatetimeMixin, ConstMonad): pass

class BoolMonad(Monad):
    def __init__(monad, translator):
        monad.translator = translator
        monad.type = bool

sql_negation = { 'IN' : 'NOT_IN', 'EXISTS' : 'NOT_EXISTS', 'LIKE' : 'NOT_LIKE', 'BETWEEN' : 'NOT_BETWEEN', 'IS_NULL' : 'IS_NOT_NULL' }
sql_negation.update((value, key) for key, value in items_list(sql_negation))

class BoolExprMonad(BoolMonad):
    def __init__(monad, translator, sql):
        monad.translator = translator
        monad.type = bool
        monad.sql = sql
    def getsql(monad, subquery=None):
        return [ monad.sql ]
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
cmp_negate.update((b, a) for a, b in items_list(cmp_negate))

class CmpMonad(BoolMonad):
    EQ = 'EQ'
    NE = 'NE'
    def __init__(monad, op, left, right):
        translator = left.translator
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
        result_type, left, right = coerce_monads(left, right)
        BoolMonad.__init__(monad, translator)
        monad.op = op
        monad.aggregated = getattr(left, 'aggregated', False) or getattr(right, 'aggregated', False)

        if isinstance(left, JsonMixin):
            left = left.cast_from_json(right.type)
        if isinstance(right, JsonMixin):
            right = right.cast_from_json(left.type)

        monad.left = left
        monad.right = right
    def negate(monad):
        return monad.translator.CmpMonad(cmp_negate[monad.op], monad.left, monad.right)
    def getsql(monad, subquery=None):
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
            for i in xrange(1, size):
                clauses.append(sqland([ [ monad.EQ, left_sql[j], right_sql[j] ] for j in xrange(1, i) ]
                                + [ [ cmp_ops[op[0] if i < size - 1 else op], left_sql[i], right_sql[i] ] ]))
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
        BoolMonad.__init__(monad, operand.translator)
        monad.operand = operand
    def negate(monad):
        return monad.operand
    def getsql(monad, subquery=None):
        return [ [ 'NOT', monad.operand.getsql()[0] ] ]

class ErrorSpecialFuncMonad(Monad):
    def __init__(monad, translator, func):
        Monad.__init__(monad, translator, func)
        monad.func = func

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
        translator = monad.translator
        for arg in args:
            assert isinstance(arg, translator.Monad)
        for value in kwargs.values():
            assert isinstance(value, translator.Monad)
        try: return monad.call(*args, **kwargs)
        except TypeError as exc:
            reraise_improved_typeerror(exc, 'call', monad.type.__name__)

class FuncBufferMonad(FuncMonad):
    func = buffer
    def call(monad, source, encoding=None, errors=None):
        translator = monad.translator
        if not isinstance(source, translator.StringConstMonad): throw(TypeError)
        source = source.value
        if encoding is not None:
            if not isinstance(encoding, translator.StringConstMonad): throw(TypeError)
            encoding = encoding.value
        if errors is not None:
            if not isinstance(errors, translator.StringConstMonad): throw(TypeError)
            errors = errors.value
        if PY2:
            if encoding and errors: source = source.encode(encoding, errors)
            elif encoding: source = source.encode(encoding)
            return translator.ConstMonad.new(translator, buffer(source))
        else:
            if encoding and errors: value = buffer(source, encoding, errors)
            elif encoding: value = buffer(source, encoding)
            else: value = buffer(source)
            return translator.ConstMonad.new(translator, value)

class FuncBoolMonad(FuncMonad):
    func = bool
    def call(monad, x):
        return x.nonzero()

class FuncIntMonad(FuncMonad):
    func = int
    def call(monad, x):
        return x.to_int()

class FuncFloatMonad(FuncMonad):
    func = float
    def call(monad, x):
        return x.to_real()

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
        for arg, name in izip((year, month, day), ('year', 'month', 'day')):
            if not isinstance(arg, translator.NumericMixin) or arg.type is not int: throw(TypeError,
                "'%s' argument of date(year, month, day) function must be of 'int' type. "
                "Got: %r" % (name, type2str(arg.type)))
            if not isinstance(arg, ConstMonad): throw(NotImplementedError)
        return translator.ConstMonad.new(translator, date(year.value, month.value, day.value))
    def call_today(monad):
        translator = monad.translator
        return translator.DateExprMonad(translator, date, [ 'TODAY' ])

class FuncTimeMonad(FuncMonad):
    func = time
    def call(monad, *args):
        translator = monad.translator
        for arg, name in izip(args, ('hour', 'minute', 'second', 'microsecond')):
            if not isinstance(arg, translator.NumericMixin) or arg.type is not int: throw(TypeError,
                "'%s' argument of time(...) function must be of 'int' type. Got: %r" % (name, type2str(arg.type)))
            if not isinstance(arg, ConstMonad): throw(NotImplementedError)
        return translator.ConstMonad.new(translator, time(*tuple(arg.value for arg in args)))

class FuncTimedeltaMonad(FuncMonad):
    func = timedelta
    def call(monad, days=None, seconds=None, microseconds=None, milliseconds=None, minutes=None, hours=None, weeks=None):
        translator = monad.translator
        args = days, seconds, microseconds, milliseconds, minutes, hours, weeks
        for arg, name in izip(args, ('days', 'seconds', 'microseconds', 'milliseconds', 'minutes', 'hours', 'weeks')):
            if arg is None: continue
            if not isinstance(arg, translator.NumericMixin) or arg.type is not int: throw(TypeError,
                "'%s' argument of timedelta(...) function must be of 'int' type. Got: %r" % (name, type2str(arg.type)))
            if not isinstance(arg, ConstMonad): throw(NotImplementedError)
        value = timedelta(*(arg.value if arg is not None else 0 for arg in args))
        return translator.ConstMonad.new(translator, value)

class FuncDatetimeMonad(FuncDateMonad):
    func = datetime
    def call(monad, year, month, day, hour=None, minute=None, second=None, microsecond=None):
        args = year, month, day, hour, minute, second, microsecond
        translator = monad.translator
        for arg, name in izip(args, ('year', 'month', 'day', 'hour', 'minute', 'second', 'microsecond')):
            if arg is None: continue
            if not isinstance(arg, translator.NumericMixin) or arg.type is not int: throw(TypeError,
                "'%s' argument of datetime(...) function must be of 'int' type. Got: %r" % (name, type2str(arg.type)))
            if not isinstance(arg, ConstMonad): throw(NotImplementedError)
        value = datetime(*(arg.value if arg is not None else 0 for arg in args))
        return translator.ConstMonad.new(translator, value)
    def call_now(monad):
        translator = monad.translator
        return translator.DatetimeExprMonad(translator, datetime, [ 'NOW' ])

class FuncBetweenMonad(FuncMonad):
    func = between
    def call(monad, x, a, b):
        check_comparable(x, a, '<')
        check_comparable(x, b, '<')
        if isinstance(x.type, EntityMeta): throw(TypeError,
            '%s instance cannot be argument of between() function: {EXPR}' % x.type.__name__)
        translator = x.translator
        sql = [ 'BETWEEN', x.getsql()[0], a.getsql()[0], b.getsql()[0] ]
        return translator.BoolExprMonad(translator, sql)

class FuncConcatMonad(FuncMonad):
    func = concat
    def call(monad, *args):
        if len(args) < 2: throw(TranslationError, 'concat() function requires at least two arguments')
        translator = args[0].translator
        result_ast = [ 'CONCAT' ]
        for arg in args:
            t = arg.type
            if isinstance(t, EntityMeta) or type(t) in (tuple, SetType):
                throw(TranslationError, 'Invalid argument of concat() function: %s' % ast2src(arg.node))
            result_ast.extend(arg.getsql())
        return translator.ExprMonad.new(translator, unicode, result_ast)

class FuncLenMonad(FuncMonad):
    func = len
    def call(monad, x):
        return x.len()

class GetattrMonad(FuncMonad):
    func = getattr
    def call(monad, obj_monad, name_monad):
        name = name_monad.node._attrname_value
        return obj_monad.getattr(name)

class FuncCountMonad(FuncMonad):
    func = itertools.count, utils.count, core.count
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
    func = sum, core.sum
    def call(monad, x):
        return x.aggregate('SUM')

class FuncAvgMonad(FuncMonad):
    func = utils.avg, core.avg
    def call(monad, x):
        return x.aggregate('AVG')

class FuncCoalesceMonad(FuncMonad):
    func = coalesce
    def call(monad, *args):
        if len(args) < 2: throw(TranslationError, 'coalesce() function requires at least two arguments')
        translator = args[0].translator
        arg = args[0]
        t = arg.type
        result = [ [ sql ] for sql in arg.getsql() ]
        for arg in args[1:]:
            if arg.type is not t: throw(TypeError, 'All arguments of coalesce() function should have the same type')
            for i, sql in enumerate(arg.getsql()):
                result[i].append(sql)
        sql = [ [ 'COALESCE' ] + coalesce_args for coalesce_args in result ]
        if not isinstance(t, EntityMeta): sql = sql[0]
        return translator.ExprMonad.new(translator, t, sql)

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
                args[i] = NumericExprMonad(translator, int, [ 'TO_INT', arg.getsql() ])
    sql = [ sqlop ] + [ arg.getsql()[0] for arg in args ]
    return translator.ExprMonad.new(translator, t, sql)

class FuncSelectMonad(FuncMonad):
    func = core.select
    def call(monad, queryset):
        translator = monad.translator
        if not isinstance(queryset, translator.QuerySetMonad): throw(TypeError,
            "'select' function expects generator expression, got: {EXPR}")
        return queryset

class FuncExistsMonad(FuncMonad):
    func = core.exists
    def call(monad, arg):
        if not isinstance(arg, monad.translator.SetMixin): throw(TypeError,
            "'exists' function expects generator expression or collection, got: {EXPR}")
        return arg.nonzero()

class FuncDescMonad(FuncMonad):
    func = core.desc
    def call(monad, expr):
        return DescMonad(expr)

class DescMonad(Monad):
    def __init__(monad, expr):
        Monad.__init__(monad, expr.translator, expr.type)
        monad.expr = expr
    def getsql(monad):
        return [ [ 'DESC', item ] for item in monad.expr.getsql() ]

class JoinMonad(Monad):
    def __init__(monad, translator, type):
        Monad.__init__(monad, translator, type)
        monad.hint_join_prev = translator.hint_join
        translator.hint_join = True
    def __call__(monad, x):
        monad.translator.hint_join = monad.hint_join_prev
        return x
registered_functions[JOIN] = JoinMonad

class FuncRandomMonad(FuncMonad):
    func = random
    def __init__(monad, translator, type):
        FuncMonad.__init__(monad, translator, type)
        translator.query_result_is_cacheable = False
    def __call__(monad):
        return NumericExprMonad(monad.translator, float, [ 'RANDOM' ])

class SetMixin(MonadMixin):
    forced_distinct = False
    def call_distinct(monad):
        new_monad = object.__new__(monad.__class__)
        new_monad.__dict__.update(monad.__dict__)
        new_monad.forced_distinct = True
        return new_monad

def make_attrset_binop(op, sqlop):
    def attrset_binop(monad, monad2):
        NumericSetExprMonad = monad.translator.NumericSetExprMonad
        return NumericSetExprMonad(op, sqlop, monad, monad2)
    return attrset_binop

class AttrSetMonad(SetMixin, Monad):
    def __init__(monad, parent, attr):
        translator = parent.translator
        item_type = normalize_type(attr.py_type)
        Monad.__init__(monad, translator, SetType(item_type))
        monad.parent = parent
        monad.attr = attr
        monad.subquery = None
        monad.tableref = None
    def cmp(monad, op, monad2):
        translator = monad.translator
        if type(monad2.type) is SetType \
           and are_comparable_types(monad.type.item_type, monad2.type.item_type): pass
        elif monad.type != monad2.type: check_comparable(monad, monad2)
        throw(NotImplementedError)
    def contains(monad, item, not_in=False):
        translator = monad.translator
        check_comparable(item, monad, 'in')
        if not translator.hint_join:
            sqlop = 'NOT_IN' if not_in else 'IN'
            subquery = monad._subselect()
            expr_list = subquery.expr_list
            from_ast = subquery.from_ast
            conditions = subquery.outer_conditions + subquery.conditions
            if len(expr_list) == 1:
                subquery_ast = [ 'SELECT', [ 'ALL' ] + expr_list, from_ast, [ 'WHERE' ] + conditions ]
                sql_ast = [ sqlop, item.getsql()[0], subquery_ast ]
            elif translator.row_value_syntax:
                subquery_ast = [ 'SELECT', [ 'ALL' ] + expr_list, from_ast, [ 'WHERE' ] + conditions ]
                sql_ast = [ sqlop, [ 'ROW' ] + item.getsql(), subquery_ast ]
            else:
                conditions += [ [ 'EQ', expr1, expr2 ] for expr1, expr2 in izip(item.getsql(), expr_list) ]
                sql_ast = [ 'NOT_EXISTS' if not_in else 'EXISTS', from_ast, [ 'WHERE' ] + conditions ]
            result = translator.BoolExprMonad(translator, sql_ast)
            result.nogroup = True
            return result
        elif not not_in:
            translator.distinct = True
            tableref = monad.make_tableref(translator.subquery)
            expr_list = monad.make_expr_list()
            expr_ast = sqland([ [ 'EQ', expr1, expr2 ]  for expr1, expr2 in izip(expr_list, item.getsql()) ])
            return translator.BoolExprMonad(translator, expr_ast)
        else:
            subquery = Subquery(translator.subquery)
            tableref = monad.make_tableref(subquery)
            attr = monad.attr
            alias, columns = tableref.make_join(pk_only=attr.reverse)
            expr_list = monad.make_expr_list()
            if not attr.reverse: columns = attr.columns
            from_ast = translator.subquery.from_ast
            from_ast[0] = 'LEFT_JOIN'
            from_ast.extend(subquery.from_ast[1:])
            conditions = [ [ 'EQ', [ 'COLUMN', alias, column ], expr ]  for column, expr in izip(columns, item.getsql()) ]
            conditions.extend(subquery.conditions)
            from_ast[-1][-1] = sqland([ from_ast[-1][-1] ] + conditions)
            expr_ast = sqland([ [ 'IS_NULL', expr ] for expr in expr_list ])
            return translator.BoolExprMonad(translator, expr_ast)
    def getattr(monad, name):
        try: return Monad.getattr(monad, name)
        except AttributeError: pass
        entity = monad.type.item_type
        if not isinstance(entity, EntityMeta): throw(AttributeError)
        attr = entity._adict_.get(name)
        if attr is None: throw(AttributeError)
        return monad.translator.AttrSetMonad(monad, attr)
    def requires_distinct(monad, joined=False, for_count=False):
        if monad.parent.requires_distinct(joined): return True
        reverse = monad.attr.reverse
        if not reverse: return True
        if reverse.is_collection:
            translator = monad.translator
            if not for_count and not translator.hint_join: return True
            if isinstance(monad.parent, monad.translator.AttrSetMonad): return True
        return False
    def count(monad):
        translator = monad.translator

        subquery = monad._subselect()
        expr_list = subquery.expr_list
        from_ast = subquery.from_ast
        inner_conditions = subquery.conditions
        outer_conditions = subquery.outer_conditions

        distinct = monad.requires_distinct(joined=translator.hint_join, for_count=True)
        sql_ast = make_aggr = None
        extra_grouping = False
        if not distinct and monad.tableref.name_path != translator.optimize:
            make_aggr = lambda expr_list: [ 'COUNT', 'ALL' ]
        elif len(expr_list) == 1:
            make_aggr = lambda expr_list: [ 'COUNT', 'DISTINCT' ] + expr_list
        elif translator.dialect == 'Oracle':
            if monad.tableref.name_path == translator.optimize:
                alias, pk_columns = monad.tableref.make_join(pk_only=True)
                make_aggr = lambda expr_list: [ 'COUNT', 'DISTINCT' if distinct else 'ALL', [ 'COLUMN', alias, 'ROWID' ] ]
            else:
                extra_grouping = True
                if translator.hint_join: make_aggr = lambda expr_list: [ 'COUNT', 'ALL' ]
                else: make_aggr = lambda expr_list: [ 'COUNT', 'ALL', [ 'COUNT', 'ALL' ] ]
        elif translator.dialect == 'PostgreSQL':
            row = [ 'ROW' ] + expr_list
            expr = [ 'CASE', None, [ [ [ 'IS_NULL', row ], [ 'VALUE', None ] ] ], row ]
            make_aggr = lambda expr_list: [ 'COUNT', 'DISTINCT', expr ]
        elif translator.row_value_syntax:
            make_aggr = lambda expr_list: [ 'COUNT', 'DISTINCT' ] + expr_list
        elif translator.dialect == 'SQLite':
            if not distinct:
                alias, pk_columns = monad.tableref.make_join(pk_only=True)
                make_aggr = lambda expr_list: [ 'COUNT', 'ALL', [ 'COLUMN', alias, 'ROWID' ] ]
            elif translator.hint_join:  # Same join as in Oracle
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
        else: throw(NotImplementedError)  # pragma: no cover
        if sql_ast: optimized = False
        elif translator.hint_join:
            sql_ast, optimized = monad._joined_subselect(make_aggr, extra_grouping, coalesce_to_zero=True)
        else:
            sql_ast, optimized = monad._aggregated_scalar_subselect(make_aggr, extra_grouping)
        translator.aggregated_subquery_paths.add(monad.tableref.name_path)
        result = translator.ExprMonad.new(translator, int, sql_ast)
        if optimized: result.aggregated = True
        else: result.nogroup = True
        return result
    len = count
    def aggregate(monad, func_name):
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
        else: assert False  # pragma: no cover

        if monad.forced_distinct and func_name in ('SUM', 'AVG'):
            make_aggr = lambda expr_list: [ func_name ] + expr_list + [ True ]
        else:
            make_aggr = lambda expr_list: [ func_name ] + expr_list

        if translator.hint_join:
            sql_ast, optimized = monad._joined_subselect(make_aggr, coalesce_to_zero=(func_name=='SUM'))
        else:
            sql_ast, optimized = monad._aggregated_scalar_subselect(make_aggr)

        result_type = float if func_name == 'AVG' else item_type
        translator.aggregated_subquery_paths.add(monad.tableref.name_path)
        result = translator.ExprMonad.new(monad.translator, result_type, sql_ast)
        if optimized: result.aggregated = True
        else: result.nogroup = True
        return result
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
        else: assert False  # pragma: no cover
        if attr.reverse:
            name_path = parent_tableref.name_path + '-' + attr.name
            monad.tableref = subquery.get_tableref(name_path) \
                             or subquery.add_tableref(name_path, parent_tableref, attr)
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
        subquery = monad._subselect()
        optimized = False
        if translator.optimize == monad.tableref.name_path:
            sql_ast = make_aggr(subquery.expr_list)
            optimized = True
            if not translator.from_optimized:
                from_ast = monad.subquery.from_ast[1:]
                from_ast[0] = from_ast[0] + [ sqland(subquery.outer_conditions) ]
                translator.subquery.from_ast.extend(from_ast)
                translator.from_optimized = True
        else: sql_ast = [ 'SELECT', [ 'AGGREGATES', make_aggr(subquery.expr_list) ],
                          subquery.from_ast,
                          [ 'WHERE' ] + subquery.outer_conditions + subquery.conditions ]
        if extra_grouping:  # This is for Oracle only, with COUNT(COUNT(*))
            sql_ast.append([ 'GROUP_BY' ] + subquery.expr_list)
        return sql_ast, optimized
    def _joined_subselect(monad, make_aggr, extra_grouping=False, coalesce_to_zero=False):
        translator = monad.translator
        subquery = monad._subselect()
        expr_list = subquery.expr_list
        from_ast = subquery.from_ast
        inner_conditions = subquery.conditions
        outer_conditions = subquery.outer_conditions

        groupby_columns = [ inner_column[:] for cond, outer_column, inner_column in outer_conditions ]
        assert len({alias for _, alias, column in groupby_columns}) == 1

        if extra_grouping:
            inner_alias = translator.subquery.make_alias('t')
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
                    new_name = 'expr-%d' % next(translator.subquery.expr_counter)
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
        expr_name = 'expr-%d' % next(translator.subquery.expr_counter)
        subquery_columns.append([ 'AS', make_aggr(expr_list), expr_name ])
        subquery_ast = [ subquery_columns, from_ast ]
        if inner_conditions and not extra_grouping:
            subquery_ast.append([ 'WHERE' ] + inner_conditions)
        subquery_ast.append([ 'GROUP_BY' ] + groupby_columns)

        alias = translator.subquery.make_alias('t')
        for cond in outer_conditions: cond[2][1] = alias
        translator.subquery.from_ast.append([ alias, 'SELECT', subquery_ast, sqland(outer_conditions) ])
        expr_ast = [ 'COLUMN', alias, expr_name ]
        if coalesce_to_zero: expr_ast = [ 'COALESCE', expr_ast, [ 'VALUE', 0 ] ]
        return expr_ast, False
    def _subselect(monad):
        if monad.subquery is not None: return monad.subquery
        attr = monad.attr
        translator = monad.translator
        subquery = Subquery(translator.subquery)
        monad.make_tableref(subquery)
        subquery.expr_list = monad.make_expr_list()
        if not attr.reverse and not attr.is_required:
            subquery.conditions.extend([ 'IS_NOT_NULL', expr ] for expr in subquery.expr_list)
        if subquery is not translator.subquery:
            outer_cond = subquery.from_ast[1].pop()
            if outer_cond[0] == 'AND': subquery.outer_conditions = outer_cond[1:]
            else: subquery.outer_conditions = [ outer_cond ]
        monad.subquery = subquery
        return subquery
    def getsql(monad, subquery=None):
        if subquery is None: subquery = monad.translator.subquery
        monad.make_tableref(subquery)
        return monad.make_expr_list()
    __add__ = make_attrset_binop('+', 'ADD')
    __sub__ = make_attrset_binop('-', 'SUB')
    __mul__ = make_attrset_binop('*', 'MUL')
    __truediv__ = make_attrset_binop('/', 'DIV')
    __floordiv__ = make_attrset_binop('//', 'FLOORDIV')

def make_numericset_binop(op, sqlop):
    def numericset_binop(monad, monad2):
        NumericSetExprMonad = monad.translator.NumericSetExprMonad
        return NumericSetExprMonad(op, sqlop, monad, monad2)
    return numericset_binop

class NumericSetExprMonad(SetMixin, Monad):
    def __init__(monad, op, sqlop, left, right):
        result_type, left, right = coerce_monads(left, right)
        assert type(result_type) is SetType
        if result_type.item_type not in numeric_types:
            throw(TypeError, _binop_errmsg % (type2str(left.type), type2str(right.type), op))
        Monad.__init__(monad, left.translator, result_type)
        monad.op = op
        monad.sqlop = sqlop
        monad.left = left
        monad.right = right
    def aggregate(monad, func_name):
        translator = monad.translator
        subquery = Subquery(translator.subquery)
        expr = monad.getsql(subquery)[0]
        translator.aggregated_subquery_paths.add(monad.tableref.name_path)
        outer_cond = subquery.from_ast[1].pop()
        if outer_cond[0] == 'AND': subquery.outer_conditions = outer_cond[1:]
        else: subquery.outer_conditions = [ outer_cond ]
        result_type = float if func_name == 'AVG' else monad.type.item_type
        aggr_ast = [ func_name, expr ]
        if monad.forced_distinct and func_name in ('SUM', 'AVG'): aggr_ast.append(True)
        if translator.optimize != monad.tableref.name_path:
            sql_ast = [ 'SELECT', [ 'AGGREGATES', aggr_ast ],
                        subquery.from_ast,
                        [ 'WHERE' ] + subquery.outer_conditions + subquery.conditions ]
            result = translator.ExprMonad.new(translator, result_type, sql_ast)
            result.nogroup = True
        else:
            if not translator.from_optimized:
                from_ast = subquery.from_ast[1:]
                from_ast[0] = from_ast[0] + [ sqland(subquery.outer_conditions) ]
                translator.subquery.from_ast.extend(from_ast)
                translator.from_optimized = True
            sql_ast = aggr_ast
            result = translator.ExprMonad.new(translator, result_type, sql_ast)
            result.aggregated = True
        return result
    def getsql(monad, subquery=None):
        if subquery is None: subquery = monad.translator.subquery
        left, right = monad.left, monad.right
        left_expr = left.getsql(subquery)[0]
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
    __truediv__ = make_numericset_binop('/', 'DIV')
    __floordiv__ = make_numericset_binop('//', 'FLOORDIV')

class QuerySetMonad(SetMixin, Monad):
    nogroup = True
    def __init__(monad, translator, subtranslator):
        monad.translator = translator
        monad.subtranslator = subtranslator
        item_type = subtranslator.expr_type
        monad.item_type = item_type
        monad_type = SetType(item_type)
        Monad.__init__(monad, translator, monad_type)
    def contains(monad, item, not_in=False):
        translator = monad.translator
        check_comparable(item, monad, 'in')
        if isinstance(item, translator.ListMonad):
            item_columns = []
            for subitem in item.items: item_columns.extend(subitem.getsql())
        else: item_columns = item.getsql()

        sub = monad.subtranslator
        if translator.hint_join and len(sub.subquery.from_ast[1]) == 3:
            subquery_ast = sub.shallow_copy_of_subquery_ast()
            select_ast, from_ast, where_ast = subquery_ast[1:4]
            subquery = translator.subquery
            if not not_in:
                translator.distinct = True
                if subquery.from_ast[0] == 'FROM':
                    subquery.from_ast[0] = 'INNER_JOIN'
            else:
                subquery.left_join = True
                subquery.from_ast[0] = 'LEFT_JOIN'
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
                new_name = 'expr-%d' % next(subquery.expr_counter)
                new_names.append(new_name)
                select_ast[i] = [ 'AS', column_ast, new_name ]

            alias = subquery.make_alias('t')
            outer_conditions = [ [ 'EQ', item_column, [ 'COLUMN', alias, new_name ] ]
                                    for item_column, new_name in izip(item_columns, new_names) ]
            subquery.from_ast.append([ alias, 'SELECT', subquery_ast[1:], sqland(outer_conditions) ])
            if not_in: sql_ast = sqland([ [ 'IS_NULL', [ 'COLUMN', alias, new_name ] ]
                                              for new_name in new_names ])
            else: sql_ast = [ 'EQ', [ 'VALUE', 1 ], [ 'VALUE', 1 ] ]
        else:
            if len(item_columns) == 1:
                subquery_ast = sub.shallow_copy_of_subquery_ast(is_not_null_checks=not_in)
                sql_ast = [ 'NOT_IN' if not_in else 'IN', item_columns[0], subquery_ast ]
            elif translator.row_value_syntax:
                subquery_ast = sub.shallow_copy_of_subquery_ast(is_not_null_checks=not_in)
                sql_ast = [ 'NOT_IN' if not_in else 'IN', [ 'ROW' ] + item_columns, subquery_ast ]
            else:
                subquery_ast = sub.shallow_copy_of_subquery_ast()
                select_ast, from_ast, where_ast = subquery_ast[1:4]
                in_conditions = [ [ 'EQ', expr1, expr2 ] for expr1, expr2 in izip(item_columns, select_ast[1:]) ]
                if not sub.aggregated: where_ast += in_conditions
                else:
                    having_ast = find_or_create_having_ast(subquery_ast)
                    having_ast += in_conditions
                sql_ast = [ 'NOT_EXISTS' if not_in else 'EXISTS' ] + subquery_ast[2:]
        return translator.BoolExprMonad(translator, sql_ast)
    def nonzero(monad):
        subquery_ast = monad.subtranslator.shallow_copy_of_subquery_ast()
        subquery_ast = [ 'EXISTS' ] + subquery_ast[2:]
        translator = monad.translator
        return translator.BoolExprMonad(translator, subquery_ast)
    def negate(monad):
        sql = monad.nonzero().sql
        assert sql[0] == 'EXISTS'
        translator = monad.translator
        return translator.BoolExprMonad(translator, [ 'NOT_EXISTS' ] + sql[1:])
    def count(monad):
        translator = monad.translator
        sub = monad.subtranslator
        if sub.aggregated: throw(TranslationError, 'Too complex aggregation in {EXPR}')
        subquery_ast = sub.shallow_copy_of_subquery_ast()
        from_ast, where_ast = subquery_ast[2:4]
        sql_ast = None

        expr_type = sub.expr_type
        if isinstance(expr_type, (tuple, EntityMeta)):
            if not sub.distinct:
                select_ast = [ 'AGGREGATES', [ 'COUNT', 'ALL' ] ]
            elif len(sub.expr_columns) == 1:
                select_ast = [ 'AGGREGATES', [ 'COUNT', 'DISTINCT' ] + sub.expr_columns ]
            elif translator.dialect == 'Oracle':
                sql_ast = [ 'SELECT', [ 'AGGREGATES', [ 'COUNT', 'ALL', [ 'COUNT', 'ALL' ] ] ],
                            from_ast, where_ast, [ 'GROUP_BY' ] + sub.expr_columns ]
            elif translator.row_value_syntax:
                select_ast = [ 'AGGREGATES', [ 'COUNT', 'DISTINCT' ] + sub.expr_columns ]
            elif translator.dialect == 'SQLite':
                if translator.sqlite_version < (3, 6, 21):
                    if sub.aggregated: throw(TranslationError)
                    alias, pk_columns = sub.tableref.make_join(pk_only=False)
                    subquery_ast = sub.shallow_copy_of_subquery_ast()
                    from_ast, where_ast = subquery_ast[2:4]
                    sql_ast = [ 'SELECT',
                        [ 'AGGREGATES', [ 'COUNT', 'DISTINCT', [ 'COLUMN', alias, 'ROWID' ] ] ],
                        from_ast, where_ast ]
                else:
                    alias = translator.subquery.make_alias('t')
                    sql_ast = [ 'SELECT', [ 'AGGREGATES', [ 'COUNT', 'ALL' ] ],
                                [ 'FROM', [ alias, 'SELECT', [
                                  [ 'DISTINCT' ] + sub.expr_columns, from_ast, where_ast ] ] ] ]
            else: assert False  # pragma: no cover
        elif len(sub.expr_columns) == 1:
            select_ast = [ 'AGGREGATES', [ 'COUNT', 'DISTINCT', sub.expr_columns[0] ] ]
        else: throw(NotImplementedError)  # pragma: no cover

        if sql_ast is None: sql_ast = [ 'SELECT', select_ast, from_ast, where_ast ]
        return translator.ExprMonad.new(translator, int, sql_ast)
    len = count
    def aggregate(monad, func_name):
        translator = monad.translator
        sub = monad.subtranslator
        if sub.aggregated: throw(TranslationError, 'Too complex aggregation in {EXPR}')
        subquery_ast = sub.shallow_copy_of_subquery_ast()
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
        else: assert False  # pragma: no cover
        assert len(sub.expr_columns) == 1
        aggr_ast = [ func_name, sub.expr_columns[0] ]
        if monad.forced_distinct and func_name in ('SUM', 'AVG'): aggr_ast.append(True)
        select_ast = [ 'AGGREGATES', aggr_ast ]
        sql_ast = [ 'SELECT', select_ast, from_ast, where_ast ]
        result_type = float if func_name == 'AVG' else expr_type
        return translator.ExprMonad.new(translator, result_type, sql_ast)
    def call_count(monad):
        return monad.count()
    def call_sum(monad):
        return monad.aggregate('SUM')
    def call_min(monad):
        return monad.aggregate('MIN')
    def call_max(monad):
        return monad.aggregate('MAX')
    def call_avg(monad):
        return monad.aggregate('AVG')

def find_or_create_having_ast(subquery_ast):
    groupby_offset = None
    for i, section in enumerate(subquery_ast):
        section_name = section[0]
        if section_name == 'GROUP_BY':
            groupby_offset = i
        elif section_name == 'HAVING':
            return section
    having_ast = [ 'HAVING' ]
    subquery_ast.insert(groupby_offset + 1, having_ast)
    return having_ast

for name, value in items_list(globals()):
    if name.endswith('Monad') or name.endswith('Mixin'):
        setattr(SQLTranslator, name, value)
del name, value
