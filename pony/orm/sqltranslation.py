import ast
import inspect
import itertools
import re
import sys
import types
from copy import deepcopy
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from functools import update_wrapper
from random import random
from uuid import UUID

from pony import utils
from pony.orm import core
from pony.orm.asttranslation import (
    ASTTranslator,
    TranslationError,
    ast2src,
    create_extractors,
    get_child_nodes,
)
from pony.orm.core import (
    JOIN,
    Attribute,
    DescWrapper,
    EntityMeta,
    Set,
    UseAnotherTranslator,
    const_functions,
    extract_vars,
    special_functions,
)
from pony.orm.decompiling import DecompileError, decompile, operator_mapping
from pony.orm.ormtypes import (
    Array,
    FuncType,
    Json,
    MethodType,
    QueryType,
    RawSQLType,
    SetType,
    are_comparable_types,
    array_types,
    coerce_types,
    comparable_types,
    normalize,
    normalize_type,
    numeric_types,
    raw_sql,
)
from pony.py23compat import PY310, buffer, int_types
from pony.utils import (
    IntegerGenerator,
    between,
    coalesce,
    concat,
    copy_ast,
    is_ident,
    localbase,
    reraise,
    throw,
)

NoneType = type(None)


def check_comparable(left_monad, right_monad, op="=="):
    t1, t2 = left_monad.type, right_monad.type
    if t1 == "METHOD":
        raise_forgot_parentheses(left_monad)
    if t2 == "METHOD":
        raise_forgot_parentheses(right_monad)
    if not are_comparable_types(t1, t2, op):
        if op in ("in", "not in") and isinstance(t2, SetType):
            t2 = t2.item_type
        throw(IncomparableTypesError, t1, t2)


class IncomparableTypesError(TypeError):
    def __init__(self, type1, type2):
        msg = "Incomparable types %r and %r in expression: {EXPR}" % (
            type2str(type1),
            type2str(type2),
        )
        TypeError.__init__(self, msg)
        self.type1 = type1
        self.type2 = type2


def sqland(items):
    if not items:
        return []
    if len(items) == 1:
        return items[0]
    result = ["AND"]
    for item in items:
        if item[0] == "AND":
            result.extend(item[1:])
        else:
            result.append(item)
    return result


def sqlor(items):
    if not items:
        return []
    if len(items) == 1:
        return items[0]
    result = ["OR"]
    for item in items:
        if item[0] == "OR":
            result.extend(item[1:])
        else:
            result.append(item)
    return result


def join_tables(alias1, alias2, columns1, columns2):
    assert len(columns1) == len(columns2)
    return sqland(
        [
            ["EQ", ["COLUMN", alias1, c1], ["COLUMN", alias2, c2]]
            for c1, c2 in zip(columns1, columns2)
        ]
    )


def type2str(t):
    if type(t) is tuple:
        return "list"
    if type(t) is SetType:
        return "Set of " + type2str(t.item_type)
    try:
        return t.__name__
    except BaseException:
        return str(t)


class Local(localbase):
    def __init__(self):
        self.translators = []

    @property
    def translator(self):
        return local.translators[-1]


translator_counter = itertools.count(1)

local = Local()


class SQLTranslator(ASTTranslator):
    dialect = None
    row_value_syntax = True
    json_path_wildcard_syntax = False
    json_values_are_comparable = True
    rowid_support = False

    def __enter__(self):
        local.translators.append(self)

    def __exit__(self, exc_type, exc_val, exc_tb):
        t = local.translators.pop()
        if isinstance(exc_val, UseAnotherTranslator):
            assert t is exc_val.translator
        else:
            assert t is self

    def default_post(self, node):
        throw(NotImplementedError)  # pragma: no cover

    def dispatch(self, node):
        if hasattr(node, "monad"):
            return  # monad already assigned somehow
        if not getattr(node, "external", False) or getattr(node, "constant", False):
            return ASTTranslator.dispatch(self, node)  # default route
        self.call(self.__class__.dispatch_external, node)

    def dispatch_external(self, node):
        varkey = self.filter_num, node.src, self.code_key
        t = self.root_translator.vartypes[varkey]
        tt = type(t)
        if t is NoneType:
            monad = ConstMonad.new(None)
        elif tt is SetType:
            if isinstance(t.item_type, EntityMeta):
                monad = EntityMonad(t.item_type)
            else:
                throw(NotImplementedError)  # pragma: no cover
        elif tt is QueryType:
            prev_translator = t.translator.deepcopy()
            prev_translator.parent = self
            prev_translator.injected = True
            if self.database is not prev_translator.database:
                throw(TranslationError, "Mixing queries from different databases")
            monad = QuerySetMonad(prev_translator)
            if t.limit is not None or t.offset is not None:
                monad = monad.call_limit(t.limit, t.offset)
        elif tt is FuncType:
            func = t.func
            func_monad_class = self.registered_functions.get(func)
            if func_monad_class is not None:
                monad = func_monad_class(func)
            else:
                monad = HybridFuncMonad(t, func.__name__)
        elif tt is MethodType:
            obj, func = t.obj, t.func
            if isinstance(obj, EntityMeta):
                entity_monad = EntityMonad(obj)
                if obj.__class__.__dict__.get(func.__name__) is not func:
                    throw(NotImplementedError)
                monad = MethodMonad(entity_monad, func.__name__)
            elif node.src == "random":  # For PyPy
                monad = FuncRandomMonad(t)
            else:
                throw(NotImplementedError)
        elif tt is tuple:
            params = []
            is_array = False
            if t and self.database.provider.array_converter_cls is not None:
                types = set(t)
                if len(types) == 1 and str in types:
                    item_type = str
                    is_array = True
                else:
                    item_type = int
                    for type_ in types:
                        if type_ is float:
                            item_type = float
                        if type_ not in (float, int) or not hasattr(type_, "__index__"):
                            break
                    else:
                        is_array = True

            for i, item_type in enumerate(t):
                if item_type is NoneType:
                    throw(
                        TypeError,
                        "Expression `%s` should not contain None values" % node.src,
                    )
                param = ParamMonad.new(item_type, (varkey, i, None))
                params.append(param)
            monad = ListMonad(params)
            if is_array:
                array_type = array_types.get(item_type, None)
                monad = ArrayParamMonad(
                    array_type, (varkey, None, None), list_monad=monad
                )
        elif isinstance(t, RawSQLType):
            monad = RawSQLMonad(t, varkey)
        else:
            monad = ParamMonad.new(t, (varkey, None, None))
        node.monad = monad
        monad.node = node
        monad.aggregated = monad.nogroup = False

    def call(self, method, node):
        try:
            monad = method(self, node)
        except Exception:
            exc_class, exc, tb = sys.exc_info()
            try:
                if not exc.args:
                    exc.args = (ast2src(node),)
                else:
                    msg = exc.args[0]
                    if isinstance(msg, str) and "{EXPR}" in msg:
                        msg = msg.replace("{EXPR}", ast2src(node))
                        exc.args = (msg,) + exc.args[1:]
                reraise(exc_class, exc, tb)
            finally:
                del exc, tb
        else:
            if monad is None:
                return
            node.monad = monad
            monad.node = node
            if not hasattr(monad, "aggregated"):
                if isinstance(monad, QuerySetMonad):
                    monad.aggregated = False
                else:
                    for child in get_child_nodes(node):
                        m = getattr(child, "monad", None)
                        if m and getattr(m, "aggregated", False):
                            monad.aggregated = True
                            break
                    else:
                        monad.aggregated = False
            if not hasattr(monad, "nogroup"):
                for child in get_child_nodes(node):
                    m = getattr(child, "monad", None)
                    if m and getattr(m, "nogroup", False):
                        monad.nogroup = True
                        break
                else:
                    monad.nogroup = False
            if monad.aggregated:
                self.aggregated = True
                if monad.nogroup:
                    if isinstance(monad, ListMonad):
                        pass
                    elif isinstance(monad, AndMonad):
                        pass
                    else:
                        throw(
                            TranslationError,
                            "Too complex aggregation, expressions cannot be combined: %s"
                            % ast2src(node),
                        )
            return monad

    def __repr__(self):
        return "%s<%d>" % (self.__class__.__name__, self.id)

    def deepcopy(self):
        result = deepcopy(self)
        result.id = next(translator_counter)
        result.copied_from = self
        return result

    def __init__(
        self,
        tree,
        parent_translator,
        code_key=None,
        filter_num=None,
        extractors=None,
        vars=None,
        vartypes=None,
        left_join=False,
        optimize=None,
    ):
        ASTTranslator.__init__(self, tree)
        self.id = next(translator_counter)
        local.translators.append(self)
        try:
            self.init(
                tree,
                parent_translator,
                code_key,
                filter_num,
                extractors,
                vars,
                vartypes,
                left_join,
                optimize,
            )
        finally:
            assert local.translators
            local.translators.pop()
            # If UseAnotherTranslator exception happened inside translator.init() then
            # the translator we take from the stack may be different from the translator that we pushed above

    def init(
        self,
        tree,
        parent_translator,
        code_key=None,
        filter_num=None,
        extractors=None,
        vars=None,
        vartypes=None,
        left_join=False,
        optimize=None,
    ):
        this = self
        assert isinstance(tree, ast.GeneratorExp), tree
        self.can_be_cached = True
        self.parent = parent_translator
        self.injected = False
        if parent_translator is None:
            self.root_translator = self
            self.database = None
            self.sqlquery = SqlQuery(self, left_join=left_join)
            assert code_key is not None and filter_num is not None
            self.code_key = self.original_code_key = code_key
            self.filter_num = self.original_filter_num = filter_num
        else:
            self.root_translator = parent_translator.root_translator
            self.database = parent_translator.database
            self.sqlquery = SqlQuery(
                self, parent_translator.sqlquery, left_join=left_join
            )
            assert code_key is None and filter_num is None
            self.code_key = parent_translator.code_key
            self.filter_num = parent_translator.filter_num
            self.original_code_key = self.original_filter_num = None
        self.extractors = extractors
        self.vars = vars
        self.vartypes = vartypes
        self.namespace_stack = (
            [{}] if not parent_translator else [parent_translator.namespace.copy()]
        )
        self.func_extractors_map = {}
        self.fixed_param_values = {}
        self.func_vartypes = {}
        self.left_join = left_join
        self.optimize = optimize
        self.from_optimized = False
        self.optimization_failed = False
        self.distinct = False
        self.conditions = self.sqlquery.conditions
        self.having_conditions = []
        self.order = []
        self.limit = self.offset = None
        self.inside_order_by = False
        self.aggregated = False if not optimize else True
        self.hint_join = False
        self.query_result_is_cacheable = True
        self.aggregated_subquery_paths = set()
        for i, generator in enumerate(tree.generators):
            target = generator.target
            if isinstance(target, ast.Tuple):
                ass_names = tuple(target.elts)
            elif isinstance(target, ast.Name):
                ass_names = (target,)
            else:
                throw(NotImplementedError, ast2src(target))

            for ass_name in ass_names:
                if not isinstance(ass_name, ast.Name):
                    throw(NotImplementedError, ast2src(ass_name))
                if not isinstance(ass_name.ctx, ast.Store):
                    throw(TypeError, ast2src(ass_name))

            names = tuple(ass_name.id for ass_name in ass_names)
            for name in names:
                if name in self.namespace and name in self.sqlquery.tablerefs:
                    throw(TranslationError, "Duplicate name: %r" % name)
                if name.startswith("__"):
                    throw(TranslationError, "Illegal name: %r" % name)

            name = names[0] if len(names) == 1 else None

            def check_name_is_single():
                if len(names) > 1:
                    throw(
                        TypeError,
                        "Single variable name expected. Got: %s" % ast2src(target),
                    )

            database = entity = None

            node = generator.iter
            monad = getattr(node, "monad", None)

            if monad:  # Lambda was encountered inside generator
                check_name_is_single()
                assert parent_translator and i == 0
                entity = monad.type.item_type
                if isinstance(monad, EntityMonad):
                    tableref = TableRef(self.sqlquery, name, entity)
                    self.sqlquery.tablerefs[name] = tableref
                elif isinstance(monad, AttrSetMonad):
                    self.sqlquery = monad._subselect(
                        self.sqlquery, extract_outer_conditions=False
                    )
                    tableref = monad.tableref
                else:
                    assert False  # pragma: no cover
                self.namespace[name] = ObjectIterMonad(tableref, entity)
            elif node.external:
                varkey = self.filter_num, node.src, self.code_key
                iterable = self.root_translator.vartypes[varkey]
                if isinstance(iterable, SetType):
                    check_name_is_single()
                    entity = iterable.item_type
                    if not isinstance(entity, EntityMeta):
                        throw(
                            TranslationError,
                            "for %s in %s" % (name, ast2src(generator.iter)),
                        )
                    if i > 0:
                        if self.left_join:
                            throw(
                                TranslationError,
                                "Collection expected inside left join query. "
                                "Got: for %s in %s" % (name, ast2src(generator.iter)),
                            )
                        self.distinct = True
                    tableref = TableRef(self.sqlquery, name, entity)
                    self.sqlquery.tablerefs[name] = tableref
                    tableref.make_join()
                    self.namespace[name] = node.monad = ObjectIterMonad(
                        tableref, entity
                    )
                elif isinstance(iterable, QueryType):
                    prev_translator = iterable.translator.deepcopy()
                    prev_limit = iterable.limit
                    prev_offset = iterable.offset
                    # Only an explicit distinct() matters, without_distinct() keeps the old behavior
                    prev_distinct = True if iterable.distinct else None
                    database = prev_translator.database
                    try:
                        self.process_query_qual(
                            prev_translator,
                            prev_limit,
                            prev_offset,
                            names,
                            try_extend_prev_query=not i,
                            prev_distinct=prev_distinct,
                        )
                    except UseAnotherTranslator as e:
                        assert local.translators and local.translators[-1] is self
                        self = e.translator
                        local.translators[-1] = self
                else:
                    throw(
                        TranslationError,
                        "Inside declarative query, iterator must be entity or query. "
                        "Got: for %s in %s" % (name, ast2src(generator.iter)),
                    )

            else:
                self.dispatch(node)
                monad = node.monad

                if isinstance(monad, QuerySetMonad):
                    subtranslator = monad.subtranslator
                    database = subtranslator.database
                    try:
                        self.process_query_qual(
                            subtranslator, monad.limit, monad.offset, names
                        )
                    except UseAnotherTranslator:
                        assert False
                else:
                    check_name_is_single()
                    attr_names = []
                    while (
                        isinstance(monad, (AttrMonad, AttrSetMonad))
                        and monad.parent is not None
                    ):
                        attr_names.append(monad.attr.name)
                        monad = monad.parent
                    attr_names.reverse()

                    if not isinstance(monad, ObjectIterMonad):
                        throw(
                            NotImplementedError,
                            "for %s in %s" % (name, ast2src(generator.iter)),
                        )
                    name_path = monad.tableref.alias  # or name_path, it is the same

                    parent_tableref = monad.tableref
                    parent_entity = parent_tableref.entity

                    last_index = len(attr_names) - 1
                    for j, attrname in enumerate(attr_names):
                        attr = parent_entity._adict_.get(attrname)
                        if attr is None:
                            throw(AttributeError, attrname)
                        entity = attr.py_type
                        if not isinstance(entity, EntityMeta):
                            throw(
                                NotImplementedError,
                                "for %s in %s" % (name, ast2src(generator.iter)),
                            )
                        can_affect_distinct = None
                        if attr.is_collection:
                            if not isinstance(attr, Set):
                                throw(NotImplementedError, ast2src(generator.iter))
                            reverse = attr.reverse
                            if reverse.is_collection:
                                if not isinstance(reverse, Set):
                                    throw(NotImplementedError, ast2src(generator.iter))
                                self.distinct = True
                            elif (
                                parent_tableref.alias
                                != tree.generators[i - 1].target.id
                            ):
                                self.distinct = True
                            else:
                                can_affect_distinct = True
                        if j == last_index:
                            name_path = name
                        else:
                            name_path += "-" + attr.name
                        tableref = self.sqlquery.add_tableref(
                            name_path, parent_tableref, attr
                        )
                        tableref.make_join(pk_only=True)
                        if j == last_index:
                            self.namespace[name] = ObjectIterMonad(
                                tableref, tableref.entity
                            )
                        if can_affect_distinct is not None:
                            tableref.can_affect_distinct = can_affect_distinct
                        parent_tableref = tableref
                        parent_entity = entity

            if database is None:
                assert entity is not None
                database = entity._database_
            assert database.schema is not None
            if self.database is None:
                self.database = database
            elif self.database is not database:
                throw(
                    TranslationError,
                    "All entities in a query must belong to the same database",
                )

            for if_ in generator.ifs:
                self.dispatch(if_)
                if if_.monad.type is not bool:
                    if_.monad = if_.monad.nonzero()
                cond_monads = (
                    if_.monad.operands
                    if isinstance(if_.monad, AndMonad)
                    else [if_.monad]
                )
                for m in cond_monads:
                    if not getattr(m, "aggregated", False):
                        self.conditions.extend(m.getsql())
                    else:
                        self.having_conditions.extend(m.getsql())

        self.dispatch(tree.elt)
        assert not self.hint_join
        monad = tree.elt.monad
        if isinstance(monad, ParamMonad):
            throw(
                TranslationError,
                "External parameter '%s' cannot be used as query result"
                % ast2src(tree.elt),
            )
        self.expr_monads = monad.items if isinstance(monad, ListMonad) else [monad]
        self.groupby_monads = None
        expr_type = monad.type
        if isinstance(expr_type, SetType):
            expr_type = expr_type.item_type
        if isinstance(expr_type, EntityMeta):
            entity = expr_type
            self.expr_type = entity
            monad.orderby_columns = list(range(1, len(entity._pk_columns_) + 1))
            if monad.aggregated:
                throw(TranslationError)
            if isinstance(monad, QuerySetMonad):
                throw(NotImplementedError)
            elif isinstance(monad, ObjectMixin):
                tableref = monad.tableref
            elif isinstance(monad, AttrSetMonad):
                tableref = monad.make_tableref(self.sqlquery)
            else:
                assert False  # pragma: no cover
            if self.aggregated:
                self.groupby_monads = [monad]
            else:
                self.distinct |= monad.requires_distinct()
            self.tableref = tableref
            pk_only = parent_translator is not None or self.aggregated
            alias, pk_columns = tableref.make_join(pk_only=pk_only)
            self.alias = alias
            self.expr_columns = [["COLUMN", alias, column] for column in pk_columns]
            self.row_layout = None
            self.col_names = [
                attr.name
                for attr in entity._attrs_
                if not attr.is_collection and not attr.lazy
            ]
        else:
            self.alias = None
            expr_monads = self.expr_monads
            if len(expr_monads) > 1:
                self.expr_type = tuple(m.type for m in expr_monads)  # ?????
                expr_columns = []
                for m in expr_monads:
                    expr_columns.extend(m.getsql())
                self.expr_columns = expr_columns
            else:
                self.expr_type = monad.type
                self.expr_columns = monad.getsql()
            if self.aggregated:
                self.groupby_monads = [
                    m for m in expr_monads if not m.aggregated and not m.nogroup
                ]
            else:
                expr_set = set()
                for m in expr_monads:
                    if isinstance(m, ObjectIterMonad):
                        expr_set.add(m.tableref.name_path)
                    elif isinstance(m, AttrMonad) and isinstance(
                        m.parent, ObjectIterMonad
                    ):
                        expr_set.add((m.parent.tableref.name_path, m.attr))
                for tr in self.sqlquery.tablerefs.values():
                    if tr.entity is None:
                        continue
                    if not tr.can_affect_distinct:
                        continue
                    if tr.name_path in expr_set:
                        continue
                    if any(
                        (tr.name_path, attr) not in expr_set
                        for attr in tr.entity._pk_attrs_
                    ):
                        self.distinct = True
                        break
            row_layout = []
            offset = 0
            provider = self.database.provider
            for m in expr_monads:
                if m.disable_distinct:
                    self.distinct = False
                expr_type = m.type
                if isinstance(expr_type, SetType):
                    expr_type = expr_type.item_type
                if isinstance(expr_type, EntityMeta):
                    next_offset = offset + len(expr_type._pk_columns_)

                    def func(values, constructor=expr_type._get_by_raw_pkval_):
                        if None in values:
                            return None
                        return constructor(values)

                    row_layout.append(
                        (func, slice(offset, next_offset), ast2src(m.node))
                    )
                    m.orderby_columns = list(range(offset + 1, next_offset + 1))
                    offset = next_offset
                else:
                    converter = provider.get_converter_by_py_type(expr_type)

                    def func(value, converter=converter):
                        if value is None:
                            return None
                        value = converter.sql2py(value)
                        value = converter.dbval2val(value)
                        return value

                    row_layout.append((func, offset, ast2src(m.node)))
                    m.orderby_columns = (offset + 1,) if not m.disable_ordering else ()
                    offset += 1
            self.row_layout = row_layout
            self.col_names = [src for func, slice_or_offset, src in self.row_layout]
        if self.aggregated:
            self.distinct = False
        self.vars = None
        if self is not this:
            raise UseAnotherTranslator(self)

    @property
    def namespace(self):
        return self.namespace_stack[-1]

    def can_be_optimized(self):
        if self.groupby_monads:
            return False
        if len(self.aggregated_subquery_paths) != 1:
            return False
        aggr_path = next(iter(self.aggregated_subquery_paths))
        for tableref in self.sqlquery.tablerefs.values():
            if tableref.joined and not aggr_path.startswith(tableref.name_path):
                return False
        return aggr_path

    def process_query_qual(
        self,
        prev_translator,
        prev_limit,
        prev_offset,
        names,
        try_extend_prev_query=False,
        prev_distinct=None,
    ):
        # prev_distinct is the explicit distinct flag of the previous query
        # (set by query.distinct() / distinct(...) / query.without_distinct()).
        # When it is specified, the previous query cannot be merged into the
        # current one, because the new query (for example, an aggregate one)
        # may drop the DISTINCT clause. In that case the previous query is
        # translated as a subquery with the requested DISTINCT setting.
        sqlquery = self.sqlquery
        tablerefs = sqlquery.tablerefs
        expr_types = prev_translator.expr_type
        if not isinstance(expr_types, tuple):
            expr_types = (expr_types,)
        expr_count = len(expr_types)

        if expr_count > 1 and len(names) == 1:
            throw(
                NotImplementedError,
                'Please unpack a tuple of (%s) in for-loop to individual variables (like: "for x, y in ...")'
                % (", ".join(ast2src(m.node) for m in prev_translator.expr_monads)),
            )
        elif expr_count > len(names):
            throw(
                TranslationError,
                'Not enough values to unpack "for %s in select(%s for ...)" (expected %d, got %d)'
                % (
                    ", ".join(names),
                    ", ".join(ast2src(m.node) for m in prev_translator.expr_monads),
                    len(names),
                    expr_count,
                ),
            )
        elif expr_count < len(names):
            throw(
                TranslationError,
                'Too many values to unpack "for %s in select(%s for ...)" (expected %d, got %d)'
                % (
                    ", ".join(names),
                    ", ".join(ast2src(m.node) for m in prev_translator.expr_monads),
                    len(names),
                    expr_count,
                ),
            )

        if try_extend_prev_query:
            if prev_translator.aggregated:
                pass
            elif prev_translator.left_join:
                pass
            elif prev_distinct is not None:
                pass
            else:
                assert self.parent is None
                assert prev_translator.vars is None
                prev_translator.code_key = self.code_key
                prev_translator.filter_num = self.filter_num
                prev_translator.extractors.update(self.extractors)
                prev_translator.vars = self.vars
                prev_translator.vartypes.update(self.vartypes)
                prev_translator.left_join = self.left_join
                prev_translator.optimize = self.optimize
                prev_translator.namespace_stack = [
                    {
                        name: expr
                        for name, expr in zip(names, prev_translator.expr_monads)
                    }
                ]
                prev_translator.limit, prev_translator.offset = (
                    combine_limit_and_offset(
                        prev_translator.limit,
                        prev_translator.offset,
                        prev_limit,
                        prev_offset,
                    )
                )
                raise UseAnotherTranslator(prev_translator)

        if (
            len(names) == 1
            and isinstance(prev_translator.expr_type, EntityMeta)
            and not prev_translator.aggregated
            and not prev_translator.distinct
            and not prev_distinct
        ):
            name = names[0]
            entity = prev_translator.expr_type
            [expr_monad] = prev_translator.expr_monads
            entity_alias = expr_monad.tableref.alias
            subquery_ast = prev_translator.construct_subquery_ast(
                prev_limit, prev_offset, star=entity_alias, distinct=prev_distinct
            )
            tableref = StarTableRef(sqlquery, name, entity, subquery_ast)
            tablerefs[name] = tableref
            tableref.make_join()
            self.namespace[name] = ObjectIterMonad(tableref, entity)
        else:
            aliases = []
            aliases_dict = {}
            for name, base_expr_monad in zip(names, prev_translator.expr_monads):
                t = base_expr_monad.type
                if isinstance(t, EntityMeta):
                    t_aliases = []
                    for suffix in t._pk_paths_:
                        alias = "%s-%s" % (name, suffix)
                        t_aliases.append(alias)
                    aliases.extend(t_aliases)
                    aliases_dict[base_expr_monad] = t_aliases
                else:
                    aliases.append(name)
                    aliases_dict[base_expr_monad] = name

            subquery_ast = prev_translator.construct_subquery_ast(
                prev_limit, prev_offset, aliases=aliases, distinct=prev_distinct
            )
            tableref = ExprTableRef(sqlquery, "t", subquery_ast, names, aliases)
            for name in names:
                tablerefs[name] = tableref
            tableref.make_join()

            for name, base_expr_monad in zip(names, prev_translator.expr_monads):
                t = base_expr_monad.type
                if isinstance(t, EntityMeta):
                    columns = aliases_dict[base_expr_monad]
                    expr_tableref = ExprJoinedTableRef(
                        sqlquery, tableref, columns, name, t
                    )
                    expr_monad = ObjectIterMonad(expr_tableref, t)
                else:
                    column = aliases_dict[base_expr_monad]
                    expr_ast = ["COLUMN", tableref.alias, column]
                    expr_monad = ExprMonad.new(t, expr_ast, base_expr_monad.nullable)
                assert name not in self.namespace
                self.namespace[name] = expr_monad

    def construct_subquery_ast(
        self,
        limit=None,
        offset=None,
        aliases=None,
        star=None,
        distinct=None,
        is_not_null_checks=False,
    ):
        subquery_ast, attr_offsets = self.construct_sql_ast(
            limit,
            offset,
            distinct,
            is_not_null_checks=is_not_null_checks,
            subquery=True,
        )
        assert len(subquery_ast) >= 3 and subquery_ast[0] == "SELECT"

        select_ast = subquery_ast[1][:]
        assert select_ast[0] in ("ALL", "DISTINCT", "AGGREGATES"), select_ast
        if aliases:
            assert not star and len(aliases) == len(select_ast) - 1
            for i, alias in enumerate(aliases):
                expr = select_ast[i + 1]
                if expr[0] == "AS":
                    expr = expr[1]
                select_ast[i + 1] = ["AS", expr, alias]
        elif star is not None:
            assert isinstance(star, str)
            for section in subquery_ast:
                assert section[0] not in ("GROUP_BY", "HAVING"), subquery_ast
            select_ast[1:] = [["STAR", star]]

        from_ast = subquery_ast[2][:]
        assert from_ast[0] in ("FROM", "LEFT_JOIN")

        if len(subquery_ast) == 3:
            where_ast = ["WHERE"]
            other_ast = []
        elif subquery_ast[3][0] != "WHERE":
            where_ast = ["WHERE"]
            other_ast = subquery_ast[3:]
        else:
            where_ast = subquery_ast[3][:]
            other_ast = subquery_ast[4:]

        if len(from_ast[1]) == 4:
            outer_conditions = from_ast[1][-1]
            from_ast[1] = from_ast[1][:-1]
            if outer_conditions[0] == "AND":
                where_ast[1:1] = outer_conditions[1:]
            else:
                where_ast.insert(1, outer_conditions)

        return ["SELECT", select_ast, from_ast, where_ast] + other_ast

    def construct_sql_ast(
        self,
        limit=None,
        offset=None,
        distinct=None,
        aggr_func_name=None,
        aggr_func_distinct=None,
        sep=None,
        for_update=False,
        nowait=False,
        skip_locked=False,
        is_not_null_checks=False,
        subquery=False,
    ):
        attr_offsets = None
        if distinct is None:
            if not self.order:
                distinct = self.distinct
        ast_transformer = lambda ast: ast
        if for_update:
            sql_ast = ["SELECT_FOR_UPDATE", nowait, skip_locked]
            self.query_result_is_cacheable = False
        else:
            sql_ast = ["SELECT"]

        select_ast = ["DISTINCT" if distinct else "ALL"] + self.expr_columns
        if aggr_func_name:
            expr_type = self.expr_type
            if isinstance(expr_type, EntityMeta):
                if aggr_func_name == "GROUP_CONCAT":
                    if expr_type._pk_is_composite_:
                        throw(
                            TypeError,
                            "`group_concat` cannot be used with entity with composite primary key",
                        )
                elif aggr_func_name != "COUNT":
                    throw(
                        TypeError,
                        "Attribute should be specified for %r aggregate function"
                        % aggr_func_name.lower(),
                    )
            elif isinstance(expr_type, tuple):
                if aggr_func_name != "COUNT":
                    throw(
                        TypeError,
                        "Single attribute should be specified for %r aggregate function"
                        % aggr_func_name.lower(),
                    )
            else:
                if aggr_func_name in ("SUM", "AVG") and expr_type not in numeric_types:
                    throw(
                        TypeError,
                        "%r is valid for numeric attributes only"
                        % aggr_func_name.lower(),
                    )
                assert len(self.expr_columns) == 1
            aggr_ast = None
            if self.groupby_monads or (
                aggr_func_name == "COUNT"
                and distinct
                and isinstance(self.expr_type, EntityMeta)
                and len(self.expr_columns) > 1
            ):
                outer_alias = "t"
                if aggr_func_name == "COUNT" and not aggr_func_distinct:
                    outer_aggr_ast = ["COUNT", None]
                else:
                    assert len(self.expr_columns) == 1
                    expr_ast = self.expr_columns[0]
                    if expr_ast[0] == "COLUMN":
                        outer_alias, column_name = expr_ast[1:]
                        outer_aggr_ast = [
                            aggr_func_name,
                            aggr_func_distinct,
                            ["COLUMN", outer_alias, column_name],
                        ]
                        if aggr_func_name == "GROUP_CONCAT" and sep is not None:
                            outer_aggr_ast.append(["VALUE", sep])
                    else:
                        select_ast = ["DISTINCT" if distinct else "ALL"] + [
                            ["AS", expr_ast, "expr"]
                        ]
                        outer_aggr_ast = [
                            aggr_func_name,
                            aggr_func_distinct,
                            ["COLUMN", "t", "expr"],
                        ]
                        if aggr_func_name == "GROUP_CONCAT" and sep is not None:
                            outer_aggr_ast.append(["VALUE", sep])

                def ast_transformer(ast):
                    return [
                        "SELECT",
                        ["AGGREGATES", outer_aggr_ast],
                        ["FROM", [outer_alias, "SELECT", ast[1:]]],
                    ]
            else:
                if aggr_func_name == "COUNT":
                    if (
                        isinstance(expr_type, (tuple, EntityMeta))
                        and not distinct
                        and not aggr_func_distinct
                    ):
                        aggr_ast = ["COUNT", aggr_func_distinct]
                    else:
                        aggr_ast = [
                            "COUNT",
                            True if aggr_func_distinct is None else aggr_func_distinct,
                            self.expr_columns[0],
                        ]
                else:
                    aggr_ast = [
                        aggr_func_name,
                        aggr_func_distinct,
                        self.expr_columns[0],
                    ]
                    if aggr_func_name == "GROUP_CONCAT" and sep is not None:
                        aggr_ast.append(["VALUE", sep])

            if aggr_ast:
                select_ast = ["AGGREGATES", aggr_ast]
        elif (
            isinstance(self.expr_type, EntityMeta)
            and not self.parent
            and not self.aggregated
            and not self.optimize
            and not subquery
        ):
            # Full entity columns are needed only for the top-level query;
            # a subquery selects entity primary key columns only
            select_ast, attr_offsets = self.expr_type._construct_select_clause_(
                self.alias, distinct, self.tableref.used_attrs
            )
        sql_ast.append(select_ast)
        sql_ast.append(self.sqlquery.from_ast)

        conditions = self.conditions[:]
        having_conditions = self.having_conditions[:]
        if is_not_null_checks:
            for monad in self.expr_monads:
                if isinstance(monad, ObjectIterMonad):
                    pass
                elif not monad.nullable:
                    pass
                else:
                    notnull_conditions = [
                        ["IS_NOT_NULL", column_ast] for column_ast in monad.getsql()
                    ]
                    if monad.aggregated:
                        having_conditions.extend(notnull_conditions)
                    else:
                        conditions.extend(notnull_conditions)
        if conditions:
            sql_ast.append(["WHERE"] + conditions)

        if self.groupby_monads:
            group_by = ["GROUP_BY"]
            for m in self.groupby_monads:
                group_by.extend(m.getsql())
            sql_ast.append(group_by)
        else:
            group_by = None

        if having_conditions:
            if not group_by:
                throw(
                    TranslationError,
                    "In order to use aggregated functions such as SUM(), COUNT(), etc., "
                    "query must have grouping columns (i.e. resulting non-aggregated values)",
                )
            sql_ast.append(["HAVING"] + having_conditions)

        if self.order and not aggr_func_name:
            sql_ast.append(["ORDER_BY"] + self.order)

        limit, offset = combine_limit_and_offset(self.limit, self.offset, limit, offset)
        if limit is not None or offset is not None:
            assert not aggr_func_name
            provider = self.database.provider
            if limit is None:
                if provider.dialect == "SQLite":
                    limit = -1
                elif provider.dialect == "MySQL":
                    limit = 18446744073709551615
            limit_section = ["LIMIT", limit]
            if offset:
                limit_section.append(offset)
            sql_ast.append(limit_section)

        sql_ast = ast_transformer(sql_ast)
        return sql_ast, attr_offsets

    def construct_delete_sql_ast(self):
        entity = self.expr_type
        expr_monad = self.tree.elt.monad
        if not isinstance(entity, EntityMeta):
            throw(
                TranslationError,
                "Delete query should be applied to a single entity. Got: %s"
                % ast2src(self.tree.expr),
            )
        force_in = False
        if self.groupby_monads:
            force_in = True
        else:
            assert not self.having_conditions
        tableref = expr_monad.tableref
        from_ast = self.sqlquery.from_ast
        if from_ast[0] != "FROM":
            force_in = True

        if not force_in and len(from_ast) == 2 and not self.sqlquery.used_from_subquery:
            sql_ast = ["DELETE", None, from_ast]
            if self.conditions:
                sql_ast.append(["WHERE"] + self.conditions)
        elif not force_in and self.dialect == "MySQL":
            sql_ast = ["DELETE", tableref.alias, from_ast]
            if self.conditions:
                sql_ast.append(["WHERE"] + self.conditions)
        else:
            delete_from_ast = ["FROM", [None, "TABLE", entity._table_]]
            if len(entity._pk_columns_) == 1:
                inner_expr = expr_monad.getsql()
                outer_expr = ["COLUMN", None, entity._pk_columns_[0]]
            elif self.rowid_support:
                inner_expr = [["COLUMN", tableref.alias, "ROWID"]]
                outer_expr = ["COLUMN", None, "ROWID"]
            elif self.row_value_syntax:
                inner_expr = expr_monad.getsql()
                outer_expr = ["ROW"] + [
                    ["COLUMN", None, column_name] for column_name in entity._pk_columns_
                ]
            else:
                throw(NotImplementedError)
            subquery_ast = ["SELECT", ["ALL"] + inner_expr, from_ast]
            if self.conditions:
                subquery_ast.append(["WHERE"] + self.conditions)
            delete_where_ast = ["WHERE", ["IN", outer_expr, subquery_ast]]
            sql_ast = ["DELETE", None, delete_from_ast, delete_where_ast]
        return sql_ast

    def get_used_attrs(self):
        if (
            isinstance(self.expr_type, EntityMeta)
            and not self.aggregated
            and not self.optimize
        ):
            return self.tableref.used_attrs
        return ()

    def without_order(self):
        self = self.deepcopy()
        self.order = []
        return self

    def order_by_numbers(self, numbers):
        if 0 in numbers:
            throw(ValueError, "Numeric arguments of order_by() method must be non-zero")
        self = self.deepcopy()
        order = self.order = self.order[:]  # only order will be changed
        expr_monads = self.expr_monads
        new_order = []
        for i in numbers:
            try:
                monad = expr_monads[abs(i) - 1]
            except IndexError:
                if len(expr_monads) > 1:
                    throw(
                        IndexError,
                        "Invalid index of order_by() method: %d "
                        "(query result is list of tuples with only %d elements in each)"
                        % (i, len(expr_monads)),
                    )
                else:
                    throw(
                        IndexError,
                        "Invalid index of order_by() method: %d "
                        "(query result is single list of elements and has only one 'column')"
                        % i,
                    )
            for pos in monad.orderby_columns:
                new_order.append((i < 0 and ["DESC", ["VALUE", pos]]) or ["VALUE", pos])
        order[:0] = new_order
        return self

    def order_by_attributes(self, attrs):
        entity = self.expr_type
        if not isinstance(entity, EntityMeta):
            throw(
                NotImplementedError,
                "Ordering by attributes is limited to queries which return simple list of objects. "
                "Try use other forms of ordering (by tuple element numbers or by full-blown lambda expr).",
            )
        self = self.deepcopy()
        order = self.order = self.order[:]  # only order will be changed
        alias = self.alias
        new_order = []
        for x in attrs:
            if isinstance(x, DescWrapper):
                attr = x.attr
                desc_wrapper = lambda column: ["DESC", column]
            elif isinstance(x, Attribute):
                attr = x
                desc_wrapper = lambda column: column
            else:
                assert False, x  # pragma: no cover
            if entity._adict_.get(attr.name) is not attr:
                throw(
                    TypeError,
                    "Attribute %s does not belong to entity %s"
                    % (attr, entity.__name__),
                )
            if attr.is_collection:
                throw(
                    TypeError,
                    "Collection attribute %s cannot be used for ordering" % attr,
                )
            for column in attr.columns:
                new_order.append(desc_wrapper(["COLUMN", alias, column]))
        order[:0] = new_order
        return self

    def apply_kwfilters(self, filterattrs, original_names=False):
        self = self.deepcopy()
        with self:
            if original_names:
                object_monad = self.tree.generators[0].iter.monad
                assert isinstance(object_monad.type, EntityMeta)
            else:
                object_monad = self.tree.elt.monad
                if not isinstance(object_monad.type, EntityMeta):
                    throw(
                        TypeError,
                        "Keyword arguments are not allowed when query result is not entity objects",
                    )

            monads = []
            none_monad = NoneMonad()
            for attr, id, is_none in filterattrs:
                attr_monad = object_monad.getattr(attr.name)
                if is_none:
                    monads.append(CmpMonad("is", attr_monad, none_monad))
                else:
                    param_monad = ParamMonad.new(attr.py_type, (id, None, None))
                    monads.append(CmpMonad("==", attr_monad, param_monad))
            for m in monads:
                self.conditions.extend(m.getsql())
            return self

    def apply_lambda(
        self,
        func_id,
        filter_num,
        order_by,
        func_ast,
        argnames,
        original_names,
        extractors,
        vars,
        vartypes,
    ):
        self = self.deepcopy()
        func_ast = copy_ast(func_ast)  # func_ast = deepcopy(func_ast)
        self.code_key = func_id
        self.filter_num = filter_num
        self.extractors.update(extractors)
        self.vars = vars
        self.vartypes = self.vartypes.copy()  # make HashableDict mutable again
        self.vartypes.update(vartypes)

        if not original_names:
            assert argnames
            namespace = {name: monad for name, monad in zip(argnames, self.expr_monads)}
        elif argnames:
            namespace = {name: self.namespace[name] for name in argnames}
        else:
            namespace = None
        if namespace is not None:
            self.namespace_stack.append(namespace)
        try:
            with self:
                self.dispatch(func_ast)
                if isinstance(func_ast, ast.Tuple):
                    nodes = func_ast.elts
                else:
                    nodes = (func_ast,)
                if order_by:
                    self.inside_order_by = True
                    new_order = []
                    for node in nodes:
                        monad = node.monad.to_single_cell_value()
                        if isinstance(monad, SetMixin):
                            t = monad.type.item_type
                            if isinstance(type(t), type):
                                t = t.__name__
                            throw(
                                TranslationError,
                                "Set of %s (%s) cannot be used for ordering"
                                % (t, ast2src(node)),
                            )
                        new_order.extend(node.monad.getsql())
                    self.order[:0] = new_order
                    self.inside_order_by = False
                else:
                    for node in nodes:
                        monad = node.monad
                        if isinstance(monad, AndMonad):
                            cond_monads = monad.operands
                        else:
                            cond_monads = [monad]
                        for m in cond_monads:
                            if not m.aggregated:
                                self.conditions.extend(m.getsql())
                            else:
                                self.having_conditions.extend(m.getsql())
                self.vars = None
                return self
        finally:
            if namespace is not None:
                ns = self.namespace_stack.pop()
                assert ns is namespace

    def preGeneratorExp(self, node):
        translator_cls = self.__class__
        try:
            subtranslator = translator_cls(node, self)
        except UseAnotherTranslator:
            assert False
        return QuerySetMonad(subtranslator)

    def postExpr(self, node):
        return node.value.monad

    def preCompare(self, node):
        monads = []
        ops = zip(node.ops, node.comparators)
        left = node.left
        self.dispatch(left)
        # op: '<' | '>' | '=' | '>=' | '<=' | '<>' | '!=' | '=='
        #         | 'in' | 'not in' | 'is' | 'is not'
        for op_node, right in ops:
            for op, cls in operator_mapping.items():
                if isinstance(op_node, cls):
                    break
            else:
                assert False, str(op_node)
            self.dispatch(right)
            if op.endswith("in"):
                monad = right.monad.contains(left.monad, op == "not in")
            else:
                monad = left.monad.cmp(op, right.monad)
            if not hasattr(monad, "aggregated"):
                monad.aggregated = getattr(left.monad, "aggregated", False) or getattr(
                    right.monad, "aggregated", False
                )
            if not hasattr(monad, "nogroup"):
                monad.nogroup = getattr(left.monad, "nogroup", False) or getattr(
                    right.monad, "nogroup", False
                )
            if monad.aggregated and monad.nogroup:
                throw(
                    TranslationError,
                    "Too complex aggregation, expressions cannot be combined: {EXPR}",
                )
            monads.append(monad)
            left = right
        if len(monads) == 1:
            return monads[0]
        return AndMonad(monads)

    def postConstant(self, node):
        value = node.value
        if type(value) is frozenset:
            value = tuple(sorted(value))
        return ConstMonad.new(value)

    def postNameConstant(self, node):  # Python <= 3.7
        return ConstMonad.new(node.value)

    def postNum(self, node):  # Python <= 3.7
        return ConstMonad.new(node.n)

    def postStr(self, node):  # Python <= 3.7
        return ConstMonad.new(node.s)

    def postBytes(self, node):  # Python <= 3.7
        return ConstMonad.new(node.s)

    def postList(self, node):
        return ListMonad([item.monad for item in node.elts])

    def postTuple(self, node):
        return ListMonad([item.monad for item in node.elts])

    def postName(self, node):
        monad = self.resolve_name(node.id)
        assert monad is not None
        return monad

    def resolve_name(self, name):
        if name not in self.namespace:
            throw(
                TranslationError,
                "Name %s is not found in %s" % (name, self.namespace),
            )

        monad = self.namespace[name]
        if not isinstance(monad, Monad):
            raise AssertionError(
                "Name `%s` was expected to be resolved to a monad. Got: %r"
                % (name, monad)
            )

        if monad.translator is not self:
            monad.translator.sqlquery.used_from_subquery = True
        return monad

    def postAdd(self, node):
        return node.left.monad + node.right.monad

    def postSub(self, node):
        return node.left.monad - node.right.monad

    def postMult(self, node):
        return node.left.monad * node.right.monad

    def postMatMult(self, node):
        throw(NotImplementedError)

    def postDiv(self, node):
        return node.left.monad / node.right.monad

    def postFloorDiv(self, node):
        return node.left.monad // node.right.monad

    def postMod(self, node):
        return node.left.monad % node.right.monad

    def postLShift(self, node):
        throw(NotImplementedError)

    def postRShift(self, node):
        throw(NotImplementedError)

    def postPow(self, node):
        return node.left.monad**node.right.monad

    def postUSub(self, node):
        return -node.operand.monad

    def postAttribute(self, node):
        return node.value.monad.getattr(node.attr)

    def postAnd(self, node):
        return AndMonad([expr.monad for expr in node.values])

    def postOr(self, node):
        return OrMonad([expr.monad for expr in node.values])

    def postBitOr(self, node):
        return node.left.monad | node.right.monad

    def postBitAnd(self, node):
        return node.left.monad & node.right.monad

    def postBitXor(self, node):
        return node.left.monad ^ node.right.monad

    def postNot(self, node):
        return node.operand.monad.negate()

    def preCall(self, node):
        for arg in node.args:
            if isinstance(arg, ast.Starred):
                throw(NotImplementedError, "%s is not supported" % ast2src(arg))
        if any(kwarg.arg is None for kwarg in node.keywords):
            throw(
                NotImplementedError, "**%s is not supported" % ast2src(node.dstar_args)
            )
        func_node = node.func
        if isinstance(func_node, ast.Call):
            if isinstance(func_node.func, ast.Name) and func_node.func.id == "getattr":
                return
        if not isinstance(func_node, (ast.Name, ast.Attribute)):
            throw(NotImplementedError)
        if len(node.args) > 1:
            return
        if not node.args:
            return
        arg = node.args[0]
        if isinstance(arg, ast.GeneratorExp):
            self.dispatch(func_node)
            func_monad = func_node.monad
            self.dispatch(arg)
            query_set_monad = arg.monad
            return func_monad(query_set_monad)
        if not isinstance(arg, ast.Lambda):
            return
        lambda_expr = arg
        self.dispatch(func_node)
        method_monad = func_node.monad
        if not isinstance(method_monad, MethodMonad):
            throw(NotImplementedError)
        entity_monad = method_monad.parent
        if not isinstance(entity_monad, (EntityMonad, AttrSetMonad)):
            throw(NotImplementedError)
        entity = entity_monad.type.item_type
        method_name = method_monad.attrname
        if method_name not in ("select", "filter", "exists"):
            throw(TypeError)
        if len(lambda_expr.args.args) != 1:
            throw(TypeError)
        if lambda_expr.args.kw_defaults:
            throw(TypeError)
        if lambda_expr.args.kwarg:
            throw(TypeError)
        if lambda_expr.args.kwonlyargs:
            throw(TypeError)
        if lambda_expr.args.posonlyargs:
            throw(TypeError)
        iter_name = lambda_expr.args.args[0].arg
        cond_expr = lambda_expr.body
        name_ast = ast.Name(entity.__name__, ast.Load())
        name_ast.monad = entity_monad
        for_expr = ast.comprehension(
            ast.Name(iter_name, ast.Store()), name_ast, [cond_expr], False
        )
        inner_expr = ast.GeneratorExp(ast.Name(iter_name, ast.Load()), [for_expr])
        translator_cls = self.__class__
        try:
            subtranslator = translator_cls(inner_expr, self)
        except UseAnotherTranslator:
            assert False
        monad = QuerySetMonad(subtranslator)
        if method_name == "exists":
            monad = monad.nonzero()
        return monad

    def postCall(self, node):
        args = []
        kwargs = {}
        for arg in node.args:
            if isinstance(arg, ast.Starred):
                throw(NotImplementedError, arg.src)
            args.append(arg.monad)
        for kw in node.keywords:
            if kw.arg is None:
                throw(NotImplementedError, kw.src)
            kwargs[kw.arg] = kw.value.monad
        func_monad = node.func.monad
        return func_monad(*args, **kwargs)

    def postkeyword(self, node):
        pass  # this node will be processed by postCall

    def postSubscript(self, node):
        assert isinstance(node.ctx, ast.Load)
        sub = node.slice
        if isinstance(sub, ast.Tuple):
            for x in sub.elts:
                if isinstance(x, ast.Slice):
                    throw(TypeError)
            key = ListMonad([item.monad for item in sub.elts])
            return node.value.monad[key]
        if isinstance(sub, ast.Slice):
            start, stop, step = sub.lower, sub.upper, sub.step
            if start is not None:
                start = start.monad
            if isinstance(start, NoneMonad):
                start = None
            if stop is not None:
                stop = stop.monad
            if isinstance(stop, NoneMonad):
                stop = None
            if step is not None:
                step = step.monad
            if isinstance(step, NoneMonad):
                step = None
            return node.value.monad[start:stop:step]
        return node.value.monad[sub.monad]

    def postSlice(self, node):
        return None

    def postIndex(self, node):
        return node.value.monad

    def postIfExp(self, node):
        test_monad, then_monad, else_monad = (
            node.test.monad,
            node.body.monad,
            node.orelse.monad,
        )
        if test_monad.type is not bool:
            test_monad = test_monad.nonzero()
        result_type = coerce_types(then_monad.type, else_monad.type)
        test_sql, then_sql, else_sql = (
            test_monad.getsql()[0],
            then_monad.getsql(),
            else_monad.getsql(),
        )
        if len(then_sql) == 1:
            then_sql, else_sql = then_sql[0], else_sql[0]
        elif not self.row_value_syntax:
            throw(NotImplementedError)
        else:
            then_sql, else_sql = ["ROW"] + then_sql, ["ROW"] + else_sql
        expr = ["CASE", None, [[test_sql, then_sql]], else_sql]
        result = ExprMonad.new(
            result_type,
            expr,
            nullable=test_monad.nullable or then_monad.nullable or else_monad.nullable,
        )
        result.aggregated = (
            test_monad.aggregated or then_monad.aggregated or else_monad.aggregated
        )
        return result

    def postJoinedStr(self, node):
        nullable = False
        sql = ["CONCAT"]
        for item in node.values:
            monad = item.monad
            if not isinstance(monad, StringMixin):
                monad = monad.to_str()
            if monad.nullable:
                nullable = True
            sql.append(monad.getsql()[0])
        return StringExprMonad(str, sql, nullable=nullable)

    def postFormattedValue(self, node):
        if node.format_spec is not None:
            throw(
                NotImplementedError,
                "You cannot set width and precision for f-string expression in query",
            )
        if node.conversion not in (-1, ord("s")):
            throw(
                NotImplementedError,
                "You cannot specify conversion type for f-string expression in query",
            )
        return node.value.monad


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
    if (
        result_type in numeric_types
        and bool in (m1.type, m2.type)
        and (result_type is not bool or not for_comparison)
    ):
        translator = m1.translator
        if translator.dialect == "PostgreSQL":
            if result_type is bool:
                result_type = int
            if m1.type is bool:
                new_m1 = NumericExprMonad(
                    int, ["TO_INT", m1.getsql()[0]], nullable=m1.nullable
                )
                new_m1.aggregated = m1.aggregated
                m1 = new_m1
            if m2.type is bool:
                new_m2 = NumericExprMonad(
                    int, ["TO_INT", m2.getsql()[0]], nullable=m2.nullable
                )
                new_m2.aggregated = m2.aggregated
                m2 = new_m2
    return result_type, m1, m2


max_alias_length = 30


class SqlQuery:
    def __init__(self, translator, parent_sqlquery=None, left_join=False):
        self.translator = translator
        self.parent_sqlquery = parent_sqlquery
        self.left_join = left_join
        self.from_ast = ["LEFT_JOIN" if left_join else "FROM"]
        self.conditions = []
        self.outer_conditions = []
        self.tablerefs = {}
        if parent_sqlquery is None:
            self.alias_counters = {}
            self.expr_counter = IntegerGenerator(1)
        else:
            self.alias_counters = parent_sqlquery.alias_counters.copy()
            self.expr_counter = parent_sqlquery.expr_counter
        self.used_from_subquery = False

    def get_tableref(self, name_path):
        tableref = self.tablerefs.get(name_path)
        parent_sqlquery = self.parent_sqlquery
        if tableref is None and parent_sqlquery:
            tableref = parent_sqlquery.get_tableref(name_path)
            if tableref is not None:
                parent_sqlquery.used_from_subquery = True
        return tableref

    def add_tableref(self, name_path, parent_tableref, attr):
        assert name_path not in self.tablerefs
        if parent_tableref.sqlquery is not self:
            parent_tableref.sqlquery.used_from_subquery = True
        tableref = JoinedTableRef(self, name_path, parent_tableref, attr)
        self.tablerefs[name_path] = tableref
        return tableref

    def make_alias(self, name):
        name = name[: max_alias_length - 3].lower()
        i = self.alias_counters.setdefault(name, 0) + 1
        alias = name if i == 1 and name != "t" else "%s-%d" % (name, i)
        self.alias_counters[name] = i
        return alias

    def join_table(self, parent_alias, alias, table_name, join_cond):
        new_item = [alias, "TABLE", table_name, join_cond]
        from_ast = self.from_ast
        for i in range(1, len(from_ast)):
            if from_ast[i][0] == parent_alias:
                for j in range(i + 1, len(from_ast)):
                    if len(from_ast[j]) < 4:  # item without join condition
                        from_ast.insert(j, new_item)
                        return
        from_ast.append(new_item)


class TableRef:
    def __init__(self, sqlquery, name, entity):
        self.sqlquery = sqlquery
        self.alias = sqlquery.make_alias(name)
        self.name_path = self.alias
        self.entity = entity
        self.joined = False
        self.can_affect_distinct = True
        self.used_attrs = set()

    def make_join(self, pk_only=False):
        entity = self.entity
        if not self.joined:
            sqlquery = self.sqlquery
            sqlquery.from_ast.append([self.alias, "TABLE", entity._table_])
            if entity._discriminator_attr_:
                discr_criteria = entity._construct_discriminator_criteria_(self.alias)
                assert discr_criteria is not None
                sqlquery.conditions.append(discr_criteria)
            self.joined = True
        return self.alias, entity._pk_columns_


class ExprTableRef(TableRef):
    def __init__(self, sqlquery, name, subquery_ast, expr_names, expr_aliases):
        TableRef.__init__(self, sqlquery, name, None)
        self.subquery_ast = subquery_ast
        self.expr_names = expr_names
        self.expr_aliases = expr_aliases

    def make_join(self, pk_only=False):
        assert self.subquery_ast[0] == "SELECT"
        if not self.joined:
            sqlquery = self.sqlquery
            sqlquery.from_ast.append([self.alias, "SELECT", self.subquery_ast[1:]])
            self.joined = True
        return self.alias, None


class StarTableRef(TableRef):
    def __init__(self, sqlquery, name, entity, subquery_ast):
        TableRef.__init__(self, sqlquery, name, entity)
        self.subquery_ast = subquery_ast

    def make_join(self, pk_only=False):
        entity = self.entity
        assert self.subquery_ast[0] == "SELECT"
        if not self.joined:
            sqlquery = self.sqlquery
            sqlquery.from_ast.append([self.alias, "SELECT", self.subquery_ast[1:]])
            if entity._discriminator_attr_:  # ???
                discr_criteria = entity._construct_discriminator_criteria_(self.alias)
                assert discr_criteria is not None
                sqlquery.conditions.append(discr_criteria)
            self.joined = True
        return self.alias, entity._pk_columns_


class ExprJoinedTableRef:
    def __init__(self, sqlquery, parent_tableref, parent_columns, name, entity):
        self.sqlquery = sqlquery
        self.parent_tableref = parent_tableref
        self.parent_columns = parent_columns
        self.name = self.name_path = name
        self.entity = entity
        self.alias = None
        self.joined = False
        self.can_affect_distinct = False
        self.used_attrs = set()

    def make_join(self, pk_only=False):
        entity = self.entity
        if self.joined:
            return self.alias, self.pk_columns
        sqlquery = self.sqlquery
        parent_alias, left_pk_columns = self.parent_tableref.make_join()
        if pk_only:
            self.alias = parent_alias
            self.pk_columns = self.parent_columns
            return self.alias, self.pk_columns
        self.alias = sqlquery.make_alias(self.name)
        self.pk_columns = entity._pk_columns_
        join_cond = join_tables(
            parent_alias, self.alias, self.parent_columns, self.pk_columns
        )
        sqlquery.join_table(parent_alias, self.alias, entity._table_, join_cond)
        self.joined = True
        return self.alias, self.pk_columns


class JoinedTableRef:
    def __init__(self, sqlquery, name_path, parent_tableref, attr):
        self.sqlquery = sqlquery
        self.name_path = name_path
        self.var_name = name_path if is_ident(name_path) else None
        self.alias = None
        self.optimized = None
        self.parent_tableref = parent_tableref
        self.attr = attr
        self.entity = attr.py_type
        assert isinstance(self.entity, EntityMeta)
        self.joined = False
        self.can_affect_distinct = False
        self.used_attrs = set()

    def make_join(self, pk_only=False):
        entity = self.entity
        if self.joined:
            if pk_only or not self.optimized:
                return self.alias, self.pk_columns
        sqlquery = self.sqlquery
        attr = self.attr
        parent_pk_only = attr.pk_offset is not None or attr.is_collection
        parent_alias, left_pk_columns = self.parent_tableref.make_join(parent_pk_only)
        pk_columns = entity._pk_columns_
        if not attr.is_collection:
            if not attr.columns:
                # one-to-one relationship with foreign key column on the right side
                reverse = attr.reverse
                assert reverse.columns and not reverse.is_collection
                rentity = reverse.entity
                pk_columns = rentity._pk_columns_
                alias = sqlquery.make_alias(self.var_name or rentity.__name__)
                join_cond = join_tables(
                    parent_alias, alias, left_pk_columns, reverse.columns
                )
            else:
                # one-to-one or many-to-one relationship with foreign key column on the left side
                if attr.pk_offset is not None:
                    offset = attr.pk_columns_offset
                    left_columns = left_pk_columns[offset : offset + len(attr.columns)]
                else:
                    left_columns = attr.columns
                if pk_only:
                    self.alias = parent_alias
                    self.pk_columns = left_columns
                    self.optimized = True
                    # tableref.joined = True
                    return parent_alias, left_columns
                alias = sqlquery.make_alias(self.var_name or entity.__name__)
                join_cond = join_tables(parent_alias, alias, left_columns, pk_columns)
        elif not attr.reverse.is_collection:
            # many-to-one relationship
            alias = sqlquery.make_alias(self.var_name or entity.__name__)
            join_cond = join_tables(
                parent_alias, alias, left_pk_columns, attr.reverse.columns
            )
        else:
            # many-to-many relationship
            right_m2m_columns = attr.reverse_columns if attr.symmetric else attr.columns
            if not self.joined:
                m2m_table = attr.table
                m2m_alias = sqlquery.make_alias("t")
                reverse_columns = (
                    attr.columns if attr.symmetric else attr.reverse.columns
                )
                m2m_join_cond = join_tables(
                    parent_alias, m2m_alias, left_pk_columns, reverse_columns
                )
                sqlquery.join_table(parent_alias, m2m_alias, m2m_table, m2m_join_cond)
                if pk_only:
                    self.alias = m2m_alias
                    self.pk_columns = right_m2m_columns
                    self.optimized = True
                    self.joined = True
                    return m2m_alias, self.pk_columns
            elif self.optimized:
                assert not pk_only
                m2m_alias = self.alias
            alias = sqlquery.make_alias(self.var_name or entity.__name__)
            join_cond = join_tables(m2m_alias, alias, right_m2m_columns, pk_columns)
        if not pk_only and entity._discriminator_attr_:
            discr_criteria = entity._construct_discriminator_criteria_(alias)
            assert discr_criteria is not None
            join_cond.append(discr_criteria)

        translator = self.sqlquery.translator.root_translator
        if (
            translator.optimize == self.name_path
            and translator.from_optimized
            and self.sqlquery is translator.sqlquery
        ):
            pass
        else:
            sqlquery.join_table(parent_alias, alias, entity._table_, join_cond)
        self.alias = alias
        self.pk_columns = pk_columns
        self.optimized = False
        self.joined = True
        return self.alias, pk_columns


def wrap_monad_method(cls_name, func):
    overrider_name = "%s_%s" % (cls_name, func.__name__)

    def wrapper(monad, *args, **kwargs):
        method = getattr(monad.translator, overrider_name, func)
        return method(monad, *args, **kwargs)

    return update_wrapper(wrapper, func)


class MonadMeta(type):
    def __new__(meta, cls_name, bases, cls_dict):
        for name, func in cls_dict.items():
            if not isinstance(func, types.FunctionType):
                continue
            if name in ("__new__", "__init__"):
                continue
            cls_dict[name] = wrap_monad_method(cls_name, func)
        return super().__new__(meta, cls_name, bases, cls_dict)


class MonadMixin(metaclass=MonadMeta):
    pass


class Monad(metaclass=MonadMeta):
    disable_distinct = False
    disable_ordering = False

    def __init__(self, type, nullable=True):
        self.node = None
        self.translator = local.translator
        self.type = type
        self.nullable = nullable
        self.mixin_init()

    def mixin_init(self):
        pass

    def to_single_cell_value(self):
        return self

    def cmp(self, op, monad2):
        return CmpMonad(op, self, monad2)

    def contains(self, item, not_in=False):
        throw(TypeError)

    def nonzero(self):
        return CmpMonad("is not", self, NoneMonad())

    def negate(self):
        return NotMonad(self)

    def getattr(self, attrname):
        try:
            property_method = getattr(self, "attr_" + attrname)
        except AttributeError:
            if not hasattr(self, "call_" + attrname):
                throw(
                    AttributeError,
                    "%r object has no attribute %r: {EXPR}"
                    % (type2str(self.type), attrname),
                )
            return MethodMonad(self, attrname)
        return property_method()

    def len(self):
        throw(TypeError)

    def count(self, distinct=None):
        distinct = distinct_from_monad(distinct, default=True)
        translator = self.translator
        if self.aggregated:
            throw(
                TranslationError, "Aggregated functions cannot be nested. Got: {EXPR}"
            )
        expr = self.getsql()

        if self.type is bool:
            expr = ["CASE", None, [[expr[0], ["VALUE", 1]]], ["VALUE", None]]
            distinct = None
        elif len(expr) == 1:
            expr = expr[0]
        elif translator.dialect == "PostgreSQL":
            row = ["ROW"] + expr
            expr = ["CASE", None, [[["IS_NULL", row], ["VALUE", None]]], row]
        # elif translator.dialect == 'PostgreSQL':  # another way
        #     alias, pk_columns = monad.tableref.make_join(pk_only=False)
        #     expr = [ 'COLUMN', alias, 'ctid' ]
        elif translator.dialect in ("SQLite", "Oracle"):
            alias, pk_columns = self.tableref.make_join(pk_only=False)
            expr = ["COLUMN", alias, "ROWID"]
        # elif translator.row_value_syntax == True:  # doesn't work in MySQL
        #     expr = ['ROW'] + expr
        else:
            throw(
                NotImplementedError,
                "%s database provider does not support entities "
                "with composite primary keys inside aggregate functions. Got: {EXPR}"
                % translator.dialect,
            )
        result = ExprMonad.new(int, ["COUNT", distinct, expr], nullable=False)
        result.aggregated = True
        return result

    def aggregate(self, func_name, distinct=None, sep=None):
        distinct = distinct_from_monad(distinct)
        translator = self.translator
        if self.aggregated:
            throw(
                TranslationError, "Aggregated functions cannot be nested. Got: {EXPR}"
            )
        expr_type = self.type
        # if isinstance(expr_type, SetType): expr_type = expr_type.item_type
        if func_name in ("SUM", "AVG"):
            if expr_type not in numeric_types:
                if expr_type is Json:
                    self = self.to_real()
                else:
                    throw(
                        TypeError,
                        "Function '%s' expects argument of numeric type, got %r in {EXPR}"
                        % (func_name, type2str(expr_type)),
                    )
        elif func_name in ("MIN", "MAX"):
            if expr_type not in comparable_types:
                throw(
                    TypeError,
                    "Function '%s' cannot be applied to type %r in {EXPR}"
                    % (func_name, type2str(expr_type)),
                )
        elif func_name == "GROUP_CONCAT":
            if isinstance(expr_type, EntityMeta) and expr_type._pk_is_composite_:
                throw(
                    TypeError,
                    "`group_concat` cannot be used with entity with composite primary key",
                )
        else:
            assert False  # pragma: no cover
        expr = self.getsql()
        if len(expr) == 1:
            expr = expr[0]
        elif translator.row_value_syntax:
            expr = ["ROW"] + expr
        else:
            throw(
                NotImplementedError,
                "%s database provider does not support entities "
                "with composite primary keys inside aggregate functions. Got: {EXPR} "
                "(you can suggest us how to write SQL for this query)"
                % translator.dialect,
            )
        if func_name == "AVG":
            result_type = float
        elif func_name == "GROUP_CONCAT":
            result_type = str
        else:
            result_type = expr_type
        if distinct is None:
            distinct = getattr(self, "forced_distinct", False) and func_name in (
                "SUM",
                "AVG",
            )
        aggr_ast = [func_name, distinct, expr]
        if func_name == "GROUP_CONCAT":
            if sep is not None:
                aggr_ast.append(["VALUE", sep])
        result = ExprMonad.new(result_type, aggr_ast, nullable=True)
        result.aggregated = True
        return result

    def __call__(self, *args, **kwargs):
        throw(TypeError)

    def __getitem__(self, key):
        throw(TypeError)

    def __add__(self, monad2):
        throw(TypeError)

    def __sub__(self, monad2):
        throw(TypeError)

    def __mul__(self, monad2):
        throw(TypeError)

    def __truediv__(self, monad2):
        throw(TypeError)

    def __floordiv__(self, monad2):
        throw(TypeError)

    def __pow__(self, monad2):
        throw(TypeError)

    def __neg__(self):
        throw(TypeError)

    def __or__(self, monad2):
        throw(TypeError)

    def __and__(self, monad2):
        throw(TypeError)

    def __xor__(self, monad2):
        throw(TypeError)

    def abs(self):
        throw(TypeError)

    def cast_from_json(self, type):
        assert False, self

    def to_int(self):
        return NumericExprMonad(
            int, ["TO_INT", self.getsql()[0]], nullable=self.nullable
        )

    def to_str(self):
        return StringExprMonad(
            str, ["TO_STR", self.getsql()[0]], nullable=self.nullable
        )

    def to_real(self):
        return NumericExprMonad(
            float, ["TO_REAL", self.getsql()[0]], nullable=self.nullable
        )


def distinct_from_monad(distinct, default=None):
    if distinct is None:
        return default
    if isinstance(distinct, NumericConstMonad) and isinstance(distinct.value, bool):
        return distinct.value
    throw(
        TypeError,
        "`distinct` value should be True or False. Got: %s" % ast2src(distinct.node),
    )


class RawSQLMonad(Monad):
    def __init__(self, rawtype, varkey, nullable=True):
        if rawtype.result_type is None:
            type = rawtype
        else:
            type = normalize_type(rawtype.result_type)
        Monad.__init__(self, type, nullable=nullable)
        self.rawtype = rawtype
        self.varkey = varkey

    def contains(self, item, not_in=False):
        translator = self.translator
        expr = item.getsql()
        if len(expr) == 1:
            expr = expr[0]
        elif translator.row_value_syntax == True:
            expr = ["ROW"] + expr
        else:
            throw(
                TranslationError,
                "%s database provider does not support tuples. Got: {EXPR} "
                % translator.dialect,
            )
        op = "NOT_IN" if not_in else "IN"
        sql = [op, expr, self.getsql()]
        return BoolExprMonad(sql, nullable=item.nullable)

    def nonzero(self):
        return self

    def getsql(self, sqlquery=None):
        provider = self.translator.database.provider
        rawtype = self.rawtype
        result = []
        types = enumerate(rawtype.types)
        for item in self.rawtype.items:
            if isinstance(item, str):
                result.append(item)
            else:
                expr, code = item
                i, param_type = next(types)
                param_converter = provider.get_converter_by_py_type(param_type)
                result.append(["PARAM", (self.varkey, i, None), param_converter])
        return [["RAWSQL", result]]


typeerror_re_1 = re.compile(
    r"\(\) takes (no|(?:exactly|at (?:least|most)))(?: (\d+))? arguments \((\d+) given\)"
)
typeerror_re_2 = re.compile(
    r"\(\) takes from (\d+) to (\d+) positional arguments but (\d+) were given"
)


def reraise_improved_typeerror(exc, func_name, orig_func_name):
    if not exc.args:
        throw(exc)
    msg = exc.args[0]
    if PY310:
        dot_index = msg.find(".") + 1
        msg = msg[dot_index:]
    if not msg.startswith(func_name):
        throw(exc)
    msg = msg[len(func_name) :]

    match = typeerror_re_1.match(msg)
    if match:
        what, takes, given = match.groups()
        takes, given = int(takes), int(given)
        if takes:
            what = "%s %d" % (what, takes - 1)
        plural = "s" if takes > 2 else ""
        new_msg = "%s() takes %s argument%s (%d given)" % (
            orig_func_name,
            what,
            plural,
            given - 1,
        )
        exc.args = (new_msg,)
        throw(exc)

    match = typeerror_re_2.match(msg)
    if match:
        start, end, given = match.groups()
        start, end, given = int(start) - 1, int(end) - 1, int(given) - 1
        if not start:
            plural = "s" if end > 1 else ""
            new_msg = "%s() takes at most %d argument%s (%d given)" % (
                orig_func_name,
                end,
                plural,
                given,
            )
        else:
            new_msg = "%s() takes from %d to %d arguments (%d given)" % (
                orig_func_name,
                start,
                end,
                given,
            )
        exc.args = (new_msg,)
        throw(exc)

    exc.args = (orig_func_name + msg,)
    throw(exc)


def raise_forgot_parentheses(monad):
    assert monad.type == "METHOD"
    throw(
        TranslationError,
        "You seems to forgot parentheses after %s" % ast2src(monad.node),
    )


class MethodMonad(Monad):
    def __init__(self, parent, attrname):
        Monad.__init__(self, "METHOD", nullable=False)
        self.parent = parent
        self.attrname = attrname

    def getattr(self, attrname):
        raise_forgot_parentheses(self)

    def __call__(self, *args, **kwargs):
        method = getattr(self.parent, "call_" + self.attrname)
        try:
            return method(*args, **kwargs)
        except TypeError as exc:
            reraise_improved_typeerror(exc, method.__name__, self.attrname)

    def contains(self, item, not_in=False):
        raise_forgot_parentheses(self)

    def nonzero(self):
        raise_forgot_parentheses(self)

    def negate(self):
        raise_forgot_parentheses(self)

    def aggregate(self, func_name, distinct=None, sep=None):
        raise_forgot_parentheses(self)

    def __getitem__(self, key):
        raise_forgot_parentheses(self)

    def __add__(self, monad2):
        raise_forgot_parentheses(self)

    def __sub__(self, monad2):
        raise_forgot_parentheses(self)

    def __mul__(self, monad2):
        raise_forgot_parentheses(self)

    def __truediv__(self, monad2):
        raise_forgot_parentheses(self)

    def __floordiv__(self, monad2):
        raise_forgot_parentheses(self)

    def __pow__(self, monad2):
        raise_forgot_parentheses(self)

    def __neg__(self):
        raise_forgot_parentheses(self)

    def abs(self):
        raise_forgot_parentheses(self)


class EntityMonad(Monad):
    def __init__(self, entity):
        Monad.__init__(self, SetType(entity))
        translator = self.translator
        if translator.database is None:
            translator.database = entity._database_
        elif translator.database is not entity._database_:
            throw(
                TranslationError,
                "All entities in a query must belong to the same database",
            )

    def __getitem__(self, *args):
        throw(NotImplementedError)


class ListMonad(Monad):
    def __init__(self, items):
        Monad.__init__(self, tuple(item.type for item in items))
        self.items = items

    def contains(self, x, not_in=False):
        if isinstance(x.type, SetType):
            throw(
                TypeError,
                "Type of `%s` is '%s'. Expression `{EXPR}` is not supported"
                % (ast2src(x.node), type2str(x.type)),
            )
        for item in self.items:
            check_comparable(x, item)
        left_sql = x.getsql()
        if len(left_sql) == 1:
            if not_in:
                sql = [
                    "NOT_IN",
                    left_sql[0],
                    [item.getsql()[0] for item in self.items],
                ]
            else:
                sql = ["IN", left_sql[0], [item.getsql()[0] for item in self.items]]
        elif not_in:
            sql = sqland(
                [
                    sqlor([["NE", a, b] for a, b in zip(left_sql, item.getsql())])
                    for item in self.items
                ]
            )
        else:
            sql = sqlor(
                [
                    sqland([["EQ", a, b] for a, b in zip(left_sql, item.getsql())])
                    for item in self.items
                ]
            )
        return BoolExprMonad(
            sql, nullable=x.nullable or any(item.nullable for item in self.items)
        )

    def getsql(self, sqlquery=None):
        return [["ROW"] + [item.getsql()[0] for item in self.items]]


class BufferMixin(MonadMixin):
    pass


class UuidMixin(MonadMixin):
    pass


_binop_errmsg = (
    "Unsupported operand types %r and %r for operation %r in expression: {EXPR}"
)


def make_numeric_binop(op, sqlop):
    def numeric_binop(monad, monad2):
        if isinstance(monad2, (AttrSetMonad, NumericSetExprMonad)):
            return NumericSetExprMonad(op, sqlop, monad, monad2)
        if monad2.type == "METHOD":
            raise_forgot_parentheses(monad2)
        result_type, monad, monad2 = coerce_monads(monad, monad2)
        if result_type is None:
            throw(
                TypeError,
                _binop_errmsg % (type2str(monad.type), type2str(monad2.type), op),
            )
        left_sql = monad.getsql()[0]
        right_sql = monad2.getsql()[0]
        return NumericExprMonad(result_type, [sqlop, left_sql, right_sql])

    numeric_binop.__name__ = sqlop
    return numeric_binop


class NumericMixin(MonadMixin):
    def mixin_init(self):
        assert self.type in numeric_types, self.type

    __add__ = make_numeric_binop("+", "ADD")
    __sub__ = make_numeric_binop("-", "SUB")
    __mul__ = make_numeric_binop("*", "MUL")
    __truediv__ = make_numeric_binop("/", "DIV")
    __floordiv__ = make_numeric_binop("//", "FLOORDIV")
    __mod__ = make_numeric_binop("%", "MOD")
    __and__ = make_numeric_binop("&", "BITAND")
    __or__ = make_numeric_binop("|", "BITOR")
    __xor__ = make_numeric_binop("^", "BITXOR")

    def __pow__(self, monad2):
        if not isinstance(monad2, NumericMixin):
            throw(
                TypeError,
                _binop_errmsg % (type2str(self.type), type2str(monad2.type), "**"),
            )
        left_sql = self.getsql()
        right_sql = monad2.getsql()
        assert len(left_sql) == len(right_sql) == 1
        return NumericExprMonad(
            float,
            ["POW", left_sql[0], right_sql[0]],
            nullable=self.nullable or monad2.nullable,
        )

    def __neg__(self):
        sql = self.getsql()[0]
        return NumericExprMonad(self.type, ["NEG", sql], nullable=self.nullable)

    def abs(self):
        sql = self.getsql()[0]
        return NumericExprMonad(self.type, ["ABS", sql], nullable=self.nullable)

    def nonzero(self):
        translator = self.translator
        sql = self.getsql()[0]
        if not (translator.dialect == "PostgreSQL" and self.type is bool):
            sql = ["NE", sql, ["VALUE", 0]]
        return BoolExprMonad(sql, nullable=False)

    def negate(self):
        sql = self.getsql()[0]
        translator = self.translator
        pg_bool = translator.dialect == "PostgreSQL" and self.type is bool
        result_sql = ["NOT", sql] if pg_bool else ["EQ", sql, ["VALUE", 0]]
        if self.nullable:
            if isinstance(self, AttrMonad):
                result_sql = ["OR", result_sql, ["IS_NULL", sql]]
            elif pg_bool:
                result_sql = ["NOT", ["COALESCE", sql, ["VALUE", True]]]
            else:
                result_sql = ["EQ", ["COALESCE", sql, ["VALUE", 0]], ["VALUE", 0]]
        return BoolExprMonad(result_sql, nullable=False)


def numeric_attr_factory(name):
    def attr_func(monad):
        sql = [name, monad.getsql()[0]]
        return NumericExprMonad(int, sql, nullable=monad.nullable)

    attr_func.__name__ = name.lower()
    return attr_func


def make_datetime_binop(op, sqlop):
    def datetime_binop(monad, monad2):
        if monad2.type != timedelta:
            throw(
                TypeError,
                _binop_errmsg % (type2str(monad.type), type2str(monad2.type), op),
            )
        expr_monad_cls = DateExprMonad if monad.type is date else DatetimeExprMonad
        return expr_monad_cls(
            monad.type,
            [sqlop, monad.getsql()[0], monad2.getsql()[0]],
            nullable=monad.nullable or monad2.nullable,
        )

    datetime_binop.__name__ = sqlop
    return datetime_binop


class DateMixin(MonadMixin):
    def mixin_init(self):
        assert self.type is date

    attr_year = numeric_attr_factory("YEAR")
    attr_month = numeric_attr_factory("MONTH")
    attr_day = numeric_attr_factory("DAY")

    def __add__(self, other):
        if other.type != timedelta:
            throw(
                TypeError,
                _binop_errmsg % (type2str(self.type), type2str(other.type), "+"),
            )
        return DateExprMonad(
            self.type,
            ["DATE_ADD", self.getsql()[0], other.getsql()[0]],
            nullable=self.nullable or other.nullable,
        )

    def __sub__(self, other):
        if other.type == timedelta:
            return DateExprMonad(
                self.type,
                ["DATE_SUB", self.getsql()[0], other.getsql()[0]],
                nullable=self.nullable or other.nullable,
            )
        elif other.type == date:
            return TimedeltaExprMonad(
                timedelta,
                ["DATE_DIFF", self.getsql()[0], other.getsql()[0]],
                nullable=self.nullable or other.nullable,
            )
        throw(
            TypeError, _binop_errmsg % (type2str(self.type), type2str(other.type), "-")
        )


class TimeMixin(MonadMixin):
    def mixin_init(self):
        assert self.type is time

    attr_hour = numeric_attr_factory("HOUR")
    attr_minute = numeric_attr_factory("MINUTE")
    attr_second = numeric_attr_factory("SECOND")


class TimedeltaMixin(MonadMixin):
    def mixin_init(self):
        assert self.type is timedelta


class DatetimeMixin(DateMixin):
    def mixin_init(self):
        assert self.type is datetime

    def call_date(self):
        sql = ["DATE", self.getsql()[0]]
        return ExprMonad.new(date, sql, nullable=self.nullable)

    attr_hour = numeric_attr_factory("HOUR")
    attr_minute = numeric_attr_factory("MINUTE")
    attr_second = numeric_attr_factory("SECOND")

    def __add__(self, other):
        if other.type != timedelta:
            throw(
                TypeError,
                _binop_errmsg % (type2str(self.type), type2str(other.type), "+"),
            )
        return DatetimeExprMonad(
            self.type,
            ["DATETIME_ADD", self.getsql()[0], other.getsql()[0]],
            nullable=self.nullable or other.nullable,
        )

    def __sub__(self, other):
        if other.type == timedelta:
            return DatetimeExprMonad(
                self.type,
                ["DATETIME_SUB", self.getsql()[0], other.getsql()[0]],
                nullable=self.nullable or other.nullable,
            )
        elif other.type == datetime:
            return TimedeltaExprMonad(
                timedelta,
                ["DATETIME_DIFF", self.getsql()[0], other.getsql()[0]],
                nullable=self.nullable or other.nullable,
            )
        throw(
            TypeError, _binop_errmsg % (type2str(self.type), type2str(other.type), "-")
        )


def make_string_binop(op, sqlop):
    def string_binop(monad, monad2):
        if not are_comparable_types(monad.type, monad2.type, sqlop):
            if monad2.type == "METHOD":
                raise_forgot_parentheses(monad2)
            throw(
                TypeError,
                _binop_errmsg % (type2str(monad.type), type2str(monad2.type), op),
            )
        left_sql = monad.getsql()
        right_sql = monad2.getsql()
        assert len(left_sql) == len(right_sql) == 1
        return StringExprMonad(
            monad.type,
            [sqlop, left_sql[0], right_sql[0]],
            nullable=monad.nullable or monad2.nullable,
        )

    string_binop.__name__ = sqlop
    return string_binop


def make_string_func(sqlop):
    def func(monad):
        sql = monad.getsql()
        assert len(sql) == 1
        return StringExprMonad(monad.type, [sqlop, sql[0]], nullable=monad.nullable)

    func.__name__ = sqlop
    return func


class StringMixin(MonadMixin):
    def mixin_init(self):
        assert issubclass(self.type, str), self.type

    __add__ = make_string_binop("+", "CONCAT")

    def __getitem__(self, index):
        root_translator = self.translator.root_translator
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

        if isinstance(index, ListMonad):
            throw(
                TypeError, "String index must be of 'int' type. Got 'tuple' in {EXPR}"
            )
        elif isinstance(index, slice):
            if index.step is not None:
                throw(TypeError, "Step is not supported in {EXPR}")
            start, stop = index.start, index.stop
            start = param_to_const(start, is_start=True)
            stop = param_to_const(stop, is_start=False)
            start_value = stop_value = None
            if start is None:
                start_value = 0
            if stop_value is None:
                stop_value = -1
            if isinstance(start, ConstMonad):
                start_value = start.value
            if isinstance(stop, ConstMonad):
                stop_value = stop.value
            if start_value == 0 and stop_value == -1:
                return self
            if (
                isinstance(self, StringConstMonad)
                and start_value is not None
                and stop_value is not None
            ):
                return ConstMonad.new(self.value[start_value:stop_value])

            if start is not None and start.type is not int:
                throw(
                    TypeError,
                    "Invalid type of start index (expected 'int', got %r) in string slice {EXPR}"
                    % type2str(start.type),
                )
            if stop is not None and stop.type is not int:
                throw(
                    TypeError,
                    "Invalid type of stop index (expected 'int', got %r) in string slice {EXPR}"
                    % type2str(stop.type),
                )
            expr_sql = self.getsql()[0]

            start_sql = None if start is None else start.getsql()[0]
            stop_sql = None if stop is None else stop.getsql()[0]
            sql = ["STRING_SLICE", expr_sql, start_sql, stop_sql]
            return StringExprMonad(
                self.type,
                sql,
                nullable=self.nullable
                or (start is not None and start.nullable)
                or (stop is not None and stop.nullable),
            )

        index = param_to_const(index)
        if isinstance(self, StringConstMonad) and isinstance(index, NumericConstMonad):
            return ConstMonad.new(self.value[index.value])
        if index.type is not int:
            throw(
                TypeError,
                "String indices must be integers. Got %r in expression {EXPR}"
                % type2str(index.type),
            )
        expr_sql = self.getsql()[0]

        if isinstance(index, NumericConstMonad):
            value = index.value
            if dialect == "PostgreSQL" and value < 0:
                index_sql = ["LENGTH", expr_sql]
                if value < -1:
                    index_sql = ["SUB", index_sql, ["VALUE", -(value + 1)]]
            else:
                if value >= 0:
                    value += 1
                index_sql = ["VALUE", value]
        else:
            inner_sql = index.getsql()[0]
            then = ["ADD", inner_sql, ["VALUE", 1]]
            else_ = (
                ["ADD", ["LENGTH", expr_sql], then]
                if dialect == "PostgreSQL"
                else inner_sql
            )
            index_sql = ["IF", ["GE", inner_sql, ["VALUE", 0]], then, else_]

        sql = ["SUBSTR", expr_sql, index_sql, ["VALUE", 1]]
        return StringExprMonad(self.type, sql, nullable=self.nullable)

    def negate(self):
        sql = self.getsql()[0]
        translator = self.translator
        if translator.dialect == "Oracle":
            result_sql = ["IS_NULL", sql]
        else:
            result_sql = ["EQ", sql, ["VALUE", ""]]
            if self.nullable:
                if isinstance(self, AttrMonad):
                    result_sql = ["OR", result_sql, ["IS_NULL", sql]]
                else:
                    result_sql = ["EQ", ["COALESCE", sql, ["VALUE", ""]], ["VALUE", ""]]
        result = BoolExprMonad(result_sql, nullable=False)
        result.aggregated = self.aggregated
        return result

    def nonzero(self):
        sql = self.getsql()[0]
        translator = self.translator
        if translator.dialect == "Oracle":
            result_sql = ["IS_NOT_NULL", sql]
        else:
            result_sql = ["NE", sql, ["VALUE", ""]]
        result = BoolExprMonad(result_sql, nullable=False)
        result.aggregated = self.aggregated
        return result

    def len(self):
        sql = self.getsql()[0]
        return NumericExprMonad(int, ["LENGTH", sql])

    def contains(self, item, not_in=False):
        check_comparable(item, self, "LIKE")
        return self._like(item, before="%", after="%", not_like=not_in)

    call_upper = make_string_func("UPPER")
    call_lower = make_string_func("LOWER")

    def call_startswith(self, arg):
        if not are_comparable_types(self.type, arg.type, None):
            if arg.type == "METHOD":
                raise_forgot_parentheses(arg)
            throw(
                TypeError,
                "Expected %r argument but got %r in expression {EXPR}"
                % (type2str(self.type), type2str(arg.type)),
            )
        return self._like(arg, after="%")

    def call_endswith(self, arg):
        if not are_comparable_types(self.type, arg.type, None):
            if arg.type == "METHOD":
                raise_forgot_parentheses(arg)
            throw(
                TypeError,
                "Expected %r argument but got %r in expression {EXPR}"
                % (type2str(self.type), type2str(arg.type)),
            )
        return self._like(arg, before="%")

    def _like(self, item, before=None, after=None, not_like=False):
        escape = False
        translator = self.translator
        if isinstance(item, StringConstMonad):
            value = item.value
            if "%" in value or "_" in value:
                escape = True
                value = value.replace("!", "!!").replace("%", "!%").replace("_", "!_")
            if before:
                value = before + value
            if after:
                value = value + after
            item_sql = ["VALUE", value]
        else:
            escape = True
            item_sql = item.getsql()[0]
            item_sql = ["REPLACE", item_sql, ["VALUE", "!"], ["VALUE", "!!"]]
            item_sql = ["REPLACE", item_sql, ["VALUE", "%"], ["VALUE", "!%"]]
            item_sql = ["REPLACE", item_sql, ["VALUE", "_"], ["VALUE", "!_"]]
            if before and after:
                item_sql = ["CONCAT", ["VALUE", before], item_sql, ["VALUE", after]]
            elif before:
                item_sql = ["CONCAT", ["VALUE", before], item_sql]
            elif after:
                item_sql = ["CONCAT", item_sql, ["VALUE", after]]
        sql = self.getsql()[0]
        if (
            not_like
            and self.nullable
            and not isinstance(self, AttrMonad)
            and translator.dialect != "Oracle"
        ):
            sql = ["COALESCE", sql, ["VALUE", ""]]
        result_sql = ["NOT_LIKE" if not_like else "LIKE", sql, item_sql]
        if escape:
            result_sql.append(["VALUE", "!"])
        if (
            not_like
            and self.nullable
            and (isinstance(self, AttrMonad) or translator.dialect == "Oracle")
        ):
            result_sql = ["OR", result_sql, ["IS_NULL", sql]]
        return BoolExprMonad(result_sql, nullable=not_like)

    def strip(self, chars, strip_type):
        if chars is not None and not are_comparable_types(self.type, chars.type, None):
            if chars.type == "METHOD":
                raise_forgot_parentheses(chars)
            throw(
                TypeError,
                "'chars' argument must be of %r type in {EXPR}, got: %r"
                % (type2str(self.type), type2str(chars.type)),
            )
        parent_sql = self.getsql()[0]
        sql = [strip_type, parent_sql]
        if chars is not None:
            sql.append(chars.getsql()[0])
        return StringExprMonad(self.type, sql, nullable=self.nullable)

    def call_strip(self, chars=None):
        return self.strip(chars, "TRIM")

    def call_lstrip(self, chars=None):
        return self.strip(chars, "LTRIM")

    def call_rstrip(self, chars=None):
        return self.strip(chars, "RTRIM")


class JsonMixin:
    disable_distinct = (
        True  # at least in Oracle we cannot use DISTINCT with JSON column
    )
    disable_ordering = (
        True  # at least in Oracle we cannot use ORDER BY with JSON column
    )

    def mixin_init(self):
        assert self.type is Json, self.type

    def get_path(self):
        return self, []

    def __getitem__(self, key):
        return JsonItemMonad(self, key)

    def contains(self, key, not_in=False):
        translator = self.translator
        if isinstance(key, ParamMonad):
            if translator.dialect == "Oracle":
                throw(
                    TypeError,
                    "For `key in JSON` operation %s supports literal key values only, "
                    "parameters are not allowed: {EXPR}" % translator.dialect,
                )
        elif not isinstance(key, StringConstMonad):
            raise NotImplementedError
        base_monad, path = self.get_path()
        base_sql = base_monad.getsql()[0]
        key_sql = key.getsql()[0]
        sql = ["JSON_CONTAINS", base_sql, path, key_sql]
        if not_in:
            sql = ["NOT", sql]
        return BoolExprMonad(sql)

    def __or__(self, other):
        if not isinstance(other, JsonMixin):
            raise TypeError("Should be JSON: %s" % ast2src(other.node))
        left_sql = self.getsql()[0]
        right_sql = other.getsql()[0]
        sql = ["JSON_CONCAT", left_sql, right_sql]
        return JsonExprMonad(Json, sql)

    def len(self):
        sql = ["JSON_ARRAY_LENGTH", self.getsql()[0]]
        return NumericExprMonad(int, sql)

    def cast_from_json(self, type):
        if type in (Json, NoneType):
            return self
        throw(
            TypeError,
            "Cannot compare whole JSON value, you need to select specific sub-item: {EXPR}",
        )

    def nonzero(self):
        return BoolExprMonad(["JSON_NONZERO", self.getsql()[0]])


class ArrayMixin(MonadMixin):
    def contains(self, key, not_in=False):
        if key.type is self.type.item_type:
            sql = "ARRAY_CONTAINS", key.getsql()[0], not_in, self.getsql()[0]
            return BoolExprMonad(sql)
        if isinstance(key, ListMonad):
            if not key.items:
                if not_in:
                    return BoolExprMonad(
                        ["EQ", ["VALUE", 0], ["VALUE", 1]], nullable=False
                    )
                else:
                    return BoolExprMonad(
                        ["EQ", ["VALUE", 1], ["VALUE", 1]], nullable=False
                    )
            sql = ["MAKE_ARRAY"]
            sql.extend(item.getsql()[0] for item in key.items)
            sql = "ARRAY_SUBSET", sql, not_in, self.getsql()[0]
            return BoolExprMonad(sql)
        elif isinstance(key, ArrayParamMonad):
            sql = "ARRAY_SUBSET", key.getsql()[0], not_in, self.getsql()[0]
            return BoolExprMonad(sql)
        throw(
            TypeError,
            "Cannot search for %s in %s: {EXPR}"
            % (type2str(key.type), type2str(self.type)),
        )

    def len(self):
        sql = ["ARRAY_LENGTH", self.getsql()[0]]
        return NumericExprMonad(int, sql)

    def nonzero(self):
        return BoolExprMonad(["GT", ["ARRAY_LENGTH", self.getsql()[0]], ["VALUE", 0]])

    def _index(self, index, from_one, plus_one):
        if isinstance(index, NumericConstMonad):
            expr_sql = self.getsql()[0]
            index_sql = index.getsql()[0]
            value = index_sql[1]
            if value >= 0:
                index_sql = ["VALUE", value + int(from_one and plus_one)]
            else:
                index_sql = [
                    "SUB",
                    ["ARRAY_LENGTH", expr_sql],
                    ["VALUE", abs(value + int(from_one and plus_one))],
                ]
            return index_sql
        elif isinstance(index, NumericMixin):
            expr_sql = self.getsql()[0]
            index0 = index.getsql()[0]
            index1 = ["ADD", index0, ["VALUE", 1]] if from_one and plus_one else index0
            index_sql = [
                "CASE",
                None,
                [[["GE", index0, ["VALUE", 0]], index1]],
                ["ADD", ["ARRAY_LENGTH", expr_sql], index1],
            ]
            return index_sql

    def __getitem__(self, index):
        dialect = self.translator.database.provider.dialect
        expr_sql = self.getsql()[0]
        from_one = dialect != "SQLite"
        if isinstance(index, NumericMixin):
            index_sql = self._index(index, from_one, plus_one=True)
            sql = ["ARRAY_INDEX", expr_sql, index_sql]
            return ExprMonad.new(self.type.item_type, sql)
        elif isinstance(index, slice):
            if index.step is not None:
                throw(TypeError, "Step is not supported in {EXPR}")
            start_sql = self._index(index.start, from_one, plus_one=True)
            stop_sql = self._index(index.stop, from_one, plus_one=False)
            sql = ["ARRAY_SLICE", expr_sql, start_sql, stop_sql]
            return ExprMonad.new(self.type, sql)


class ObjectMixin(MonadMixin):
    def mixin_init(self):
        assert isinstance(self.type, EntityMeta)

    def negate(self):
        return CmpMonad("is", self, NoneMonad())

    def nonzero(self):
        return CmpMonad("is not", self, NoneMonad())

    def getattr(self, attrname):
        if isinstance(self, ParamMonad):
            throw(
                NotImplementedError,
                "{EXPR} for external expressions inside hybrid methods is not supported",
            )
        entity = self.type
        attr = entity._adict_.get(attrname) or entity._subclass_adict_.get(attrname)
        if attr is None:
            if hasattr(entity, attrname):
                attr = getattr(entity, attrname, None)
                if isinstance(attr, property):
                    new_monad = HybridMethodMonad(self, attrname, attr.fget)
                    return new_monad()
                if callable(attr):
                    func = attr
                    if func is not None:
                        return HybridMethodMonad(self, attrname, func)
                throw(NotImplementedError, "{EXPR} cannot be translated to SQL")
            throw(
                AttributeError,
                "Entity %s does not have attribute %s: {EXPR}"
                % (entity.__name__, attrname),
            )
        if hasattr(self, "tableref"):
            self.tableref.used_attrs.add(attr)
        if not attr.is_collection:
            return AttrMonad.new(self, attr)
        else:
            return AttrSetMonad(self, attr)

    def requires_distinct(self, joined=False):
        return self.attr.reverse.is_collection or self.parent.requires_distinct(
            joined
        )  # parent ???


class ObjectIterMonad(ObjectMixin, Monad):
    def __init__(self, tableref, entity):
        Monad.__init__(self, entity)
        self.tableref = tableref

    def getsql(self, sqlquery=None):
        alias, pk_columns = self.tableref.make_join(pk_only=True)
        return [["COLUMN", alias, column] for column in pk_columns]

    def requires_distinct(self, joined=False):
        return self.tableref.name_path != self.translator.tree.generators[-1].target.id


class AttrMonad(Monad):
    @staticmethod
    def new(parent, attr, *args, **kwargs):
        t = normalize_type(attr.py_type)
        if t in numeric_types:
            cls = NumericAttrMonad
        elif t is str:
            cls = StringAttrMonad
        elif t is date:
            cls = DateAttrMonad
        elif t is time:
            cls = TimeAttrMonad
        elif t is timedelta:
            cls = TimedeltaAttrMonad
        elif t is datetime:
            cls = DatetimeAttrMonad
        elif t is buffer:
            cls = BufferAttrMonad
        elif t is UUID:
            cls = UuidAttrMonad
        elif t is Json:
            cls = JsonAttrMonad
        elif isinstance(t, EntityMeta):
            cls = ObjectAttrMonad
        elif isinstance(t, type) and issubclass(t, Array):
            cls = ArrayAttrMonad
        else:
            throw(NotImplementedError, t)  # pragma: no cover
        return cls(parent, attr, *args, **kwargs)

    def __new__(cls, *args):
        if cls is AttrMonad:
            assert False, "Abstract class"  # pragma: no cover
        return Monad.__new__(cls)

    def __init__(self, parent, attr):
        assert self.__class__ is not AttrMonad
        attr_type = normalize_type(attr.py_type)
        Monad.__init__(self, attr_type)
        self.parent = parent
        self.attr = attr
        self.nullable = attr.nullable

    def getsql(self, sqlquery=None):
        parent = self.parent
        attr = self.attr
        entity = attr.entity
        pk_only = attr.pk_offset is not None
        alias, parent_columns = parent.tableref.make_join(pk_only)
        if pk_only:
            if entity._pk_is_composite_:
                offset = attr.pk_columns_offset
                columns = parent_columns[offset : offset + len(attr.columns)]
            else:
                columns = parent_columns
        elif not attr.columns:
            assert isinstance(self, ObjectAttrMonad)
            sqlquery = self.translator.sqlquery
            self.translator.left_join = sqlquery.left_join = True
            sqlquery.from_ast[0] = "LEFT_JOIN"
            alias, columns = self.tableref.make_join()
        else:
            columns = attr.columns
        return [["COLUMN", alias, column] for column in columns]


class ObjectAttrMonad(ObjectMixin, AttrMonad):
    def __init__(self, parent, attr):
        AttrMonad.__init__(self, parent, attr)
        translator = self.translator
        parent_monad = self.parent
        name_path = "-".join((parent_monad.tableref.name_path, attr.name))
        self.tableref = translator.sqlquery.get_tableref(name_path)
        if self.tableref is None:
            parent_sqlquery = parent_monad.tableref.sqlquery
            self.tableref = parent_sqlquery.add_tableref(
                name_path, parent_monad.tableref, attr
            )


class StringAttrMonad(StringMixin, AttrMonad):
    pass


class NumericAttrMonad(NumericMixin, AttrMonad):
    pass


class DateAttrMonad(DateMixin, AttrMonad):
    pass


class TimeAttrMonad(TimeMixin, AttrMonad):
    pass


class TimedeltaAttrMonad(TimedeltaMixin, AttrMonad):
    pass


class DatetimeAttrMonad(DatetimeMixin, AttrMonad):
    pass


class BufferAttrMonad(BufferMixin, AttrMonad):
    pass


class UuidAttrMonad(UuidMixin, AttrMonad):
    pass


class JsonAttrMonad(JsonMixin, AttrMonad):
    pass


class ArrayAttrMonad(ArrayMixin, AttrMonad):
    pass


class ParamMonad(Monad):
    @staticmethod
    def new(t, paramkey):
        t = normalize_type(t)
        if t in numeric_types:
            cls = NumericParamMonad
        elif t is str:
            cls = StringParamMonad
        elif t is date:
            cls = DateParamMonad
        elif t is time:
            cls = TimeParamMonad
        elif t is timedelta:
            cls = TimedeltaParamMonad
        elif t is datetime:
            cls = DatetimeParamMonad
        elif t is buffer:
            cls = BufferParamMonad
        elif t is UUID:
            cls = UuidParamMonad
        elif t is Json:
            cls = JsonParamMonad
        elif isinstance(t, type) and issubclass(t, Array):
            cls = ArrayParamMonad
        elif isinstance(t, EntityMeta):
            cls = ObjectParamMonad
        else:
            throw(
                NotImplementedError, "Parameter {EXPR} has unsupported type %r" % (t,)
            )
        result = cls(t, paramkey)
        result.aggregated = False
        return result

    def __new__(cls, *args, **kwargs):
        if cls is ParamMonad:
            assert False, "Abstract class"  # pragma: no cover
        return Monad.__new__(cls)

    def __init__(self, t, paramkey):
        t = normalize_type(t)
        Monad.__init__(self, t, nullable=False)
        self.paramkey = paramkey
        if not isinstance(t, EntityMeta):
            provider = self.translator.database.provider
            self.converter = provider.get_converter_by_py_type(t)
        else:
            self.converter = None

    def getsql(self, sqlquery=None):
        return [["PARAM", self.paramkey, self.converter]]


class ObjectParamMonad(ObjectMixin, ParamMonad):
    def __init__(self, entity, paramkey):
        ParamMonad.__init__(self, entity, paramkey)
        if self.translator.database is not entity._database_:
            assert self.translator.database is entity._database_, (
                paramkey,
                self.translator.database,
                entity._database_,
            )
        varkey, i, j = paramkey
        assert j is None
        self.params = tuple((varkey, i, j) for j in range(len(entity._pk_converters_)))

    def getsql(self, sqlquery=None):
        entity = self.type
        assert len(self.params) == len(entity._pk_converters_)
        return [
            ["PARAM", param, converter]
            for param, converter in zip(self.params, entity._pk_converters_)
        ]

    def requires_distinct(self, joined=False):
        assert False  # pragma: no cover


class StringParamMonad(StringMixin, ParamMonad):
    pass


class NumericParamMonad(NumericMixin, ParamMonad):
    pass


class DateParamMonad(DateMixin, ParamMonad):
    pass


class TimeParamMonad(TimeMixin, ParamMonad):
    pass


class TimedeltaParamMonad(TimedeltaMixin, ParamMonad):
    pass


class DatetimeParamMonad(DatetimeMixin, ParamMonad):
    pass


class BufferParamMonad(BufferMixin, ParamMonad):
    pass


class UuidParamMonad(UuidMixin, ParamMonad):
    pass


class ArrayParamMonad(ArrayMixin, ParamMonad):
    def __init__(self, t, paramkey, list_monad=None):
        ParamMonad.__init__(self, t, paramkey)
        self.list_monad = list_monad

    def contains(self, key, not_in=False):
        if key.type is self.type.item_type:
            return self.list_monad.contains(key, not_in)
        return ArrayMixin.contains(self, key, not_in)


class JsonParamMonad(JsonMixin, ParamMonad):
    def getsql(self, sqlquery=None):
        return [["JSON_PARAM", ParamMonad.getsql(self)[0]]]


class ExprMonad(Monad):
    @staticmethod
    def new(t, sql, nullable=True):
        if t in numeric_types:
            cls = NumericExprMonad
        elif t is str:
            cls = StringExprMonad
        elif t is date:
            cls = DateExprMonad
        elif t is time:
            cls = TimeExprMonad
        elif t is timedelta:
            cls = TimedeltaExprMonad
        elif t is datetime:
            cls = DatetimeExprMonad
        elif t is Json:
            cls = JsonExprMonad
        elif isinstance(t, EntityMeta):
            cls = ObjectExprMonad
        elif isinstance(t, type) and issubclass(t, Array):
            cls = ArrayExprMonad
        else:
            throw(NotImplementedError, t)  # pragma: no cover
        return cls(t, sql, nullable=nullable)

    def __new__(cls, *args, **kwargs):
        if cls is ExprMonad:
            assert False, "Abstract class"  # pragma: no cover
        return Monad.__new__(cls)

    def __init__(self, type, sql, nullable=True):
        Monad.__init__(self, type, nullable=nullable)
        self.sql = sql

    def getsql(self, sqlquery=None):
        return [self.sql]


class ObjectExprMonad(ObjectMixin, ExprMonad):
    def getsql(self, sqlquery=None):
        return self.sql


class StringExprMonad(StringMixin, ExprMonad):
    pass


class NumericExprMonad(NumericMixin, ExprMonad):
    pass


class DateExprMonad(DateMixin, ExprMonad):
    pass


class TimeExprMonad(TimeMixin, ExprMonad):
    pass


class TimedeltaExprMonad(TimedeltaMixin, ExprMonad):
    pass


class DatetimeExprMonad(DatetimeMixin, ExprMonad):
    pass


class JsonExprMonad(JsonMixin, ExprMonad):
    pass


class ArrayExprMonad(ArrayMixin, ExprMonad):
    pass


class JsonItemMonad(JsonMixin, Monad):
    def __init__(self, parent, key):
        assert isinstance(parent, JsonMixin), parent
        Monad.__init__(self, Json)
        self.parent = parent
        if isinstance(key, slice):
            if key != slice(None, None, None):
                throw(NotImplementedError)
            self.key_ast = ["VALUE", key]
        elif isinstance(
            key, (ParamMonad, StringConstMonad, NumericConstMonad, EllipsisMonad)
        ):
            self.key_ast = key.getsql()[0]
        else:
            throw(TypeError, "Invalid JSON path item: %s" % ast2src(key.node))
        translator = self.translator
        if (
            isinstance(key, (slice, EllipsisMonad))
            and not translator.json_path_wildcard_syntax
        ):
            throw(
                TranslationError,
                "%s does not support wildcards in JSON path: {EXPR}"
                % translator.dialect,
            )

    def get_path(self):
        path = []
        while isinstance(self, JsonItemMonad):
            path.append(self.key_ast)
            self = self.parent
        path.reverse()
        return self, path

    def to_int(self):
        return self.cast_from_json(int)

    def to_str(self):
        return self.cast_from_json(str)

    def to_real(self):
        return self.cast_from_json(float)

    def cast_from_json(self, type):
        translator = self.translator
        if issubclass(type, Json):
            if not translator.json_values_are_comparable:
                throw(
                    TranslationError,
                    "%s does not support comparison of json structures: {EXPR}"
                    % translator.dialect,
                )
            return self
        base_monad, path = self.get_path()
        sql = ["JSON_VALUE", base_monad.getsql()[0], path, type]
        return ExprMonad.new(Json if type is NoneType else type, sql)

    def getsql(self):
        base_monad, path = self.get_path()
        base_sql = base_monad.getsql()[0]
        translator = self.translator
        if translator.inside_order_by and translator.dialect == "SQLite":
            return [["JSON_VALUE", base_sql, path, None]]
        return [["JSON_QUERY", base_sql, path]]


class ConstMonad(Monad):
    @staticmethod
    def new(value):
        value_type, value = normalize(value)
        if isinstance(value_type, tuple):
            return ListMonad([ConstMonad.new(item) for item in value])
        elif value_type in numeric_types:
            cls = NumericConstMonad
        elif value_type is str:
            cls = StringConstMonad
        elif value_type is date:
            cls = DateConstMonad
        elif value_type is time:
            cls = TimeConstMonad
        elif value_type is timedelta:
            cls = TimedeltaConstMonad
        elif value_type is datetime:
            cls = DatetimeConstMonad
        elif value_type is NoneType:
            cls = NoneMonad
        elif value_type is buffer:
            cls = BufferConstMonad
        elif value_type is Json:
            cls = JsonConstMonad
        elif issubclass(value_type, type(Ellipsis)):
            cls = EllipsisMonad
        else:
            throw(NotImplementedError, value_type)  # pragma: no cover
        result = cls(value)
        result.aggregated = False
        return result

    def __new__(cls, *args):
        if cls is ConstMonad:
            assert False, "Abstract class"  # pragma: no cover
        return Monad.__new__(cls)

    def __init__(self, value):
        value_type, value = normalize(value)
        Monad.__init__(self, value_type, nullable=value_type is NoneType)
        self.value = value

    def getsql(self, sqlquery=None):
        return [["VALUE", self.value]]


class NoneMonad(ConstMonad):
    type = NoneType

    def __init__(self, value=None):
        assert value is None
        ConstMonad.__init__(self, value)

    def cmp(self, op, monad2):
        return CmpMonad(op, self, monad2)

    def contains(self, item, not_in=False):
        return NoneMonad()

    def nonzero(self):
        return NoneMonad()

    def negate(self):
        return NoneMonad()

    def getattr(self, attrname):
        return NoneMonad()

    def len(self):
        return NoneMonad()

    def count(self, distinct=None):
        return NumericExprMonad(int, [["VALUE", 0]], nullable=False)

    def aggregate(self, func_name, distinct=None, sep=None):
        return NoneMonad()

    def __call__(self, *args, **kwargs):
        return NoneMonad()

    def __getitem__(self, key):
        return NoneMonad()

    def __add__(self, monad2):
        return NoneMonad()

    def __sub__(self, monad2):
        return NoneMonad()

    def __mul__(self, monad2):
        return NoneMonad()

    def __truediv__(self, monad2):
        return NoneMonad()

    def __floordiv__(self, monad2):
        return NoneMonad()

    def __pow__(self, monad2):
        return NoneMonad()

    def __neg__(self):
        return NoneMonad()

    def __or__(self, monad2):
        return NoneMonad()

    def __and__(self, monad2):
        return NoneMonad()

    def __xor__(self, monad2):
        return NoneMonad()

    def abs(self):
        return NoneMonad()

    def to_int(self):
        return NoneMonad()

    def to_str(self):
        return NoneMonad()

    def to_real(self):
        return NoneMonad()


class EllipsisMonad(ConstMonad):
    pass


class StringConstMonad(StringMixin, ConstMonad):
    def len(self):
        return ConstMonad.new(len(self.value))


class JsonConstMonad(JsonMixin, ConstMonad):
    pass


class BufferConstMonad(BufferMixin, ConstMonad):
    pass


class NumericConstMonad(NumericMixin, ConstMonad):
    pass


class DateConstMonad(DateMixin, ConstMonad):
    pass


class TimeConstMonad(TimeMixin, ConstMonad):
    pass


class TimedeltaConstMonad(TimedeltaMixin, ConstMonad):
    pass


class DatetimeConstMonad(DatetimeMixin, ConstMonad):
    pass


class BoolMonad(Monad):
    def __init__(self, nullable=True):
        Monad.__init__(self, bool, nullable=nullable)

    def nonzero(self):
        return self


sql_negation = {
    "IN": "NOT_IN",
    "EXISTS": "NOT_EXISTS",
    "LIKE": "NOT_LIKE",
    "BETWEEN": "NOT_BETWEEN",
    "IS_NULL": "IS_NOT_NULL",
}
sql_negation.update((value, key) for key, value in list(sql_negation.items()))


class BoolExprMonad(BoolMonad):
    def __init__(self, sql, nullable=True):
        BoolMonad.__init__(self, nullable=nullable)
        self.sql = sql

    def getsql(self, sqlquery=None):
        return [self.sql]

    def negate(self):
        sql = self.sql
        sqlop = sql[0]
        negated_op = sql_negation.get(sqlop)
        if negated_op is not None:
            negated_sql = [negated_op] + sql[1:]
        elif negated_op == "NOT":
            assert len(sql) == 2
            negated_sql = sql[1]
        else:
            return NotMonad(self)
        return BoolExprMonad(negated_sql, nullable=self.nullable)


cmp_ops = {">=": "GE", ">": "GT", "<=": "LE", "<": "LT"}

cmp_negate = {"<": ">=", "<=": ">", "==": "!=", "is": "is not"}
cmp_negate.update((b, a) for a, b in list(cmp_negate.items()))


class CmpMonad(BoolMonad):
    EQ = "EQ"
    NE = "NE"

    def __init__(self, op, left, right):
        if op == "<>":
            op = "!="
        if left.type is NoneType:
            left, right = right, left
        if right.type is NoneType:
            if op == "==":
                op = "is"
            elif op == "!=":
                op = "is not"
        elif op == "is":
            op = "=="
        elif op == "is not":
            op = "!="
        check_comparable(left, right, op)
        result_type, left, right = coerce_monads(left, right, for_comparison=True)
        BoolMonad.__init__(self, nullable=left.nullable or right.nullable)
        self.op = op
        self.aggregated = getattr(left, "aggregated", False) or getattr(
            right, "aggregated", False
        )

        if isinstance(left, JsonMixin):
            left = left.cast_from_json(right.type)
        if isinstance(right, JsonMixin):
            right = right.cast_from_json(left.type)

        self.left = left
        self.right = right

    def negate(self):
        return CmpMonad(cmp_negate[self.op], self.left, self.right)

    def getsql(self, sqlquery=None):
        op = self.op
        if (
            self.left.type is NoneType and self.right.type is NoneType
        ):  # in hybrid methods
            return [["EQ" if op == "is" else "NE", ["VALUE", 1], ["VALUE", 1]]]
        left_sql = self.left.getsql()
        if op == "is":
            return [sqland([["IS_NULL", item] for item in left_sql])]
        if op == "is not":
            return [sqland([["IS_NOT_NULL", item] for item in left_sql])]
        right_sql = self.right.getsql()
        if len(left_sql) == 1 and left_sql[0][0] == "ROW":
            left_sql = left_sql[0][1:]
        if len(right_sql) == 1 and right_sql[0][0] == "ROW":
            right_sql = right_sql[0][1:]
        assert len(left_sql) == len(right_sql)
        size = len(left_sql)
        if op in ("<", "<=", ">", ">="):
            if size == 1:
                return [[cmp_ops[op], left_sql[0], right_sql[0]]]
            if self.translator.row_value_syntax:
                return [[cmp_ops[op], ["ROW"] + left_sql, ["ROW"] + right_sql]]
            clauses = []
            for i in range(size):
                clause = [[self.EQ, left_sql[j], right_sql[j]] for j in range(i)]
                clause.append([cmp_ops[op], left_sql[i], right_sql[i]])
                clauses.append(sqland(clause))
            return [sqlor(clauses)]
        if op == "==":
            return [sqland([[self.EQ, a, b] for a, b in zip(left_sql, right_sql)])]
        if op == "!=":
            return [sqlor([[self.NE, a, b] for a, b in zip(left_sql, right_sql)])]
        assert False, op  # pragma: no cover


class LogicalBinOpMonad(BoolMonad):
    def __init__(self, operands):
        assert len(operands) >= 2
        items = []
        for operand in operands:
            if operand.type is not bool:
                items.append(operand.nonzero())
            elif isinstance(operand, LogicalBinOpMonad) and self.binop == operand.binop:
                items.extend(operand.operands)
            else:
                items.append(operand)
        nullable = any(item.nullable for item in items)
        BoolMonad.__init__(self, nullable=nullable)
        self.operands = items

    def getsql(self, sqlquery=None):
        result = [self.binop]
        for operand in self.operands:
            operand_sql = operand.getsql()
            assert len(operand_sql) == 1
            result.extend(operand_sql)
        return [result]


class AndMonad(LogicalBinOpMonad):
    binop = "AND"


class OrMonad(LogicalBinOpMonad):
    binop = "OR"


class NotMonad(BoolMonad):
    def __init__(self, operand):
        if operand.type is not bool:
            operand = operand.nonzero()
        BoolMonad.__init__(self, nullable=operand.nullable)
        self.operand = operand

    def negate(self):
        return self.operand

    def getsql(self, sqlquery=None):
        return [["NOT", self.operand.getsql()[0]]]


class HybridFuncMonad(Monad):
    def __init__(self, func_type, func_name, *params):
        Monad.__init__(self, func_type)
        self.func = func_type.func
        self.func_name = func_name
        self.params = params

    def __call__(self, *args, **kwargs):
        translator = self.translator
        name_mapping = inspect.getcallargs(self.func, *(self.params + args), **kwargs)

        for name, value in name_mapping.items():
            if not isinstance(value, Monad):
                value = ConstMonad.new(value)
                name_mapping[name] = value

        func = self.func
        func_id = id(func)
        try:
            func_ast, external_names, cells = decompile(func)
        except DecompileError:
            throw(
                TranslationError,
                "%s(...) is too complex to decompile" % self.func_name,
            )

        func_ast, func_extractors = create_extractors(
            func_id,
            func_ast,
            func.__globals__,
            {},
            special_functions,
            const_functions,
            outer_names=name_mapping,
        )

        root_translator = translator.root_translator
        if func not in root_translator.func_extractors_map:
            func_vars, func_vartypes = extract_vars(
                func_id,
                translator.filter_num,
                func_extractors,
                func.__globals__,
                {},
                cells,
            )
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
            if len(e.args) == 1 and isinstance(e.args[0], str):
                msg = e.args[0] + " (inside %s)" % (self.func_name)
                e.args = (msg,)
            raise
        finally:
            stack.pop()
        return func_ast.monad


class HybridMethodMonad(HybridFuncMonad):
    def __init__(self, parent, attrname, func):
        entity = parent.type
        assert isinstance(entity, EntityMeta)
        func_name = "%s.%s" % (entity.__name__, attrname)
        HybridFuncMonad.__init__(self, FuncType(func), func_name, parent)


registered_functions = SQLTranslator.registered_functions = {}


class FuncMonadMeta(MonadMeta):
    def __new__(meta, cls_name, bases, cls_dict):
        func = cls_dict.get("func")
        monad_cls = super().__new__(meta, cls_name, bases, cls_dict)
        if func:
            if type(func) is tuple:
                functions = func
            else:
                functions = (func,)
            for func in functions:
                registered_functions[func] = monad_cls
        return monad_cls


class FuncMonad(Monad, metaclass=FuncMonadMeta):
    def __call__(self, *args, **kwargs):
        for arg in args:
            assert isinstance(arg, Monad)
        for value in kwargs.values():
            assert isinstance(value, Monad)
        try:
            return self.call(*args, **kwargs)
        except TypeError as exc:
            reraise_improved_typeerror(exc, "call", self.type.__name__)


def get_classes(classinfo):
    if isinstance(classinfo, EntityMonad):
        yield classinfo.type.item_type
    elif isinstance(classinfo, ListMonad):
        for item in classinfo.items:
            yield from get_classes(item)
    else:
        throw(TypeError, ast2src(classinfo.node))


class FuncIsinstanceMonad(FuncMonad):
    func = isinstance

    def call(self, obj, classinfo):
        if not isinstance(obj, ObjectMixin):
            throw(
                ValueError,
                "Inside a query, isinstance first argument should be of entity type. Got: %s"
                % ast2src(obj.node),
            )
        entity = obj.type
        classes = list(get_classes(classinfo))
        subclasses = set()
        for cls in classes:
            if entity._root_ is cls._root_:
                subclasses.add(cls)
                subclasses.update(cls._subclasses_)
        if entity in subclasses:
            return BoolExprMonad(["EQ", ["VALUE", 1], ["VALUE", 1]], nullable=False)

        subclasses.intersection_update(entity._subclasses_)
        if not subclasses:
            return BoolExprMonad(["EQ", ["VALUE", 0], ["VALUE", 1]], nullable=False)

        discr_attr = entity._discriminator_attr_
        assert discr_attr is not None
        discr_values = [["VALUE", cls._discriminator_] for cls in subclasses]
        alias, pk_columns = obj.tableref.make_join(pk_only=True)
        sql = ["IN", ["COLUMN", alias, discr_attr.column], discr_values]
        return BoolExprMonad(sql, nullable=False)


class FuncBufferMonad(FuncMonad):
    func = buffer

    def call(self, source, encoding=None, errors=None):
        if not isinstance(source, StringConstMonad):
            throw(TypeError)
        source = source.value
        if encoding is not None:
            if not isinstance(encoding, StringConstMonad):
                throw(TypeError)
            encoding = encoding.value
        if errors is not None:
            if not isinstance(errors, StringConstMonad):
                throw(TypeError)
            errors = errors.value
        if encoding and errors:
            value = buffer(source, encoding, errors)
        elif encoding:
            value = buffer(source, encoding)
        else:
            value = buffer(source)
        return ConstMonad.new(value)


class FuncBoolMonad(FuncMonad):
    func = bool

    def call(self, x):
        return x.nonzero()


class FuncIntMonad(FuncMonad):
    func = int

    def call(self, x):
        return x.to_int()


class FuncStrMonad(FuncMonad):
    func = str

    def call(self, x):
        return x.to_str()


class FuncFloatMonad(FuncMonad):
    func = float

    def call(self, x):
        return x.to_real()


class FuncDecimalMonad(FuncMonad):
    func = Decimal

    def call(self, x):
        if not isinstance(x, StringConstMonad):
            throw(TypeError)
        return ConstMonad.new(Decimal(x.value))


class FuncDateMonad(FuncMonad):
    func = date

    def call(self, year, month, day):
        for arg, name in zip((year, month, day), ("year", "month", "day")):
            if not isinstance(arg, NumericMixin) or arg.type is not int:
                throw(
                    TypeError,
                    "'%s' argument of date(year, month, day) function must be of 'int' type. "
                    "Got: %r" % (name, type2str(arg.type)),
                )
            if not isinstance(arg, ConstMonad):
                throw(NotImplementedError)
        return ConstMonad.new(date(year.value, month.value, day.value))

    def call_today(self):
        return DateExprMonad(date, ["TODAY"], nullable=self.nullable)


class FuncTimeMonad(FuncMonad):
    func = time

    def call(self, *args):
        for arg, name in zip(args, ("hour", "minute", "second", "microsecond")):
            if not isinstance(arg, NumericMixin) or arg.type is not int:
                throw(
                    TypeError,
                    "'%s' argument of time(...) function must be of 'int' type. Got: %r"
                    % (name, type2str(arg.type)),
                )
            if not isinstance(arg, ConstMonad):
                throw(NotImplementedError)
        return ConstMonad.new(time(*tuple(arg.value for arg in args)))


class FuncTimedeltaMonad(FuncMonad):
    func = timedelta

    def call(
        self,
        days=None,
        seconds=None,
        microseconds=None,
        milliseconds=None,
        minutes=None,
        hours=None,
        weeks=None,
    ):
        args = days, seconds, microseconds, milliseconds, minutes, hours, weeks
        for arg, name in zip(
            args,
            (
                "days",
                "seconds",
                "microseconds",
                "milliseconds",
                "minutes",
                "hours",
                "weeks",
            ),
        ):
            if arg is None:
                continue
            if not isinstance(arg, NumericMixin) or arg.type is not int:
                throw(
                    TypeError,
                    "'%s' argument of timedelta(...) function must be of 'int' type. Got: %r"
                    % (name, type2str(arg.type)),
                )
            if not isinstance(arg, ConstMonad):
                throw(NotImplementedError)
        value = timedelta(*(arg.value if arg is not None else 0 for arg in args))
        return ConstMonad.new(value)


class FuncDatetimeMonad(FuncDateMonad):
    func = datetime

    def call(
        self, year, month, day, hour=None, minute=None, second=None, microsecond=None
    ):
        args = year, month, day, hour, minute, second, microsecond
        for arg, name in zip(
            args, ("year", "month", "day", "hour", "minute", "second", "microsecond")
        ):
            if arg is None:
                continue
            if not isinstance(arg, NumericMixin) or arg.type is not int:
                throw(
                    TypeError,
                    "'%s' argument of datetime(...) function must be of 'int' type. Got: %r"
                    % (name, type2str(arg.type)),
                )
            if not isinstance(arg, ConstMonad):
                throw(NotImplementedError)
        value = datetime(*(arg.value if arg is not None else 0 for arg in args))
        return ConstMonad.new(value)

    def call_now(self):
        return DatetimeExprMonad(datetime, ["NOW"], nullable=self.nullable)


class FuncBetweenMonad(FuncMonad):
    func = between

    def call(self, x, a, b):
        check_comparable(x, a, "<")
        check_comparable(x, b, "<")
        if isinstance(x.type, EntityMeta):
            throw(
                TypeError,
                "%s instance cannot be argument of between() function: {EXPR}"
                % x.type.__name__,
            )
        sql = ["BETWEEN", x.getsql()[0], a.getsql()[0], b.getsql()[0]]
        return BoolExprMonad(sql, nullable=x.nullable or a.nullable or b.nullable)


class FuncConcatMonad(FuncMonad):
    func = concat

    def call(self, *args):
        if len(args) < 2:
            throw(TranslationError, "concat() function requires at least two arguments")
        result_ast = ["CONCAT"]
        translator = self.translator
        for arg in args:
            t = arg.type
            if isinstance(t, EntityMeta) or type(t) in (tuple, SetType):
                throw(
                    TranslationError,
                    "Invalid argument of concat() function: %s" % ast2src(arg.node),
                )
            if translator.database.provider_name == "cockroach" and not isinstance(
                arg, StringMixin
            ):
                arg = arg.to_str()
            result_ast.extend(arg.getsql())
        return ExprMonad.new(
            str, result_ast, nullable=any(arg.nullable for arg in args)
        )


class FuncLenMonad(FuncMonad):
    func = len

    def call(self, x):
        return x.len()


class FuncGetattrMonad(FuncMonad):
    func = getattr

    def call(self, obj_monad, name_monad):
        if isinstance(name_monad, ConstMonad):
            attrname = name_monad.value
        elif isinstance(name_monad, ParamMonad):
            translator = self.translator.root_translator
            key = name_monad.paramkey[0]
            if key in translator.fixed_param_values:
                attrname = translator.fixed_param_values[key]
            else:
                attrname = translator.vars[key]
                translator.fixed_param_values[key] = attrname
        else:
            throw(
                TranslationError,
                "Expression `{EXPR}` cannot be translated into SQL "
                "because %s will be different for each row" % ast2src(name_monad.node),
            )
        if not isinstance(attrname, str):
            throw(
                TypeError,
                "In `{EXPR}` second argument should be a string. Got: %r" % attrname,
            )
        return obj_monad.getattr(attrname)


class FuncRawSQLMonad(FuncMonad):
    func = raw_sql

    def call(self, *args):
        throw(
            TranslationError,
            "Expression `{EXPR}` cannot be translated into SQL "
            "because raw SQL fragment will be different for each row",
        )


class FuncCountMonad(FuncMonad):
    func = itertools.count, utils.count, core.count

    def call(self, x=None, distinct=None):
        if isinstance(x, StringConstMonad) and x.value == "*":
            x = None
        if x is not None:
            return x.count(distinct)
        result = ExprMonad.new(int, ["COUNT", None], nullable=False)
        result.aggregated = True
        return result


class FuncAbsMonad(FuncMonad):
    func = abs

    def call(self, x):
        return x.abs()


class FuncSumMonad(FuncMonad):
    func = sum, core.sum

    def call(self, x, distinct=None):
        return x.aggregate("SUM", distinct)


class FuncAvgMonad(FuncMonad):
    func = utils.avg, core.avg

    def call(self, x, distinct=None):
        return x.aggregate("AVG", distinct)


class FuncGroupConcatMonad(FuncMonad):
    func = utils.group_concat, core.group_concat

    def call(self, x, sep=None, distinct=None):
        if sep is not None:
            if distinct and self.translator.database.provider.dialect == "SQLite":
                throw(
                    TypeError,
                    "SQLite does not allow to specify distinct and separator in group_concat at the same time: {EXPR}",
                )
            if not (isinstance(sep, StringConstMonad) and isinstance(sep.value, str)):
                throw(
                    TypeError,
                    "`sep` option of `group_concat` should be type of str. Got: %s"
                    % ast2src(sep.node),
                )
            sep = sep.value
        return x.aggregate("GROUP_CONCAT", distinct=distinct, sep=sep)


class FuncCoalesceMonad(FuncMonad):
    func = coalesce

    def call(self, *args):
        if len(args) < 2:
            throw(
                TranslationError, "coalesce() function requires at least two arguments"
            )
        arg = args[0].to_single_cell_value()
        t = arg.type
        result = [[sql] for sql in arg.getsql()]
        for arg in args[1:]:
            arg = arg.to_single_cell_value()
            if arg.type is not t:
                t2 = coerce_types(t, arg.type)
                if t2 is None:
                    throw(
                        TypeError,
                        "All arguments of coalesce() function should have the same type",
                    )
                t = t2
            for i, sql in enumerate(arg.getsql()):
                result[i].append(sql)
        sql = [["COALESCE"] + coalesce_args for coalesce_args in result]
        if not isinstance(t, EntityMeta):
            sql = sql[0]
        return ExprMonad.new(t, sql, nullable=all(arg.nullable for arg in args))


class FuncDistinctMonad(FuncMonad):
    func = utils.distinct, core.distinct

    def call(self, x):
        if isinstance(x, SetMixin):
            return x.call_distinct()
        if not isinstance(x, NumericMixin):
            throw(TypeError)
        result = object.__new__(x.__class__)
        result.__dict__.update(x.__dict__)
        result.forced_distinct = True
        return result


class FuncMinMonad(FuncMonad):
    func = min, core.min

    def call(self, *args):
        if not args:
            throw(TypeError, "min() function expected at least one argument")
        if len(args) == 1:
            return args[0].aggregate("MIN")
        return minmax(self, "MIN", *args)


class FuncMaxMonad(FuncMonad):
    func = max, core.max

    def call(self, *args):
        if not args:
            throw(TypeError, "max() function expected at least one argument")
        if len(args) == 1:
            return args[0].aggregate("MAX")
        return minmax(self, "MAX", *args)


def minmax(monad, sqlop, *args):
    assert len(args) > 1
    translator = monad.translator
    t = args[0].type
    if t == "METHOD":
        raise_forgot_parentheses(args[0])
    if t not in comparable_types:
        throw(
            TypeError,
            "Value of type %r is not valid as argument of %r function in expression {EXPR}"
            % (type2str(t), sqlop.lower()),
        )
    for arg in args[1:]:
        t2 = arg.type
        if t2 == "METHOD":
            raise_forgot_parentheses(arg)
        t3 = coerce_types(t, t2)
        if t3 is None:
            throw(IncomparableTypesError, t, t2)
        t = t3
    if t3 in numeric_types and translator.dialect == "PostgreSQL":
        args = list(args)
        for i, arg in enumerate(args):
            if arg.type is bool:
                args[i] = NumericExprMonad(
                    int, ["TO_INT", arg.getsql()[0]], nullable=arg.nullable
                )
    sql = [sqlop, None] + [arg.getsql()[0] for arg in args]
    return ExprMonad.new(t, sql, nullable=any(arg.nullable for arg in args))


class FuncSelectMonad(FuncMonad):
    func = core.select

    def call(self, queryset):
        if not isinstance(queryset, QuerySetMonad):
            throw(
                TypeError, "'select' function expects generator expression, got: {EXPR}"
            )
        return queryset


class FuncExistsMonad(FuncMonad):
    func = core.exists

    def call(self, arg):
        if not isinstance(arg, SetMixin):
            throw(
                TypeError,
                "'exists' function expects generator expression or collection, got: {EXPR}",
            )
        return arg.nonzero()


class FuncDescMonad(FuncMonad):
    func = core.desc

    def call(self, expr):
        return DescMonad(expr)


class DescMonad(Monad):
    def __init__(self, expr):
        Monad.__init__(self, expr.type, nullable=expr.nullable)
        self.expr = expr

    def getsql(self):
        return [["DESC", item] for item in self.expr.getsql()]


class JoinMonad(Monad):
    def __init__(self, type):
        Monad.__init__(self, type)
        translator = self.translator
        self.hint_join_prev = translator.hint_join
        translator.hint_join = True

    def __call__(self, x):
        self.translator.hint_join = self.hint_join_prev
        return x


registered_functions[JOIN] = JoinMonad


class FuncRandomMonad(FuncMonad):
    func = random

    def __init__(self, type):
        FuncMonad.__init__(self, type)
        self.translator.query_result_is_cacheable = False

    def __call__(self):
        return NumericExprMonad(float, ["RANDOM"], nullable=False)


class SetMixin(MonadMixin):
    forced_distinct = False

    def call_distinct(self):
        new_monad = object.__new__(self.__class__)
        new_monad.__dict__.update(self.__dict__)
        new_monad.forced_distinct = True
        return new_monad


def make_attrset_binop(op, sqlop):
    def attrset_binop(monad, monad2):
        return NumericSetExprMonad(op, sqlop, monad, monad2)

    return attrset_binop


class AttrSetMonad(SetMixin, Monad):
    def __init__(self, parent, attr):
        item_type = normalize_type(attr.py_type)
        Monad.__init__(self, SetType(item_type))
        self.parent = parent
        self.attr = attr
        self.sqlquery = None
        self.tableref = None

    def cmp(self, op, monad2):
        if type(monad2.type) is SetType and are_comparable_types(
            self.type.item_type, monad2.type.item_type
        ):
            pass
        elif self.type != monad2.type:
            check_comparable(self, monad2)
        throw(NotImplementedError)

    def contains(self, item, not_in=False):
        translator = self.translator
        check_comparable(item, self, "in")
        if not translator.hint_join:
            sqlop = "NOT_IN" if not_in else "IN"
            sqlquery = self._subselect()
            expr_list = sqlquery.expr_list
            from_ast = sqlquery.from_ast
            conditions = sqlquery.outer_conditions + sqlquery.conditions
            if len(expr_list) == 1:
                subquery_ast = [
                    "SELECT",
                    ["ALL"] + expr_list,
                    from_ast,
                    ["WHERE"] + conditions,
                ]
                sql_ast = [sqlop, item.getsql()[0], subquery_ast]
            elif translator.row_value_syntax:
                subquery_ast = [
                    "SELECT",
                    ["ALL"] + expr_list,
                    from_ast,
                    ["WHERE"] + conditions,
                ]
                sql_ast = [sqlop, ["ROW"] + item.getsql(), subquery_ast]
            else:
                conditions += [
                    ["EQ", expr1, expr2]
                    for expr1, expr2 in zip(item.getsql(), expr_list)
                ]
                sql_ast = [
                    "NOT_EXISTS" if not_in else "EXISTS",
                    from_ast,
                    ["WHERE"] + conditions,
                ]
            result = BoolExprMonad(sql_ast, nullable=False)
            result.nogroup = True
            return result
        elif not not_in:
            translator.distinct = True
            tableref = self.make_tableref(translator.sqlquery)
            expr_list = self.make_expr_list()
            expr_ast = sqland(
                [["EQ", expr1, expr2] for expr1, expr2 in zip(expr_list, item.getsql())]
            )
            return BoolExprMonad(expr_ast, nullable=False)
        else:
            sqlquery = SqlQuery(translator, translator.sqlquery)
            tableref = self.make_tableref(sqlquery)
            attr = self.attr
            alias, columns = tableref.make_join(pk_only=attr.reverse)
            expr_list = self.make_expr_list()
            if not attr.reverse:
                columns = attr.columns
            from_ast = translator.sqlquery.from_ast
            from_ast[0] = "LEFT_JOIN"
            from_ast.extend(sqlquery.from_ast[1:])
            conditions = [
                ["EQ", ["COLUMN", alias, column], expr]
                for column, expr in zip(columns, item.getsql())
            ]
            conditions.extend(sqlquery.conditions)
            from_ast[-1][-1] = sqland([from_ast[-1][-1]] + conditions)
            expr_ast = sqland([["IS_NULL", expr] for expr in expr_list])
            return BoolExprMonad(expr_ast, nullable=False)

    def getattr(self, name):
        try:
            return Monad.getattr(self, name)
        except AttributeError:
            pass
        entity = self.type.item_type
        if not isinstance(entity, EntityMeta):
            throw(AttributeError)
        attr = entity._adict_.get(name)
        if attr is None:
            throw(AttributeError)
        return AttrSetMonad(self, attr)

    def call_select(self):
        # calling with lambda argument processed in preCall
        return self

    call_filter = call_select

    def call_exists(self):
        return self

    def requires_distinct(self, joined=False, for_count=False):
        if self.parent.requires_distinct(joined):
            return True
        reverse = self.attr.reverse
        if not reverse:
            return True
        if reverse.is_collection:
            translator = self.translator
            if not for_count and not translator.hint_join:
                return True
            if isinstance(self.parent, AttrSetMonad):
                return True
        return False

    def count(self, distinct=None):
        translator = self.translator
        distinct = distinct_from_monad(
            distinct,
            self.requires_distinct(joined=translator.hint_join, for_count=True),
        )

        sqlquery = self._subselect()
        expr_list = sqlquery.expr_list
        from_ast = sqlquery.from_ast
        inner_conditions = sqlquery.conditions
        outer_conditions = sqlquery.outer_conditions

        sql_ast = make_aggr = None
        extra_grouping = False
        if not distinct and self.tableref.name_path != translator.optimize:
            make_aggr = lambda expr_list: ["COUNT", None]
        elif len(expr_list) == 1:
            make_aggr = lambda expr_list: ["COUNT", True] + expr_list
        elif translator.dialect == "Oracle":
            if self.tableref.name_path == translator.optimize:
                alias, pk_columns = self.tableref.make_join(pk_only=True)
                make_aggr = lambda expr_list: [
                    "COUNT",
                    distinct,
                    ["COLUMN", alias, "ROWID"],
                ]
            else:
                extra_grouping = True
                if translator.hint_join:
                    make_aggr = lambda expr_list: ["COUNT", None]
                else:
                    make_aggr = lambda expr_list: ["COUNT", None, ["COUNT", None]]
        elif translator.dialect == "PostgreSQL":
            row = ["ROW"] + expr_list
            cond = ["IS_NULL", row]
            if translator.database.provider_name == "cockroach":
                cond = ["OR"] + [["IS_NULL", expr] for expr in expr_list]
            expr = ["CASE", None, [[cond, ["VALUE", None]]], row]
            make_aggr = lambda expr_list: ["COUNT", True, expr]
        elif translator.row_value_syntax:
            make_aggr = lambda expr_list: ["COUNT", True] + expr_list
        elif translator.dialect == "SQLite":
            if not distinct:
                alias, pk_columns = self.tableref.make_join(pk_only=True)
                make_aggr = lambda expr_list: [
                    "COUNT",
                    None,
                    ["COLUMN", alias, "ROWID"],
                ]
            elif translator.hint_join:  # Same join as in Oracle
                extra_grouping = True
                make_aggr = lambda expr_list: ["COUNT", None]
            elif translator.sqlite_version < (3, 6, 21):
                alias, pk_columns = self.tableref.make_join(pk_only=False)
                make_aggr = lambda expr_list: [
                    "COUNT",
                    True,
                    ["COLUMN", alias, "ROWID"],
                ]
            else:
                sql_ast = [
                    "SELECT",
                    ["AGGREGATES", ["COUNT", None]],
                    [
                        "FROM",
                        [
                            "t",
                            "SELECT",
                            [
                                ["DISTINCT"] + expr_list,
                                from_ast,
                                ["WHERE"] + outer_conditions + inner_conditions,
                            ],
                        ],
                    ],
                ]
        else:
            throw(NotImplementedError)  # pragma: no cover
        if sql_ast:
            optimized = False
        elif translator.hint_join:
            sql_ast, optimized = self._joined_subselect(
                make_aggr, extra_grouping, coalesce_to_zero=True
            )
        else:
            sql_ast, optimized = self._aggregated_scalar_subselect(
                make_aggr, extra_grouping
            )
        translator.aggregated_subquery_paths.add(self.tableref.name_path)
        result = ExprMonad.new(int, sql_ast, nullable=False)
        if optimized:
            result.aggregated = True
        else:
            result.nogroup = True
        return result

    len = count

    def aggregate(self, func_name, distinct=None, sep=None):
        distinct = distinct_from_monad(
            distinct, default=self.forced_distinct and func_name in ("SUM", "AVG")
        )
        translator = self.translator
        item_type = self.type.item_type

        if func_name in ("SUM", "AVG"):
            if item_type not in numeric_types:
                throw(
                    TypeError,
                    "Function %s() expects query or items of numeric type, got %r in {EXPR}"
                    % (func_name.lower(), type2str(item_type)),
                )
        elif func_name in ("MIN", "MAX"):
            if item_type not in comparable_types:
                throw(
                    TypeError,
                    "Function %s() expects query or items of comparable type, got %r in {EXPR}"
                    % (func_name.lower(), type2str(item_type)),
                )
        elif func_name == "GROUP_CONCAT":
            if isinstance(item_type, EntityMeta) and item_type._pk_is_composite_:
                throw(
                    TypeError,
                    "`group_concat` cannot be used with entity with composite primary key",
                )
        else:
            assert False  # pragma: no cover

        def make_aggr(expr_list):
            result = [func_name, distinct] + expr_list
            if sep is not None:
                assert func_name == "GROUP_CONCAT"
                result.append(["VALUE", sep])
            return result

        # make_aggr = lambda expr_list: [ func_name, distinct ] + expr_list

        if translator.hint_join:
            sql_ast, optimized = self._joined_subselect(
                make_aggr, coalesce_to_zero=(func_name == "SUM")
            )
        else:
            sql_ast, optimized = self._aggregated_scalar_subselect(make_aggr)

        if func_name == "AVG":
            result_type = float
        elif func_name == "GROUP_CONCAT":
            result_type = str
        else:
            result_type = item_type
        translator.aggregated_subquery_paths.add(self.tableref.name_path)
        result = ExprMonad.new(result_type, sql_ast, nullable=func_name != "SUM")
        if optimized:
            result.aggregated = True
        else:
            result.nogroup = True
        return result

    def nonzero(self):
        sqlquery = self._subselect()
        sql_ast = [
            "EXISTS",
            sqlquery.from_ast,
            ["WHERE"] + sqlquery.outer_conditions + sqlquery.conditions,
        ]
        return BoolExprMonad(sql_ast, nullable=False)

    def negate(self):
        sqlquery = self._subselect()
        sql_ast = [
            "NOT_EXISTS",
            sqlquery.from_ast,
            ["WHERE"] + sqlquery.outer_conditions + sqlquery.conditions,
        ]
        return BoolExprMonad(sql_ast, nullable=False)

    call_is_empty = negate

    def make_tableref(self, sqlquery):
        parent = self.parent
        attr = self.attr
        if isinstance(parent, ObjectMixin):
            parent_tableref = parent.tableref
        elif isinstance(parent, AttrSetMonad):
            parent_tableref = parent.make_tableref(sqlquery)
        else:
            assert False  # pragma: no cover
        if attr.reverse:
            name_path = parent_tableref.name_path + "-" + attr.name
            self.tableref = sqlquery.get_tableref(name_path) or sqlquery.add_tableref(
                name_path, parent_tableref, attr
            )
        else:
            self.tableref = parent_tableref
        self.tableref.can_affect_distinct = True
        return self.tableref

    def make_expr_list(self):
        attr = self.attr
        pk_only = attr.reverse or attr.pk_offset is not None
        alias, columns = self.tableref.make_join(pk_only)
        if attr.reverse:
            pass
        elif pk_only:
            offset = attr.pk_columns_offset
            columns = columns[offset : offset + len(attr.columns)]
        else:
            columns = attr.columns
        return [["COLUMN", alias, column] for column in columns]

    def _aggregated_scalar_subselect(self, make_aggr, extra_grouping=False):
        translator = self.translator
        sqlquery = self._subselect()
        optimized = False
        if translator.optimize == self.tableref.name_path:
            sql_ast = make_aggr(sqlquery.expr_list)
            optimized = True
            if not translator.from_optimized:
                from_ast = self.sqlquery.from_ast[1:]
                assert sqlquery.outer_conditions
                from_ast[0] = from_ast[0] + [sqland(sqlquery.outer_conditions)]
                translator.sqlquery.from_ast.extend(from_ast)
                translator.from_optimized = True
        else:
            sql_ast = [
                "SELECT",
                ["AGGREGATES", make_aggr(sqlquery.expr_list)],
                sqlquery.from_ast,
                ["WHERE"] + sqlquery.outer_conditions + sqlquery.conditions,
            ]
        if extra_grouping:  # This is for Oracle only, with COUNT(COUNT(*))
            sql_ast.append(["GROUP_BY"] + sqlquery.expr_list)
        return sql_ast, optimized

    def _joined_subselect(
        self, make_aggr, extra_grouping=False, coalesce_to_zero=False
    ):
        translator = self.translator
        sqlquery = self._subselect()
        expr_list = sqlquery.expr_list
        from_ast = sqlquery.from_ast
        inner_conditions = sqlquery.conditions
        outer_conditions = sqlquery.outer_conditions

        groupby_columns = [
            inner_column[:] for cond, outer_column, inner_column in outer_conditions
        ]
        assert len({alias for _, alias, column in groupby_columns}) == 1

        if extra_grouping:
            inner_alias = translator.sqlquery.make_alias("t")
            inner_columns = ["DISTINCT"]
            col_mapping = {}
            col_names = set()
            for i, column_ast in enumerate(groupby_columns + expr_list):
                assert column_ast[0] == "COLUMN"
                tname, cname = column_ast[1:]
                if cname not in col_names:
                    col_mapping[tname, cname] = cname
                    col_names.add(cname)
                    expr = ["AS", column_ast, cname]
                    new_name = cname
                else:
                    new_name = "expr-%d" % next(translator.sqlquery.expr_counter)
                    col_mapping[tname, cname] = new_name
                    expr = ["AS", column_ast, new_name]
                inner_columns.append(expr)
                if i < len(groupby_columns):
                    groupby_columns[i] = ["COLUMN", inner_alias, new_name]
            inner_select = [inner_columns, from_ast]
            if inner_conditions:
                inner_select.append(["WHERE"] + inner_conditions)
            from_ast = ["FROM", [inner_alias, "SELECT", inner_select]]
            outer_conditions = outer_conditions[:]
            for i, (cond, outer_column, inner_column) in enumerate(outer_conditions):
                assert inner_column[0] == "COLUMN"
                tname, cname = inner_column[1:]
                new_name = col_mapping[tname, cname]
                outer_conditions[i] = [
                    cond,
                    outer_column,
                    ["COLUMN", inner_alias, new_name],
                ]

        subselect_columns = ["ALL"]
        for column_ast in groupby_columns:
            assert column_ast[0] == "COLUMN"
            subselect_columns.append(["AS", column_ast, column_ast[2]])
        expr_name = "expr-%d" % next(translator.sqlquery.expr_counter)
        subselect_columns.append(["AS", make_aggr(expr_list), expr_name])
        subquery_ast = [subselect_columns, from_ast]
        if inner_conditions and not extra_grouping:
            subquery_ast.append(["WHERE"] + inner_conditions)
        subquery_ast.append(["GROUP_BY"] + groupby_columns)

        alias = translator.sqlquery.make_alias("t")
        for cond in outer_conditions:
            cond[2][1] = alias
        translator.sqlquery.from_ast.append(
            [alias, "SELECT", subquery_ast, sqland(outer_conditions)]
        )
        expr_ast = ["COLUMN", alias, expr_name]
        if coalesce_to_zero:
            expr_ast = ["COALESCE", expr_ast, ["VALUE", 0]]
        return expr_ast, False

    def _subselect(self, sqlquery=None, extract_outer_conditions=True):
        if self.sqlquery is not None:
            return self.sqlquery
        attr = self.attr
        translator = self.translator
        if sqlquery is None:
            sqlquery = SqlQuery(translator, translator.sqlquery)
        self.make_tableref(sqlquery)
        sqlquery.expr_list = self.make_expr_list()
        if not attr.reverse and not attr.is_required:
            sqlquery.conditions.extend(
                ["IS_NOT_NULL", expr] for expr in sqlquery.expr_list
            )
        if sqlquery is not translator.sqlquery and extract_outer_conditions:
            outer_cond = sqlquery.from_ast[1].pop()
            if outer_cond[0] == "AND":
                sqlquery.outer_conditions = outer_cond[1:]
            else:
                sqlquery.outer_conditions = [outer_cond]
        self.sqlquery = sqlquery
        return sqlquery

    def getsql(self, sqlquery=None):
        if sqlquery is None:
            sqlquery = self.translator.sqlquery
        self.make_tableref(sqlquery)
        return self.make_expr_list()

    __add__ = make_attrset_binop("+", "ADD")
    __sub__ = make_attrset_binop("-", "SUB")
    __mul__ = make_attrset_binop("*", "MUL")
    __truediv__ = make_attrset_binop("/", "DIV")
    __floordiv__ = make_attrset_binop("//", "FLOORDIV")


def make_numericset_binop(op, sqlop):
    def numericset_binop(monad, monad2):
        return NumericSetExprMonad(op, sqlop, monad, monad2)

    return numericset_binop


class NumericSetExprMonad(SetMixin, Monad):
    def __init__(self, op, sqlop, left, right):
        result_type, left, right = coerce_monads(left, right)
        assert type(result_type) is SetType
        if result_type.item_type not in numeric_types:
            throw(
                TypeError,
                _binop_errmsg % (type2str(left.type), type2str(right.type), op),
            )
        Monad.__init__(self, result_type)
        self.op = op
        self.sqlop = sqlop
        self.left = left
        self.right = right

    def aggregate(self, func_name, distinct=None, sep=None):
        distinct = distinct_from_monad(
            distinct, default=self.forced_distinct and func_name in ("SUM", "AVG")
        )
        translator = self.translator
        sqlquery = SqlQuery(translator, translator.sqlquery)
        expr = self.getsql(sqlquery)[0]
        translator.aggregated_subquery_paths.add(self.tableref.name_path)
        outer_cond = sqlquery.from_ast[1].pop()
        if outer_cond[0] == "AND":
            sqlquery.outer_conditions = outer_cond[1:]
        else:
            sqlquery.outer_conditions = [outer_cond]
        if func_name == "AVG":
            result_type = float
        elif func_name == "GROUP_CONCAT":
            result_type = str
        else:
            result_type = self.type.item_type
        aggr_ast = [func_name, distinct, expr]
        if func_name == "GROUP_CONCAT":
            if sep is not None:
                aggr_ast.append(["VALUE", sep])
        if translator.optimize != self.tableref.name_path:
            sql_ast = [
                "SELECT",
                ["AGGREGATES", aggr_ast],
                sqlquery.from_ast,
                ["WHERE"] + sqlquery.outer_conditions + sqlquery.conditions,
            ]
            result = ExprMonad.new(result_type, sql_ast, nullable=func_name != "SUM")
            result.nogroup = True
        else:
            if not translator.from_optimized:
                from_ast = sqlquery.from_ast[1:]
                assert sqlquery.outer_conditions
                from_ast[0] = from_ast[0] + [sqland(sqlquery.outer_conditions)]
                translator.sqlquery.from_ast.extend(from_ast)
                translator.from_optimized = True
            sql_ast = aggr_ast
            result = ExprMonad.new(result_type, sql_ast, nullable=func_name != "SUM")
            result.aggregated = True
        return result

    def getsql(self, sqlquery=None):
        if sqlquery is None:
            sqlquery = self.translator.sqlquery
        left, right = self.left, self.right
        left_expr = left.getsql(sqlquery)[0]
        right_expr = right.getsql(sqlquery)[0]
        if isinstance(left, NumericMixin):
            left_path = ""
        else:
            left_path = left.tableref.name_path + "-"
        if isinstance(right, NumericMixin):
            right_path = ""
        else:
            right_path = right.tableref.name_path + "-"
        if left_path.startswith(right_path):
            tableref = left.tableref
        elif right_path.startswith(left_path):
            tableref = right.tableref
        else:
            throw(
                TranslationError,
                "Cartesian product detected in %s" % ast2src(self.node),
            )
        self.tableref = tableref
        return [[self.sqlop, left_expr, right_expr]]

    __add__ = make_numericset_binop("+", "ADD")
    __sub__ = make_numericset_binop("-", "SUB")
    __mul__ = make_numericset_binop("*", "MUL")
    __truediv__ = make_numericset_binop("/", "DIV")
    __floordiv__ = make_numericset_binop("//", "FLOORDIV")


class QuerySetMonad(SetMixin, Monad):
    nogroup = True

    def __init__(self, subtranslator):
        item_type = subtranslator.expr_type
        monad_type = SetType(item_type)
        Monad.__init__(self, monad_type)
        self.subtranslator = subtranslator
        self.item_type = item_type
        self.limit = self.offset = None

    def to_single_cell_value(self):
        return ExprMonad.new(self.item_type, self.getsql()[0])

    def requires_distinct(self, joined=False):
        assert False

    def call_limit(self, limit=None, offset=None):
        if limit is not None and not isinstance(limit, int_types):
            if not isinstance(limit, (NoneMonad, NumericConstMonad)):
                throw(TypeError, "`limit` parameter should be of int type")
            limit = limit.value
        if offset is not None and not isinstance(offset, int_types):
            if not isinstance(offset, (NoneMonad, NumericConstMonad)):
                throw(TypeError, "`offset` parameter should be of int type")
            offset = offset.value
        self.limit = limit
        self.offset = offset
        return self

    def contains(self, item, not_in=False):
        translator = self.translator
        check_comparable(item, self, "in")
        if isinstance(item, ListMonad):
            item_columns = []
            for subitem in item.items:
                item_columns.extend(subitem.getsql())
        else:
            item_columns = item.getsql()

        sub = self.subtranslator
        if translator.hint_join and len(sub.sqlquery.from_ast[1]) == 3:
            subquery_ast = sub.construct_subquery_ast(
                self.limit, self.offset, distinct=False
            )
            select_ast, from_ast, where_ast = subquery_ast[1:4]
            sqlquery = translator.sqlquery
            if not not_in:
                translator.distinct = True
                if sqlquery.from_ast[0] == "FROM":
                    sqlquery.from_ast[0] = "INNER_JOIN"
            else:
                sqlquery.left_join = True
                sqlquery.from_ast[0] = "LEFT_JOIN"
            col_names = set()
            new_names = []
            for i, column_ast in enumerate(select_ast):
                if not i:
                    continue  # 'ALL'
                if column_ast[0] == "COLUMN":
                    tab_name, col_name = column_ast[1:]
                    if col_name not in col_names:
                        col_names.add(col_name)
                        new_names.append(col_name)
                        select_ast[i] = ["AS", column_ast, col_name]
                        continue
                new_name = "expr-%d" % next(sqlquery.expr_counter)
                new_names.append(new_name)
                select_ast[i] = ["AS", column_ast, new_name]

            alias = sqlquery.make_alias("t")
            outer_conditions = [
                ["EQ", item_column, ["COLUMN", alias, new_name]]
                for item_column, new_name in zip(item_columns, new_names)
            ]
            sqlquery.from_ast.append(
                [alias, "SELECT", subquery_ast[1:], sqland(outer_conditions)]
            )
            if not_in:
                sql_ast = sqland(
                    [["IS_NULL", ["COLUMN", alias, new_name]] for new_name in new_names]
                )
            else:
                sql_ast = ["EQ", ["VALUE", 1], ["VALUE", 1]]
        else:
            if len(item_columns) == 1:
                subquery_ast = sub.construct_subquery_ast(
                    self.limit, self.offset, distinct=False, is_not_null_checks=not_in
                )
                sql_ast = ["NOT_IN" if not_in else "IN", item_columns[0], subquery_ast]
            elif translator.row_value_syntax:
                subquery_ast = sub.construct_subquery_ast(
                    self.limit, self.offset, distinct=False, is_not_null_checks=not_in
                )
                sql_ast = [
                    "NOT_IN" if not_in else "IN",
                    ["ROW"] + item_columns,
                    subquery_ast,
                ]
            else:
                ambiguous_names = set()
                if sub.injected:
                    for name in translator.sqlquery.tablerefs:
                        if name in sub.sqlquery.tablerefs:
                            ambiguous_names.add(name)
                subquery_ast = sub.construct_subquery_ast(
                    self.limit, self.offset, distinct=False
                )
                if ambiguous_names:
                    select_ast = subquery_ast[1]
                    expr_aliases = []
                    for i, expr_ast in enumerate(select_ast):
                        if i > 0:
                            if expr_ast[0] == "AS":
                                expr_ast = expr_ast[1]
                            expr_alias = "expr-%d" % i
                            expr_aliases.append(expr_alias)
                            expr_ast = ["AS", expr_ast, expr_alias]
                            select_ast[i] = expr_ast

                    new_table_alias = translator.sqlquery.make_alias("t")
                    new_select_ast = ["ALL"]
                    for expr_alias in expr_aliases:
                        new_select_ast.append(["COLUMN", new_table_alias, expr_alias])
                    new_from_ast = [
                        "FROM",
                        [new_table_alias, "SELECT", subquery_ast[1:]],
                    ]
                    new_where_ast = ["WHERE"]
                    subquery_ast = [
                        "SELECT",
                        new_select_ast,
                        new_from_ast,
                        new_where_ast,
                    ]
                select_ast, from_ast, where_ast = subquery_ast[1:4]
                in_conditions = [
                    ["EQ", expr1, expr2]
                    for expr1, expr2 in zip(item_columns, select_ast[1:])
                ]
                if not ambiguous_names and sub.aggregated:
                    having_ast = find_or_create_having_ast(subquery_ast)
                    having_ast += in_conditions
                else:
                    where_ast += in_conditions
                sql_ast = ["NOT_EXISTS" if not_in else "EXISTS"] + subquery_ast[2:]
        return BoolExprMonad(sql_ast, nullable=False)

    def nonzero(self):
        subquery_ast = self.subtranslator.construct_subquery_ast(distinct=False)
        expr_monads = self.subtranslator.expr_monads
        if len(expr_monads) > 1:
            throw(NotImplementedError)
        expr_monad = expr_monads[0]
        if not isinstance(expr_monad, ObjectIterMonad):
            sql = expr_monad.nonzero().getsql()
            assert subquery_ast[3][0] == "WHERE"
            subquery_ast[3].append(sql[0])
        subquery_ast = ["EXISTS"] + subquery_ast[2:]
        return BoolExprMonad(subquery_ast, nullable=False)

    def negate(self):
        sql = self.nonzero().sql
        assert sql[0] == "EXISTS"
        return BoolExprMonad(["NOT_EXISTS"] + sql[1:], nullable=False)

    def count(self, distinct=None):
        distinct = distinct_from_monad(distinct)
        translator = self.translator
        sub = self.subtranslator

        if sub.aggregated:
            throw(TranslationError, "Too complex aggregation in {EXPR}")
        subquery_ast = sub.construct_subquery_ast(distinct=False)
        from_ast, where_ast = subquery_ast[2:4]
        sql_ast = None

        expr_type = sub.expr_type
        if isinstance(expr_type, (tuple, EntityMeta)):
            if not sub.distinct and not distinct:
                select_ast = ["AGGREGATES", ["COUNT", None]]
            elif len(sub.expr_columns) == 1:
                select_ast = [
                    "AGGREGATES",
                    ["COUNT", True if distinct is None else distinct]
                    + sub.expr_columns,
                ]
            elif translator.dialect == "Oracle":
                sql_ast = [
                    "SELECT",
                    ["AGGREGATES", ["COUNT", None, ["COUNT", None]]],
                    from_ast,
                    where_ast,
                    ["GROUP_BY"] + sub.expr_columns,
                ]
            elif translator.row_value_syntax:
                select_ast = [
                    "AGGREGATES",
                    ["COUNT", True if distinct is None else distinct]
                    + sub.expr_columns,
                ]
            elif translator.dialect == "SQLite":
                if translator.sqlite_version < (3, 6, 21):
                    if sub.aggregated:
                        throw(TranslationError)
                    alias, pk_columns = sub.tableref.make_join(pk_only=False)
                    subquery_ast = sub.construct_subquery_ast(distinct=False)
                    from_ast, where_ast = subquery_ast[2:4]
                    sql_ast = [
                        "SELECT",
                        [
                            "AGGREGATES",
                            [
                                "COUNT",
                                True if distinct is None else distinct,
                                ["COLUMN", alias, "ROWID"],
                            ],
                        ],
                        from_ast,
                        where_ast,
                    ]
                else:
                    alias = translator.sqlquery.make_alias("t")
                    sql_ast = [
                        "SELECT",
                        ["AGGREGATES", ["COUNT", None]],
                        [
                            "FROM",
                            [
                                alias,
                                "SELECT",
                                [
                                    ["DISTINCT" if distinct is not False else "ALL"]
                                    + sub.expr_columns,
                                    from_ast,
                                    where_ast,
                                ],
                            ],
                        ],
                    ]
            else:
                assert False  # pragma: no cover
        elif len(sub.expr_columns) == 1:
            select_ast = [
                "AGGREGATES",
                ["COUNT", True if distinct is None else distinct, sub.expr_columns[0]],
            ]
        else:
            throw(NotImplementedError)  # pragma: no cover

        if sql_ast is None:
            sql_ast = ["SELECT", select_ast, from_ast, where_ast]
        return ExprMonad.new(int, sql_ast, nullable=False)

    len = count

    def aggregate(self, func_name, distinct=None, sep=None):
        distinct = distinct_from_monad(
            distinct, default=self.forced_distinct and func_name in ("SUM", "AVG")
        )
        sub = self.subtranslator
        if sub.aggregated:
            throw(TranslationError, "Too complex aggregation in {EXPR}")
        subquery_ast = sub.construct_subquery_ast(distinct=False)
        from_ast, where_ast = subquery_ast[2:4]
        expr_type = sub.expr_type
        if func_name in ("SUM", "AVG"):
            if expr_type not in numeric_types:
                throw(
                    TypeError,
                    "Function %s() expects query or items of numeric type, got %r in {EXPR}"
                    % (func_name.lower(), type2str(expr_type)),
                )
        elif func_name in ("MIN", "MAX"):
            if expr_type not in comparable_types:
                throw(
                    TypeError,
                    "Function %s() cannot be applied to type %r in {EXPR}"
                    % (func_name.lower(), type2str(expr_type)),
                )
        elif func_name == "GROUP_CONCAT":
            if isinstance(expr_type, EntityMeta) and expr_type._pk_is_composite_:
                throw(
                    TypeError,
                    "`group_concat` cannot be used with entity with composite primary key",
                )
        else:
            assert False  # pragma: no cover
        assert len(sub.expr_columns) == 1
        aggr_ast = [func_name, distinct, sub.expr_columns[0]]
        if func_name == "GROUP_CONCAT":
            if sep is not None:
                aggr_ast.append(["VALUE", sep])
        select_ast = ["AGGREGATES", aggr_ast]
        sql_ast = ["SELECT", select_ast, from_ast, where_ast]
        if func_name == "AVG":
            result_type = float
        elif func_name == "GROUP_CONCAT":
            result_type = str
        else:
            result_type = expr_type
        return ExprMonad.new(result_type, sql_ast, func_name != "SUM")

    def call_count(self, distinct=None):
        return self.count(distinct=distinct)

    def call_sum(self, distinct=None):
        return self.aggregate("SUM", distinct)

    def call_min(self):
        return self.aggregate("MIN")

    def call_max(self):
        return self.aggregate("MAX")

    def call_avg(self, distinct=None):
        return self.aggregate("AVG", distinct)

    def call_group_concat(self, sep=None, distinct=None):
        if sep is not None:
            if not isinstance(sep, str):
                throw(
                    TypeError,
                    "`sep` option of `group_concat` should be type of str. Got: %s"
                    % type(sep).__name__,
                )
        return self.aggregate("GROUP_CONCAT", distinct, sep=sep)

    def getsql(self):
        return [self.subtranslator.construct_subquery_ast(self.limit, self.offset)]


def find_or_create_having_ast(sections):
    groupby_offset = None
    for i, section in enumerate(sections):
        section_name = section[0]
        if section_name == "GROUP_BY":
            groupby_offset = i
        elif section_name == "HAVING":
            return section
    having_ast = ["HAVING"]
    sections.insert(groupby_offset + 1, having_ast)
    return having_ast
