from __future__ import annotations

from binascii import hexlify
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Concatenate,
    Literal,
    ParamSpec,
    TypeAlias,
    TypeVar,
    cast,
)

from pony import options
from pony.converting import timedelta2str
from pony.orm.core import (
    Adapter,
    Attribute,
    Entity,
    OptAttrValue,
    ParamKey,
    QueryResult,
    QueryVars,
)
from pony.orm.ormtypes import RawSQL
from pony.py23compat import int_types
from pony.utils import datetime2timestamp, is_ident, throw

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Sequence

    import _typeshed
    from typing_extensions import Unpack

    from pony.orm.dbapiprovider import (
        Converter,
        DBAPIProvider,
        StrConverter,
    )

# An SQL AST node is a sequence whose first item is the name of the SQLBuilder
# method that renders it. The other items are the arguments of that method:
# strings, numbers, parameters, converters, types or nested nodes (Any, since
# the items are heterogeneous).
SqlAst: TypeAlias = "Sequence[Any]"

_T = TypeVar("_T", bound="str | Value | Param | Sequence")
_PT = TypeVar("_PT", bound="Param")
_P = ParamSpec("_P")


class AstError(Exception):
    pass


class Param:
    __slots__ = "style", "id", "paramkey", "converter", "optimistic"
    style: str
    id: int | None
    paramkey: ParamKey
    converter: Converter | None
    optimistic: bool

    def __init__(
        self,
        paramstyle: str,
        paramkey: ParamKey,
        converter: Converter | None = None,
        optimistic: bool = False,
    ) -> None:
        self.style = paramstyle
        self.id = None
        self.paramkey = paramkey
        self.converter = converter
        self.optimistic = optimistic

    def eval(self, values: QueryVars) -> Any:
        varkey, i, j = self.paramkey
        value: Any = values[varkey]
        if i is not None:
            t = type(value)
            if t is tuple:
                assert isinstance(value, tuple)
                value = value[i]
            elif t is RawSQL:
                assert isinstance(value, RawSQL)
                value = value.values[i]
            elif hasattr(value, "_get_items"):
                assert isinstance(value, QueryResult)
                value = value._get_items()[i]
            else:
                assert False, t
        if j is not None:
            assert type(type(value)).__name__ == "EntityMeta"
            assert isinstance(value, Entity)
            value = value._get_raw_pkval_()[j]
        converter = self.converter
        if value is not None and converter is not None:
            if converter.attr is None:
                value = converter.val2dbval(value)
            value = converter.py2sql(value)
        return value

    def __str__(self):
        paramstyle = self.style
        if paramstyle == "qmark":
            return "?"
        elif paramstyle == "format":
            return "%s"
        elif paramstyle == "numeric":
            return ":%d" % self.id
        elif paramstyle == "named":
            return ":p%d" % self.id
        elif paramstyle == "pyformat":
            return "%%(p%d)s" % self.id
        else:
            throw(NotImplementedError)

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.paramkey)


class CompositeParam(Param):
    __slots__ = "items", "func"

    def __init__(
        self,
        paramstyle: str,
        paramkey: tuple[ParamKey | str, ...],
        items: list[Param | Value],
        func: Callable,
    ) -> None:
        for item in items:
            assert isinstance(item, (Param, Value)), item
        Param.__init__(self, paramstyle, paramkey)  # type: ignore[arg-type]
        self.items = items
        self.func = func

    def eval(
        self,
        values: (
            QueryVars
            | dict[Attribute, Entity | OptAttrValue]
            | list[Entity]
            | list[OptAttrValue]
            | tuple[int | str, ...]
        ),
    ) -> str:
        args = [
            item.eval(values) if isinstance(item, Param) else item.value  # type: ignore[arg-type]
            for item in self.items
        ]
        return self.func(args)


class Value:
    __slots__ = "paramstyle", "value"

    def __init__(self, paramstyle: str, value: OptAttrValue) -> None:
        self.paramstyle = paramstyle
        self.value = value

    def __str__(self):
        value = self.value
        if value is None:
            return "null"
        if isinstance(value, bool):
            return value and "1" or "0"
        if isinstance(value, str):
            return self.quote_str(value)
        if isinstance(value, datetime):
            return "TIMESTAMP " + self.quote_str(datetime2timestamp(value))
        if isinstance(value, date):
            return "DATE " + self.quote_str(str(value))
        if isinstance(value, timedelta):
            return "INTERVAL '%s' HOUR TO SECOND" % timedelta2str(value)
        if isinstance(value, (int, float, Decimal)):
            return str(value)
        if isinstance(value, bytes):
            return "X'%s'" % hexlify(value).decode("ascii")
        assert False, repr(value)  # pragma: no cover

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.value)

    def quote_str(self, s: str) -> str:
        if self.paramstyle in ("format", "pyformat"):
            s = s.replace("%", "%%")
        return "'%s'" % s.replace("'", "''")


def flat(tree: Any) -> list:
    stack = [tree]
    result: list[Any] = []
    stack_pop = stack.pop
    stack_extend = stack.extend
    result_append = result.append
    while stack:
        x = stack_pop()
        if isinstance(x, str):
            result_append(x)
        else:
            try:
                stack_extend(x)
            except TypeError:
                result_append(x)
    return result[::-1]


def flat_conditions(
    conditions: (
        tuple[
            list[
                str
                | list[str | float | bool | None | type[str | float | bool | None]]
                | Converter
                | list
                | tuple
            ],
            ...,
        ]
        | tuple[  # noqa: Y090
            tuple[str, list[str | float | Converter | Sequence], bool, list[str]]
        ]
    ),
) -> list[
    (
        list[
            str
            | list[
                str | int | float | bool | None | type[str | int | float | bool | None]
            ]
            | Converter
            | list
            | tuple
        ]
        | tuple[str, list[str | int | float | Converter | Sequence], bool, list[str]]
    )
]:
    result = []
    for condition in conditions:
        if condition[0] == "AND":
            result.extend(flat_conditions(condition[1:]))  # type: ignore[arg-type]
        else:
            result.append(condition)
    return result


def join(delimiter: str, items: Iterable[_T]) -> list[_T | str]:
    items = iter(items)
    try:
        result: list[_T | str] = [next(items)]
    except StopIteration:
        return []
    for item in items:
        result.append(delimiter)
        result.append(item)
    return result


def move_conditions_from_inner_join_to_where(
    sections: tuple[SqlAst, ...],
) -> Sequence[SqlAst]:
    new_sections = list(sections)
    for i, section in enumerate(sections):
        if section[0] == "FROM":
            new_from_list = ["FROM"] + [list(item) for item in section[1:]]
            new_sections[i] = new_from_list
            if len(sections) > i + 1 and sections[i + 1][0] == "WHERE":
                new_where_list = list(sections[i + 1])
                new_sections[i + 1] = new_where_list
            else:
                new_where_list = ["WHERE"]
                new_sections.insert(i + 1, new_where_list)
            break
    else:
        return sections
    for join in new_from_list[2:]:
        assert isinstance(join, list)
        if join[1] in ("TABLE", "SELECT") and len(join) == 4:
            new_where_list.append(join.pop())
    return new_sections


def make_binary_op(symbol: str, default_parentheses: bool = False) -> Callable:
    def binary_op(builder, expr1, expr2, parentheses=None):
        if parentheses is None:
            parentheses = default_parentheses
        if parentheses:
            return "(", builder(expr1), symbol, builder(expr2), ")"
        return builder(expr1), symbol, builder(expr2)

    return binary_op


def make_unary_func(symbol: str) -> Callable:
    def unary_func(builder, expr):
        return "%s(" % symbol, builder(expr), ")"

    return unary_func


def indentable(
    method: Callable[Concatenate[SQLBuilder, _P], _T],
) -> Callable[Concatenate[SQLBuilder, _P], _T | tuple[str, _T]]:
    def new_method(
        builder: SQLBuilder, *args: _P.args, **kwargs: _P.kwargs
    ) -> _T | tuple[str, _T]:
        result = method(builder, *args, **kwargs)
        if builder.indent <= 1:
            return result
        return builder.indent_spaces * (builder.indent - 1), result

    new_method.__name__ = method.__name__
    return new_method


class SQLBuilder:
    provider: DBAPIProvider
    quote_name: Callable[[str | Iterable[str]], str]
    paramstyle: str
    ast: list[str | list | None]
    indent: int
    keys: dict[str | ParamKey | tuple[ParamKey | str, ...], Param]
    inner_join_syntax: bool
    suppress_aliases: bool
    result: list[str | Value | Param]
    params: tuple[Param, ...]
    layout: list[str | ParamKey | tuple[ParamKey | str, ...]]
    sql: str
    adapter: Adapter

    dialect: ClassVar[str | _typeshed.MaybeNone] = None
    param_class: ClassVar[type[Param]] = Param
    composite_param_class: ClassVar[type[CompositeParam]] = CompositeParam
    value_class: type[Value] = Value
    indent_spaces: ClassVar[str] = " " * 4
    least_func_name: ClassVar[str] = "least"
    greatest_func_name: ClassVar[str] = "greatest"

    def __init__(self, provider: DBAPIProvider, ast: list[str | list | None]) -> None:
        self.provider = provider
        self.quote_name = provider.quote_name
        self.paramstyle = paramstyle = provider.paramstyle
        self.ast = ast
        self.indent = 0
        self.keys = {}
        self.inner_join_syntax = options.INNER_JOIN_SYNTAX
        self.suppress_aliases = False
        self.result = flat(self(ast))
        params = tuple(x for x in self.result if isinstance(x, Param))
        layout: list[str | ParamKey | tuple[ParamKey | str, ...]] = []
        for i, param in enumerate(params):
            if param.id is None:
                param.id = i + 1
            layout.append(param.paramkey)
        self.layout = layout
        self.sql = "".join(map(str, self.result)).rstrip("\n")
        if paramstyle in ("qmark", "format"):

            def adapter(values):  # pyright: ignore[reportRedeclaration]
                return tuple(param.eval(values) for param in params)

        elif paramstyle == "numeric":

            def adapter(values):  # pyright: ignore[reportRedeclaration]
                return tuple(param.eval(values) for param in params)

        elif paramstyle in ("named", "pyformat"):

            def adapter(values):
                return {"p%d" % param.id: param.eval(values) for param in params}

        else:
            throw(NotImplementedError, paramstyle)
        self.params = params
        self.adapter = adapter

    def __call__(self, ast: SqlAst) -> Any:
        if isinstance(ast, str):
            throw(AstError, "An SQL AST list was expected. Got string: %r" % ast)
        symbol = ast[0]
        if not isinstance(symbol, str):
            throw(AstError, "Invalid node name in AST: %r" % ast)
        method = getattr(self, symbol, None)
        if method is None:
            throw(AstError, "Method not found: %s" % symbol)
        try:
            assert method is not None
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
    def INSERT(
        self,
        table_name: str,
        columns: Iterable[str | Iterable],
        values: Iterable[Sequence[str | tuple[int, None, None] | Converter]],
        returning: str | None = None,
    ) -> SqlAst:
        return [
            "INSERT INTO ",
            self.quote_name(table_name),
            " (",
            join(", ", [self.quote_name(column) for column in columns]),
            ") VALUES (",
            join(", ", [self(value) for value in values]),
            ")",
        ]

    def DEFAULT(self):
        return "DEFAULT"

    def UPDATE(
        self,
        table_name: str,
        pairs: list[tuple[str, list[str | tuple[int, None, None] | Converter]]],
        where: list[str | list[str | list]] | None = None,
    ) -> list[str | list]:
        return [
            "UPDATE ",
            self.quote_name(table_name),
            "\nSET ",
            join(
                ", ",
                [(self.quote_name(name), " = ", self(param)) for name, param in pairs],
            ),
            where and ["\n", self(where)] or [],
        ]

    def DELETE(
        self,
        alias: str | None,
        from_ast: list[str | list[str | None] | list[str]],
        where: list[str | list] | None = None,
    ) -> tuple:
        self.indent += 1
        if alias is not None:
            assert isinstance(alias, str)
            if not where:
                return "DELETE ", self.quote_name(alias), " ", self(from_ast)
            return (
                "DELETE ",
                self.quote_name(alias),
                " ",
                self(from_ast),
                self(where),
            )
        else:
            assert (
                from_ast[0] == "FROM"
                and len(from_ast) == 2
                and from_ast[1][1] == "TABLE"
            )
            alias = from_ast[1][0]
            if alias is not None:
                self.suppress_aliases = True
            if not where:
                return "DELETE ", self(from_ast)
            return "DELETE ", self(from_ast), self(where)

    def _subquery(self, *sections: SqlAst) -> list[str | list[str]]:
        self.indent += 1
        new_sections: Sequence[SqlAst] = sections
        if not self.inner_join_syntax:
            new_sections = move_conditions_from_inner_join_to_where(sections)
        result = [self(s) for s in new_sections]
        self.indent -= 1
        return result

    def SELECT(self, *sections: SqlAst) -> SqlAst:
        prev_suppress_aliases = self.suppress_aliases
        self.suppress_aliases = False
        try:
            result = self._subquery(*sections)
            if self.indent:
                indent = self.indent_spaces * self.indent
                return "(\n", result, indent + ")"
            return result
        finally:
            self.suppress_aliases = prev_suppress_aliases

    def SELECT_FOR_UPDATE(
        self, nowait: bool, skip_locked: bool, *sections: SqlAst
    ) -> SqlAst | tuple[SqlAst, str, str, str, str]:
        assert not self.indent
        result = self.SELECT(*sections)
        nowait_str = " NOWAIT" if nowait else ""
        skip_locked_str = " SKIP LOCKED" if skip_locked else ""
        return result, "FOR UPDATE", nowait_str, skip_locked_str, "\n"

    def EXISTS(self, *sections: SqlAst) -> tuple[str, str, str, list, str, str]:
        result = self._subquery(*sections)
        indent = self.indent_spaces * self.indent
        return "EXISTS (\n", indent, "SELECT 1\n", result, indent, ")"

    def NOT_EXISTS(
        self, *sections: SqlAst
    ) -> tuple[str, tuple[str, str, str, list, str, str]]:
        return "NOT ", self.EXISTS(*sections)

    @indentable
    def ALL(self, *expr_list: SqlAst) -> SqlAst:
        exprs = [self(e) for e in expr_list]
        return "SELECT ", join(", ", exprs), "\n"

    @indentable
    def DISTINCT(self, *expr_list: SqlAst) -> SqlAst:
        exprs = [self(e) for e in expr_list]
        return "SELECT DISTINCT ", join(", ", exprs), "\n"

    @indentable
    def AGGREGATES(self, *expr_list: SqlAst) -> SqlAst:
        exprs = [self(e) for e in expr_list]
        return "SELECT ", join(", ", exprs), "\n"

    def AS(self, expr: list, alias: str) -> tuple[Sequence, str, str]:
        return self(expr), " AS ", self.quote_name(alias)

    def compound_name(self, name_parts):
        return ".".join(p and self.quote_name(p) or "" for p in name_parts)

    def sql_join(
        self, join_type: str, sources: tuple[list[str | list], ...]
    ) -> list[str]:
        indent = self.indent_spaces * (self.indent - 1)
        indent2 = indent + self.indent_spaces
        result: list[Any] = [indent, "FROM "]
        alias: str | list | None
        for i, source in enumerate(sources):
            if len(source) == 3:
                alias, kind, x = source
                join_cond = None
            elif len(source) == 4:
                alias, kind, x, join_cond = source
            else:
                throw(AstError, "Invalid source in FROM section: %r" % source)
            if i > 0:
                if join_cond is None:
                    result.append(", ")
                else:
                    result += ["\n", indent, "  %s JOIN " % join_type]
            if self.suppress_aliases:
                alias = None
            elif alias is not None:
                alias = self.quote_name(alias)
            if kind == "TABLE":
                if isinstance(x, str):
                    result.append(self.quote_name(x))
                else:
                    result.append(self.compound_name(x))
                if alias is not None:
                    result += " ", alias  # Oracle does not support 'AS' here
            elif kind == "SELECT":
                if alias is None:
                    throw(AstError, "Subquery in FROM section must have an alias")
                result += [
                    self.SELECT(*x),
                    " ",
                    alias,
                ]  # Oracle does not support 'AS' here
            else:
                throw(AstError, "Invalid source kind in FROM section: %r" % kind)
            if join_cond is not None:
                result += ["\n", indent2, "ON ", self(join_cond)]
        result.append("\n")
        return result

    def FROM(self, *sources) -> list[str]:
        return self.sql_join("INNER", sources)

    def INNER_JOIN(self, *sources) -> list[str]:
        self.inner_join_syntax = True
        return self.sql_join("INNER", sources)

    @indentable
    def LEFT_JOIN(self, *sources) -> SqlAst:
        return self.sql_join("LEFT", sources)

    def WHERE(self, *conditions) -> str | list[str]:
        if not conditions:
            return ""
        flat_conds = flat_conditions(conditions)
        indent = self.indent_spaces * (self.indent - 1)
        result = [indent, "WHERE "]
        extend = result.extend
        extend((self(flat_conds[0]), "\n"))
        for condition in flat_conds[1:]:
            extend((indent, "  AND ", self(condition), "\n"))
        return result

    def HAVING(self, *conditions) -> str | list[str]:
        if not conditions:
            return ""
        flat_conds = flat_conditions(conditions)
        indent = self.indent_spaces * (self.indent - 1)
        result = [indent, "HAVING "]
        extend = result.extend
        extend((self(flat_conds[0]), "\n"))
        for condition in flat_conds[1:]:
            extend((indent, "  AND ", self(condition), "\n"))
        return result

    @indentable
    def GROUP_BY(self, *expr_list: SqlAst) -> SqlAst:
        exprs = [self(e) for e in expr_list]
        return "GROUP BY ", join(", ", exprs), "\n"

    @indentable
    def UNION(self, kind, *sections: SqlAst) -> SqlAst:
        return "UNION ", kind, "\n", self.SELECT(*sections)

    @indentable
    def INTERSECT(self, *sections: SqlAst) -> SqlAst:
        return "INTERSECT\n", self.SELECT(*sections)

    @indentable
    def EXCEPT(self, *sections: SqlAst) -> SqlAst:
        return "EXCEPT\n", self.SELECT(*sections)

    @indentable
    def ORDER_BY(self, *order_list: SqlAst) -> SqlAst:
        result = ["ORDER BY "]
        result.extend(join(", ", [self(expr) for expr in order_list]))
        result.append("\n")
        return result

    def DESC(self, expr: list[int | str]) -> tuple[Value | Sequence, str]:
        return self(expr), " DESC"

    @indentable
    def LIMIT(
        self, limit: int | None | Literal["null"], offset: int | None = None
    ) -> str | Sequence[str]:
        if limit is None:
            limit = "null"
        else:
            assert isinstance(limit, int_types)
        assert offset is None or isinstance(offset, int)
        if offset:
            return "LIMIT %s OFFSET %d\n" % (limit, offset)
        else:
            return "LIMIT %s\n" % limit

    def COLUMN(self, table_alias: str | None, col_name: str) -> list[str]:
        if self.suppress_aliases or not table_alias:
            return ["%s" % self.quote_name(col_name)]
        return ["%s.%s" % (self.quote_name(table_alias), self.quote_name(col_name))]

    def PARAM(
        self,
        paramkey: str | ParamKey,
        converter: Converter | None = None,
        optimistic: bool = False,
    ) -> Param:
        return self.make_param(self.param_class, paramkey, converter, optimistic)

    def make_param(
        self,
        param_class: type[_PT],
        paramkey: str | ParamKey | tuple[ParamKey | str, ...],
        *args,
    ) -> _PT:
        keys = self.keys
        param = keys.get(paramkey)
        if param is None:
            param = param_class(self.paramstyle, paramkey, *args)  # type: ignore[arg-type]
            keys[paramkey] = param
        return cast("_PT", param)

    def make_composite_param(
        self,
        paramkey: tuple[ParamKey | str, ...],
        items: list[Param | Value],
        func: Callable,
    ) -> CompositeParam:
        return self.make_param(self.composite_param_class, paramkey, items, func)

    def STAR(self, table_alias: str) -> tuple[str, str]:
        return self.quote_name(table_alias), ".*"

    def ROW(self, *items) -> tuple[str, list[Value | str | list[str]], str]:
        return "(", join(", ", map(self, items)), ")"

    def VALUE(self, value: OptAttrValue) -> Value:
        return self.value_class(self.paramstyle, value)

    def AND(self, *cond_list) -> list[str | tuple[list[str], str]]:
        conds = [self(condition) for condition in cond_list]
        return join(" AND ", conds)

    def OR(self, *cond_list) -> tuple[str, list[str | tuple[list[str], str]], str]:
        conds = [self(condition) for condition in cond_list]
        return "(", join(" OR ", conds), ")"

    def NOT(self, condition: list[str | list]) -> tuple[str, tuple, str]:
        return "NOT (", self(condition), ")"

    def POW(
        self, expr1: list[int | str | list], expr2: list[int | str | list]
    ) -> tuple[str, Value | tuple, str, Value | tuple, str]:
        return "power(", self(expr1), ", ", self(expr2), ")"

    EQ = make_binary_op(" = ")
    NE = make_binary_op(" <> ")
    LT = make_binary_op(" < ")
    LE = make_binary_op(" <= ")
    GT = make_binary_op(" > ")
    GE = make_binary_op(" >= ")
    ADD = make_binary_op(" + ", True)
    SUB = make_binary_op(" - ", True)
    MUL = make_binary_op(" * ", True)
    DIV = make_binary_op(" / ", True)
    FLOORDIV = make_binary_op(" / ", True)

    def MOD(self, a, b) -> tuple[str, Any, str, Any, str]:
        symbol = " %% " if self.paramstyle in ("format", "pyformat") else " % "
        return "(", self(a), symbol, self(b), ")"

    def FLOAT_EQ(self, a, b) -> tuple:
        a, b = self(a), self(b)
        return (
            "abs(",
            a,
            " - ",
            b,
            ") / coalesce(nullif(greatest(abs(",
            a,
            "), abs(",
            b,
            ")), 0), 1) <= 1e-14",
        )

    def FLOAT_NE(self, a, b) -> tuple:
        a, b = self(a), self(b)
        return (
            "abs(",
            a,
            " - ",
            b,
            ") / coalesce(nullif(greatest(abs(",
            a,
            "), abs(",
            b,
            ")), 0), 1) > 1e-14",
        )

    def CONCAT(self, *args) -> tuple[str, list, str]:
        return "(", join(" || ", map(self, args)), ")"

    def NEG(self, expr: list[str]) -> tuple[str, list[str], str]:
        return "-(", self(expr), ")"

    def IS_NULL(
        self, expr: list[str | list[str | list] | type[None]]
    ) -> tuple[Sequence, str]:
        return self(expr), " IS NULL"

    def IS_NOT_NULL(self, expr: list[str | list | type[None]]) -> tuple[Sequence, str]:
        return self(expr), " IS NOT NULL"

    def LIKE(
        self,
        expr: list[str | list[str | list]],
        template: list[str | list[str | list]],
        escape: list[str] | None = None,
    ) -> tuple[Unpack[tuple], str, Value]:
        result = self(expr), " LIKE ", self(template)
        if escape:
            return result + (" ESCAPE ", self(escape))
        return result

    def NOT_LIKE(
        self,
        expr: list[str],
        template: list[str | list[str | list]],
        escape: list[str] | None = None,
    ) -> tuple[Unpack[tuple], str, Value]:
        result = self(expr), " NOT LIKE ", self(template)
        if escape:
            return result + (" ESCAPE ", self(escape))
        return result

    def BETWEEN(self, expr1, expr2, expr3):
        return self(expr1), " BETWEEN ", self(expr2), " AND ", self(expr3)

    def NOT_BETWEEN(self, expr1, expr2, expr3):
        return self(expr1), " NOT BETWEEN ", self(expr2), " AND ", self(expr3)

    def IN(
        self, expr1: list, x: list
    ) -> (
        str
        | tuple[
            list[str] | tuple[str, list, str],
            str,
            list[str | Param | Value | tuple[str, list, str]],
            str,
        ]
        | tuple[Value | Param | list[str], str, tuple[str, list, str]]
    ):
        if not x:
            return "0 = 1"
        if len(x) >= 1 and x[0] == "SELECT":
            return self(expr1), " IN ", self(x)
        expr_list = [self(expr) for expr in x]
        return self(expr1), " IN (", join(", ", expr_list), ")"

    def NOT_IN(
        self, expr1: list, x: list
    ) -> (
        str
        | tuple[
            list[str] | tuple[str, list, str],
            str,
            list[str | Param | Value | tuple[str, list, str]],
            str,
        ]
        | tuple[Value | Param | list[str], str, tuple[str, list, str]]
    ):
        if not x:
            return "1 = 1"
        if len(x) >= 1 and x[0] == "SELECT":
            return self(expr1), " NOT IN ", self(x)
        expr_list = [self(expr) for expr in x]
        return self(expr1), " NOT IN (", join(", ", expr_list), ")"

    def COUNT(self, distinct: bool | None, *expr_list) -> Sequence:
        assert distinct in (None, True, False)
        if not distinct:
            if not expr_list:
                return ["COUNT(*)"]
            if self.dialect == "PostgreSQL":
                return "COUNT(", self.ROW(*expr_list), ")"
            else:
                return "COUNT(", join(", ", map(self, expr_list)), ")"
        if not expr_list:
            throw(AstError, "COUNT(DISTINCT) without argument")
        if len(expr_list) == 1:
            return "COUNT(DISTINCT ", self(expr_list[0]), ")"

        if self.dialect == "PostgreSQL":
            return "COUNT(DISTINCT ", self.ROW(*expr_list), ")"
        elif self.dialect == "MySQL":
            return "COUNT(DISTINCT ", join(", ", map(self, expr_list)), ")"
        # Oracle and SQLite queries translated to completely different subquery syntax
        else:
            throw(NotImplementedError)  # This line must not be executed

    def SUM(self, distinct: bool | None, expr: list[str]) -> tuple[str, list[str], str]:
        assert distinct in (None, True, False)
        return (
            distinct and "coalesce(SUM(DISTINCT " or "coalesce(SUM(",
            self(expr),
            "), 0)",
        )

    def AVG(self, distinct: bool | None, expr: list) -> tuple[str, Sequence, str]:
        assert distinct in (None, True, False)
        return distinct and "AVG(DISTINCT " or "AVG(", self(expr), ")"

    def GROUP_CONCAT(
        self, distinct: bool | None, expr: list[str], sep: list[str] | None = None
    ) -> tuple[tuple, str]:
        assert distinct in (None, True, False)
        result: tuple[Any, ...] = (
            distinct and "GROUP_CONCAT(DISTINCT " or "GROUP_CONCAT(",
            self(expr),
        )
        if sep is not None:
            if self.provider.dialect == "MySQL":
                result = result, " SEPARATOR ", self(sep)
            else:
                result = result, ", ", self(sep)
        return result, ")"

    UPPER = make_unary_func("upper")
    LOWER = make_unary_func("lower")
    LENGTH = make_unary_func("length")
    ABS = make_unary_func("abs")

    def COALESCE(self, *args) -> tuple[str, list, str]:
        if len(args) < 2:
            assert False  # pragma: no cover
        return "coalesce(", join(", ", map(self, args)), ")"

    def MIN(self, distinct: bool | None, *args) -> tuple[str, str, list, str]:
        assert not distinct, distinct
        if len(args) == 0:
            assert False  # pragma: no cover
        elif len(args) == 1:
            fname = "MIN"
        else:
            fname = self.least_func_name
        return fname, "(", join(", ", map(self, args)), ")"

    def MAX(self, distinct: bool | None, *args) -> tuple[str, str, list, str]:
        assert not distinct, distinct
        if len(args) == 0:
            assert False  # pragma: no cover
        elif len(args) == 1:
            fname = "MAX"
        else:
            fname = self.greatest_func_name
        return fname, "(", join(", ", map(self, args)), ")"

    def SUBSTR(
        self,
        expr: list[str | list[str]],
        start: list[str | int | list[str | int | list]],
        len: list[str | int] | None = None,
    ) -> tuple:
        if len is None:
            return "substr(", self(expr), ", ", self(start), ")"
        return "substr(", self(expr), ", ", self(start), ", ", self(len), ")"

    def STRING_SLICE(
        self,
        expr: Sequence[str],
        start: Sequence[str | int | list[str | int | list] | None] | None,
        stop: Sequence[str | int | list[str | int | list] | None] | None,
    ) -> tuple[
        str,
        list[str],
        str,
        Value | list[str] | tuple,
        str,
        Value | list[str] | tuple,
        str,
    ]:
        index_sql: list[Any]
        len_sql: list[Any] | None
        start_sql: list[Any]
        stop_sql: list[Any]
        stop_positive: list[Any]
        stop_negative: list[Any]

        if start is None:
            start = ["VALUE", 0]

        if start[0] == "VALUE":
            start_value = start[1]
            assert isinstance(start_value, int)
            if self.dialect == "PostgreSQL" and start_value < 0:
                index_sql = ["LENGTH", expr]
                if start_value < -1:
                    index_sql = ["SUB", index_sql, ["VALUE", -(start_value + 1)]]
            else:
                if start_value >= 0:
                    start_value += 1
                index_sql = ["VALUE", start_value]
        else:
            inner_sql = start
            then = ["ADD", inner_sql, ["VALUE", 1]]
            else_ = (
                ["ADD", ["LENGTH", expr], then]
                if self.dialect == "PostgreSQL"
                else inner_sql
            )
            index_sql = ["IF", ["GE", inner_sql, ["VALUE", 0]], then, else_]

        if stop is None:
            len_sql = None
        elif stop[0] == "VALUE":
            stop_value = stop[1]
            assert isinstance(stop_value, int)
            if start[0] == "VALUE":
                start_value = start[1]
                assert isinstance(start_value, int)
                if start_value >= 0 and stop_value >= 0:
                    len_sql = ["VALUE", stop_value - start_value]
                elif start_value < 0 and stop_value < 0:
                    len_sql = ["VALUE", stop_value - start_value]
                elif start_value >= 0 and stop_value < 0:
                    len_sql = [
                        "SUB",
                        ["LENGTH", expr],
                        ["VALUE", start_value - stop_value],
                    ]
                    len_sql = ["MAX", False, len_sql, ["VALUE", 0]]
                elif start_value < 0 and stop_value >= 0:
                    len_sql = ["SUB", ["VALUE", stop_value + 1], index_sql]
                    len_sql = ["MAX", False, len_sql, ["VALUE", 0]]
                else:
                    assert False  # pragma: nocover1
            else:
                start_sql = ["COALESCE", start, ["VALUE", 0]]
                if stop_value >= 0:
                    start_positive = ["SUB", stop, start_sql]
                    start_negative = ["SUB", ["VALUE", stop_value + 1], index_sql]
                else:
                    start_positive = [
                        "SUB",
                        ["LENGTH", expr],
                        ["ADD", start_sql, ["VALUE", -stop_value]],
                    ]
                    start_negative = ["SUB", stop, start_sql]
                len_sql = [
                    "IF",
                    ["GE", start_sql, ["VALUE", 0]],
                    start_positive,
                    start_negative,
                ]
                len_sql = ["MAX", False, len_sql, ["VALUE", 0]]
        else:
            stop_sql = ["COALESCE", stop, ["VALUE", -1]]
            if start[0] == "VALUE":
                start_value = start[1]
                assert isinstance(start_value, int)
                start_sql = ["VALUE", start_value]
                if start_value >= 0:
                    stop_positive = ["SUB", stop_sql, start_sql]
                    stop_negative = [
                        "SUB",
                        ["LENGTH", expr],
                        ["SUB", start_sql, stop_sql],
                    ]
                else:
                    stop_positive = ["SUB", ["ADD", stop_sql, ["VALUE", 1]], index_sql]
                    stop_negative = ["SUB", stop_sql, start_sql]
                len_sql = [
                    "IF",
                    ["GE", stop_sql, ["VALUE", 0]],
                    stop_positive,
                    stop_negative,
                ]
                len_sql = ["MAX", False, len_sql, ["VALUE", 0]]
            else:
                start_sql = ["COALESCE", start, ["VALUE", 0]]
                both_positive = ["SUB", stop_sql, start_sql]
                both_negative = both_positive
                start_positive = ["SUB", ["LENGTH", expr], ["SUB", start_sql, stop_sql]]
                stop_positive = ["SUB", ["ADD", stop_sql, ["VALUE", 1]], index_sql]
                len_sql = [
                    "CASE",
                    None,
                    [
                        (
                            [
                                "AND",
                                ["GE", start_sql, ["VALUE", 0]],
                                ["GE", stop_sql, ["VALUE", 0]],
                            ],
                            both_positive,
                        ),
                        (
                            [
                                "AND",
                                ["LT", start_sql, ["VALUE", 0]],
                                ["LT", stop_sql, ["VALUE", 0]],
                            ],
                            both_negative,
                        ),
                        (
                            [
                                "AND",
                                ["GE", start_sql, ["VALUE", 0]],
                                ["LT", stop_sql, ["VALUE", 0]],
                            ],
                            start_positive,
                        ),
                        (
                            [
                                "AND",
                                ["LT", start_sql, ["VALUE", 0]],
                                ["GE", stop_sql, ["VALUE", 0]],
                            ],
                            stop_positive,
                        ),
                    ],
                ]
                len_sql = ["MAX", False, len_sql, ["VALUE", 0]]
        sql = ["SUBSTR", expr, index_sql, len_sql]
        return self(sql)

    def CASE(self, expr: None, cases: list, default: list | None = None) -> list:
        if (
            expr is None
            and default is not None
            and default[0] == "CASE"
            and default[1] is None
        ):
            cases2, default2 = default[2:]
            return self.CASE(None, tuple(cases) + tuple(cases2), default2)  # type: ignore[arg-type]
        result = ["case"]
        if expr is not None:
            result.append(" ")
            result.extend(self(expr))
        for condition, expr in cases:
            result.extend((" when ", self(condition), " then ", self(expr)))  # type: ignore[arg-type]
        if default is not None:
            result.extend((" else ", self(default)))
        result.append(" end")
        return result

    def IF(
        self, cond: list[str | list], then: list[str | list], else_: list[str | list]
    ) -> list[str | Param | list[str] | tuple]:
        return self.CASE(None, [(cond, then)], else_)

    def TRIM(
        self, expr: list[str], chars: list[str | tuple | StrConverter] | None = None
    ) -> tuple[str, list[str], str] | tuple[str, list[str], str, Value | Param, str]:
        if chars is None:
            return "trim(", self(expr), ")"
        return "trim(", self(expr), ", ", self(chars), ")"

    def LTRIM(
        self, expr: list[str], chars: list[str | tuple | StrConverter] | None = None
    ) -> tuple[str, list[str], str] | tuple[str, list[str], str, Value | Param, str]:
        if chars is None:
            return "ltrim(", self(expr), ")"
        return "ltrim(", self(expr), ", ", self(chars), ")"

    def RTRIM(
        self, expr: list[str], chars: list[str | tuple | StrConverter] | None = None
    ) -> tuple[str, list[str], str] | tuple[str, list[str], str, Value | Param, str]:
        if chars is None:
            return "rtrim(", self(expr), ")"
        return "rtrim(", self(expr), ", ", self(chars), ")"

    def REPLACE(
        self,
        str: list[str | StrConverter | Sequence],
        from_: list[str],
        to: list[str],
    ) -> tuple[str, Param | Sequence, str, Value, str, Value, str]:
        return "replace(", self(str), ", ", self(from_), ", ", self(to), ")"

    def TO_INT(
        self, expr: list[str | list[str] | list[str | int]]
    ) -> tuple[str, list[str] | tuple, str]:
        return "CAST(", self(expr), " AS integer)"

    def TO_STR(
        self, expr: list[str | list[str] | list[str | int]]
    ) -> tuple[str, list[str] | tuple, str]:
        return "CAST(", self(expr), " AS text)"

    def TO_REAL(
        self, expr: list[str | list[str] | list[str | int]]
    ) -> tuple[str, list[str] | tuple, str]:
        return "CAST(", self(expr), " AS real)"

    def TODAY(self) -> str:
        return "CURRENT_DATE"

    def NOW(self) -> str:
        return "CURRENT_TIMESTAMP"

    def DATE(self, expr: SqlAst) -> tuple[str, Any, str]:
        return "DATE(", self(expr), ")"

    def YEAR(self, expr) -> tuple:
        return "EXTRACT(YEAR FROM ", self(expr), ")"

    def MONTH(self, expr) -> tuple:
        return "EXTRACT(MONTH FROM ", self(expr), ")"

    def DAY(self, expr) -> tuple:
        return "EXTRACT(DAY FROM ", self(expr), ")"

    def HOUR(self, expr) -> tuple:
        return "EXTRACT(HOUR FROM ", self(expr), ")"

    def MINUTE(self, expr) -> tuple:
        return "EXTRACT(MINUTE FROM ", self(expr), ")"

    def SECOND(self, expr) -> tuple:
        return "EXTRACT(SECOND FROM ", self(expr), ")"

    def RANDOM(self) -> str:
        return "RAND()"

    def RAWSQL(
        self, sql: list[str | list[str | ParamKey | Converter]]
    ) -> str | list[str | Param]:
        if isinstance(sql, str):
            return sql
        return [x if isinstance(x, str) else self(x) for x in sql]

    def build_json_path(
        self, path: Sequence[SqlAst]
    ) -> tuple[Param | Value, bool, bool]:
        empty_slice = slice(None, None, None)
        has_params = False
        has_wildcards = False
        items = [self(element) for element in path]
        path_sql: Param | Value
        for item in items:
            if isinstance(item, Param):
                has_params = True
            elif isinstance(item, Value):
                value = item.value
                if value is Ellipsis or value == empty_slice:
                    has_wildcards = True
                else:
                    assert isinstance(value, (int, str)), value
            else:
                assert False, item
        if has_params:
            paramkey = tuple(
                (
                    item.paramkey
                    if isinstance(item, Param)
                    else None
                    if type(item.value) is slice
                    else item.value
                )
                for item in items
            )
            path_sql = self.make_composite_param(paramkey, items, self.eval_json_path)  # type: ignore[arg-type]
        else:
            result_value = self.eval_json_path(item.value for item in items)
            path_sql = self.value_class(self.paramstyle, result_value)
        return path_sql, has_params, has_wildcards

    @classmethod
    def eval_json_path(cls, values: Iterable[str | int]) -> str:
        result = ["$"]
        append = result.append
        empty_slice = slice(None, None, None)
        for value in values:
            if isinstance(value, int):
                append("[%d]" % value)
            elif isinstance(value, str):
                append(
                    "." + value
                    if is_ident(value)
                    else '."%s"' % value.replace('"', '\\"')
                )
            elif value is Ellipsis:
                append(".*")
            elif value == empty_slice:
                append("[*]")
            else:
                assert False, value
        return "".join(result)

    def JSON_QUERY(self, expr: Sequence[str], path: Sequence[Sequence]) -> tuple:
        throw(NotImplementedError)

    def JSON_VALUE(
        self,
        expr: Sequence[str],
        path: Sequence[Sequence],
        type: type[OptAttrValue] | _typeshed.MaybeNone,
    ) -> tuple:
        throw(NotImplementedError)

    def JSON_NONZERO(self, expr: Sequence[str | Sequence]) -> tuple:
        throw(NotImplementedError)

    def JSON_CONCAT(self, left, right) -> tuple:
        throw(NotImplementedError)

    def JSON_CONTAINS(
        self, expr: Sequence[str], path: list[Sequence[str]], key: Sequence
    ) -> Sequence:
        throw(NotImplementedError)

    def JSON_ARRAY_LENGTH(self, value: Sequence[str | Sequence]) -> tuple:
        throw(NotImplementedError)

    def JSON_PARAM(self, expr: Sequence[str | Sequence | Converter]) -> tuple:
        return self(expr)

    def ARRAY_INDEX(self, col: Sequence[str], index: Sequence) -> tuple:
        throw(NotImplementedError)

    def ARRAY_CONTAINS(
        self, key: Sequence[str | float], not_in: bool, col: Sequence[str]
    ) -> tuple:
        throw(NotImplementedError)

    def ARRAY_SUBSET(self, array1: Sequence, not_in: bool, array2: Sequence) -> tuple:
        throw(NotImplementedError)

    def ARRAY_LENGTH(self, array: Sequence[str]) -> tuple:
        throw(NotImplementedError)

    def ARRAY_SLICE(
        self,
        array: Sequence[str],
        start: Sequence[str | int | Sequence | None] | None,
        stop: Sequence[str | int | Sequence | None] | None,
    ) -> tuple:
        throw(NotImplementedError)

    def MAKE_ARRAY(self, *items) -> tuple:
        throw(NotImplementedError)
