from __future__ import annotations

import sys
import types
import weakref
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from functools import wraps
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    SupportsIndex,
    TypeAlias,
    TypeVar,
    cast,
    overload,
)
from uuid import UUID

from pony.py23compat import buffer, int_types
from pony.utils import deref_proxy, parse_expr, throw

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping
    from types import CodeType

    from typing_extensions import Self

    from pony.orm.core import (
        Attribute,
        Comparable,
        Entity,
        EntityIter,
        EntityMeta,
        ExprType,
        Numeric,
        OptAttrValue,
        PyType,
        Query,
        QueryKey,
        QueryResult,
        QueryResultIterator,
        SetIterator,
    )
    from pony.orm.sqltranslation import SQLTranslator

_FuncType: TypeAlias = "types.FunctionType | types.BuiltinFunctionType"
_RawSQLItems: TypeAlias = "tuple[str | tuple[str, CodeType], ...]"

_T = TypeVar("_T")
_FT = TypeVar("_FT", bound=_FuncType)
_QT = TypeVar("_QT", bound="Query | QueryResult | QueryResultIterator | SetIterator")
_OACT = TypeVar("_OACT", bound="OptAttrValue | types.EllipsisType")
_TT = TypeVar(
    "_TT", bound="PyType | EntityMeta | type[None] | slice | types.EllipsisType"
)

NoneType = type(None)


class LongStr(str):
    lazy: ClassVar[bool] = True


LongUnicode: type[LongStr] = LongStr


class SetType:
    __slots__ = "item_type"

    item_type: type

    def __deepcopy__(self, memo: dict[int, object]) -> Self:
        return self  # SetType instances are "immutable"

    def __init__(self, item_type: type) -> None:
        self.item_type = item_type

    def __eq__(self, other: object) -> bool:
        return type(other) is SetType and self.item_type == other.item_type

    def __ne__(self, other):
        return type(other) is not SetType or self.item_type != other.item_type

    def __hash__(self) -> int:
        return hash(self.item_type) + 1


class FuncType:
    __slots__ = "func"

    func: Callable

    def __deepcopy__(self, memo: dict[int, object]) -> Self:
        return self  # FuncType instances are "immutable"

    def __init__(self, func: Callable) -> None:
        self.func = func

    def __eq__(self, other: object) -> bool:
        return type(other) is FuncType and self.func == other.func

    def __ne__(self, other):
        return type(other) is not FuncType or self.func != other.func

    def __hash__(self) -> int:
        return hash(self.func) + 1

    def __repr__(self):
        return "FuncType(%s at %d)" % (self.func.__name__, id(self.func))


class MethodType:
    __slots__ = "obj", "func"

    obj: object
    func: Callable

    def __deepcopy__(self, memo):
        return self  # MethodType instances are "immutable"

    def __init__(self, method: types.MethodType) -> None:
        self.obj = method.__self__
        self.func = method.__func__

    def __eq__(self, other):
        return (
            type(other) is MethodType
            and self.obj == other.obj
            and self.func == other.func
        )

    def __ne__(self, other):
        return (
            type(other) is not MethodType
            or self.obj != other.obj
            or self.func != other.func
        )

    def __hash__(self) -> int:
        return hash(self.obj) ^ hash(self.func)


raw_sql_cache: dict[str, tuple[_RawSQLItems, tuple[CodeType, ...]]] = {}


def parse_raw_sql(
    sql: str,
) -> tuple[_RawSQLItems, tuple[CodeType, ...]]:
    result = raw_sql_cache.get(sql)
    if result is not None:
        return result
    if not isinstance(sql, str) or not sql:
        throw(TypeError, "Raw SQL string fragment expected. Got: %r" % sql)
    items: list[str | tuple[str, CodeType]] = []
    codes: list[CodeType] = []
    pos = 0
    while True:
        try:
            i = sql.index("$", pos)
        except ValueError:
            items.append(sql[pos:])
            break
        items.append(sql[pos:i])
        if sql[i + 1] == "$":
            items.append("$")
            pos = i + 2
        else:
            try:
                expr, _ = parse_expr(sql, i + 1)
            except ValueError:
                raise ValueError(sql[i:])
            pos = i + 1 + len(expr)
            if expr.endswith(";"):
                expr = expr[:-1]
            code = compile(expr, "<?>", "eval")  # expr correction check
            codes.append(code)
            items.append((expr, code))
    result = tuple(items), tuple(codes)
    raw_sql_cache[sql] = result
    return result


def raw_sql(sql: str, result_type: PyType | None = None) -> RawSQL:
    globals = sys._getframe(1).f_globals
    locals = sys._getframe(1).f_locals
    return RawSQL(sql, globals, locals, result_type)


class RawSQL:
    sql: str
    items: _RawSQLItems
    codes: tuple[CodeType, ...]
    types: tuple[type, ...]
    values: tuple[Any, ...]
    result_type: PyType | None

    def __deepcopy__(self, memo):
        assert (
            False
        )  # should not attempt to deepcopy RawSQL instances, because of locals/globals

    def __init__(
        self,
        sql: str,
        globals: dict[str, Any] | None = None,
        locals: Mapping[str, object] | None = None,
        result_type: PyType | None = None,
    ) -> None:
        self.sql = sql
        self.items, self.codes = parse_raw_sql(sql)
        self.types, self.values = normalize(
            tuple(eval(code, globals, locals) for code in self.codes)
        )
        self.result_type = result_type

    def _get_type_(self) -> RawSQLType:
        return RawSQLType(self.sql, self.items, self.types, self.result_type)

    if TYPE_CHECKING:

        def __lt__(self, other: object) -> bool: ...

        def __gt__(self, other: object) -> bool: ...


class RawSQLType:
    sql: str
    items: _RawSQLItems
    types: tuple[type, ...]
    result_type: PyType | None

    def __deepcopy__(self, memo: Any) -> Self:
        return self  # RawSQLType instances are "immutable"

    def __init__(
        self,
        sql: str,
        items: _RawSQLItems,
        types: tuple[type, ...],
        result_type: PyType | None,
    ) -> None:
        self.sql = sql
        self.items = items
        self.types = types
        self.result_type = result_type

    def __hash__(self) -> int:
        return hash(self.sql) ^ hash(self.types)

    def __eq__(self, other: object) -> bool:
        return (
            type(other) is RawSQLType
            and self.sql == other.sql
            and self.types == other.types
        )

    def __ne__(self, other):
        return not self.__eq__(other)


class QueryType:
    query_key: QueryKey
    translator: SQLTranslator
    limit: int | None
    offset: int | None

    def __init__(
        self, query: Query, limit: int | None = None, offset: int | None = None
    ) -> None:
        self.query_key = query._key
        self.translator = query._translator
        self.limit = limit
        self.offset = offset

    def __hash__(self) -> int:
        result = hash(self.query_key)
        if self.limit is not None:
            result ^= hash(self.limit + 3)
        if self.offset is not None:
            result ^= hash(self.offset)
        return result

    def __eq__(self, other):
        return (
            type(other) is QueryType
            and self.query_key == other.query_key
            and self.limit == other.limit
            and self.offset == other.offset
        )

    def __ne__(self, other):
        return not self.__eq__(other)


@overload
def normalize(value: tuple) -> tuple[tuple, tuple]: ...
@overload
def normalize(value: EntityMeta | EntityIter) -> tuple[SetType, Entity]: ...
@overload
def normalize(value: types.MethodType) -> tuple[MethodType, types.MethodType]: ...
@overload
def normalize(value: _FT) -> tuple[FuncType, _FT]: ...
@overload
def normalize(value: _QT) -> tuple[QueryType, _QT]: ...
@overload
def normalize(value: RawSQL) -> tuple[RawSQLType, RawSQL]: ...
@overload
def normalize(value: _OACT) -> tuple[type[_OACT], _OACT]: ...
def normalize(value):
    value = deref_proxy(value)
    t = type(value)
    if t is tuple:
        item_types, item_values = [], []
        for item in value:
            item_type, item_value = normalize(item)
            item_values.append(item_value)
            item_types.append(item_type)
        return tuple(item_types), tuple(item_values)

    if t.__name__ == "EntityMeta":
        return SetType(value), value

    if t.__name__ == "EntityIter":
        entity = value.entity
        return SetType(entity), entity

    if isinstance(value, str):
        return str, value

    if t in function_types:
        return FuncType(value), value

    if t is types.MethodType:
        return MethodType(value), value

    if hasattr(value, "_get_type_"):
        return value._get_type_(), value

    return normalize_type(t), value


@overload
def normalize_type(t: _TT) -> _TT: ...
@overload
def normalize_type(t: tuple[_TT, ...]) -> tuple[_TT, ...]: ...
def normalize_type(t):
    tt = type(t)
    if tt is tuple:
        return tuple(normalize_type(item) for item in t)
    if not isinstance(t, type):
        return t
    assert t.__name__ != "EntityMeta"
    if tt.__name__ == "EntityMeta":
        return t
    if t is NoneType:
        return t
    t = type_normalization_dict.get(t, t)
    if t in primitive_types:
        return t
    if t in (slice, type(Ellipsis)):
        return t
    if issubclass(t, str):
        return str
    if issubclass(t, (dict, Json)):
        return Json
    if issubclass(t, Array):
        return t
    throw(TypeError, "Unsupported type %r" % t.__name__)


coercions: dict[
    tuple[type, type],
    type[int | float | Decimal | datetime],
] = {
    (int, float): float,
    (int, Decimal): Decimal,
    (date, datetime): datetime,
    (bool, int): int,
    (bool, float): float,
    (bool, Decimal): Decimal,
}
coercions.update(((t2, t1), t3) for ((t1, t2), t3) in list(coercions.items()))


def coerce_types(t1: type | SetType, t2: type | SetType) -> type | SetType | None:
    if t1 == t2:
        return t1
    is_set_type = False
    if type(t1) is SetType:
        assert isinstance(t1, SetType)
        is_set_type = True
        t1 = t1.item_type
    if type(t2) is SetType:
        assert isinstance(t2, SetType)
        is_set_type = True
        t2 = t2.item_type
    assert not isinstance(t1, SetType) and not isinstance(t2, SetType)
    result = coercions.get((t1, t2))
    if result is not None and is_set_type:
        return SetType(result)
    return result


def are_comparable_types(
    t1: ExprType,
    t2: ExprType,
    op: str | None = "==",
) -> bool:
    # types must be normalized already!
    # op=None means a plain compatibility check without an operator
    tt1 = type(t1)
    tt2 = type(t2)

    t12 = {t1, t2}
    if Json in t12 and t12 < {Json, str, str, int, bool, float}:
        return True
    if op in ("in", "not in"):
        if tt2 is RawSQLType:
            return True
        if tt2 is not SetType:
            return False
        assert isinstance(t2, SetType)
        op = "=="
        t2 = t2.item_type
        tt2 = type(t2)
    if op in ("is", "is not"):
        return t1 is not None and t2 is NoneType
    if tt1 is tuple:
        if tt2 is not tuple:
            return False
        assert isinstance(t1, tuple) and isinstance(t2, tuple)
        if len(t1) != len(t2):
            return False
        for item1, item2 in zip(t1, t2):
            if not are_comparable_types(item1, item2):
                return False
        return True
    if tt1 is RawSQLType or tt2 is RawSQLType:
        return True
    if op in ("==", "<>", "!="):
        if t1 is NoneType and t2 is NoneType:
            return False
        if t1 is NoneType or t2 is NoneType:
            return True
        if t1 in primitive_types:
            if t1 is t2:
                return True
            if (t1, t2) in coercions:
                return True
            if tt1 is not type or tt2 is not type:
                return False
            if TYPE_CHECKING:
                assert isinstance(t1, type)
            assert isinstance(t2, type)
            if issubclass(t1, int_types) and issubclass(t2, str):
                return True
            if issubclass(t2, int_types) and issubclass(t1, str):
                return True
            return False
        if tt1.__name__ == tt2.__name__ == "EntityMeta":
            # mypy cannot narrow type[...] items to EntityMeta instances
            return cast("EntityMeta", t1)._root_ is cast("EntityMeta", t2)._root_
        return False
    if t1 is t2 and t1 in comparable_types:
        return True
    return (t1, t2) in coercions


class TrackedValue:
    obj_ref: weakref.ReferenceType[Entity]

    def __init__(self, obj: Entity, attr: Attribute) -> None:
        self.obj_ref = weakref.ref(obj)
        self.attr = attr

    @classmethod
    def make(cls, obj: Entity | Any, attr: Attribute | Any, value: Any) -> Any:
        if isinstance(value, dict):
            return TrackedDict(obj, attr, value)
        if isinstance(value, list):
            return TrackedList(obj, attr, value)
        return value

    def _changed_(self) -> None:
        obj = self.obj_ref()
        if obj is not None:
            obj._attr_changed_(self.attr)

    def get_untracked(self) -> Any:
        assert False, "Abstract method"  # pragma: no cover


def tracked_method(func: Callable[..., _T]) -> Callable[..., _T]:
    @wraps(func)
    def new_func(self, *args, **kwargs) -> _T:
        obj = self.obj_ref()
        attr = self.attr
        if obj is not None:
            args = tuple(TrackedValue.make(obj, attr, arg) for arg in args)
            if kwargs:
                kwargs = {
                    key: TrackedValue.make(obj, attr, value)
                    for key, value in kwargs.items()
                }
        result = func(self, *args, **kwargs)
        self._changed_()
        return result

    return new_func


class TrackedDict(TrackedValue, dict):
    def __init__(self, obj: Entity, attr: Attribute, value: dict[str, Any]) -> None:
        TrackedValue.__init__(self, obj, attr)
        dict.__init__(
            self, {key: self.make(obj, attr, val) for key, val in value.items()}
        )

    def __reduce__(self) -> tuple[type[dict], tuple[dict[str, Any]]]:
        return dict, (dict(self),)

    __setitem__ = tracked_method(dict.__setitem__)
    __delitem__ = tracked_method(dict.__delitem__)
    _update = tracked_method(dict.update)

    def update(self, *args, **kwargs) -> None:
        dicts = [arg if isinstance(arg, dict) else dict(arg) for arg in args]
        return self._update(*dicts, **kwargs)

    setdefault = tracked_method(dict.setdefault)
    pop = tracked_method(dict.pop)
    popitem = tracked_method(dict.popitem)
    clear = tracked_method(dict.clear)

    def get_untracked(self) -> dict:
        return {
            key: val.get_untracked() if isinstance(val, TrackedValue) else val
            for key, val in self.items()
        }


class TrackedList(TrackedValue, list):
    def __init__(self, obj: Entity, attr: Attribute, value: list) -> None:
        TrackedValue.__init__(self, obj, attr)
        list.__init__(self, (self.make(obj, attr, val) for val in value))

    def __reduce__(self) -> tuple[type[list], tuple[list]]:
        return list, (list(self),)

    __setitem__ = tracked_method(list.__setitem__)
    __delitem__ = tracked_method(list.__delitem__)
    extend = tracked_method(list.extend)
    append = tracked_method(list.append)
    pop = tracked_method(list.pop)
    remove = tracked_method(list.remove)
    insert = tracked_method(list.insert)
    reverse = tracked_method(list.reverse)
    sort = tracked_method(list.sort)
    clear = tracked_method(list.clear)

    def get_untracked(self) -> list:
        return [
            val.get_untracked() if isinstance(val, TrackedValue) else val
            for val in self
        ]


def validate_item(
    item_type: type[str | float], item: str | int | SupportsIndex
) -> str | int | float:
    if not isinstance(item, item_type):
        if item_type is not str and hasattr(item, "__index__"):
            return cast("SupportsIndex", item).__index__()
        throw(
            TypeError,
            "Cannot store %r item in array of %r"
            % (type(item).__name__, item_type.__name__),
        )
    assert isinstance(item, item_type)
    return item


class TrackedArray(TrackedList):
    item_type: type[str | int | float]

    def __init__(self, obj: Entity, attr: Attribute, value: list) -> None:
        TrackedList.__init__(self, obj, attr, value)
        self.item_type = attr.py_type.item_type

    def extend(self, items: Iterable[Any]) -> None:
        items = [validate_item(self.item_type, item) for item in items]
        TrackedList.extend(self, items)

    def append(self, item: Any) -> None:
        item = validate_item(self.item_type, item)
        TrackedList.append(self, item)

    def insert(self, index, item):
        item = validate_item(self.item_type, item)
        TrackedList.insert(self, index, item)

    def __setitem__(self, index, item):
        item = validate_item(self.item_type, item)
        TrackedList.__setitem__(self, index, item)

    def __contains__(self, item: Any) -> bool:
        if not isinstance(item, str) and hasattr(item, "__iter__"):
            return all(it in set(self) for it in item)
        return list.__contains__(self, item)


class Json:
    """A wrapper over a dict or list"""

    wrapped: dict[str, str] | list[str]

    @classmethod
    def default_empty_value(cls) -> dict[Any, Any]:
        return {}

    def __init__(self, wrapped: dict[str, str] | list[str]) -> None:
        self.wrapped = wrapped

    def __repr__(self):
        return "<Json %r>" % self.wrapped


class Array:
    item_type: ClassVar[Any] = None  # Should be overridden in subclass

    @classmethod
    def default_empty_value(cls) -> list[Any]:
        return []


class IntArray(Array):
    item_type: ClassVar[type[int]] = int


class StrArray(Array):
    item_type: ClassVar[type[str]] = str


class FloatArray(Array):
    item_type: ClassVar[type[float]] = float


numeric_types: set[type[Numeric]] = {bool, int, float, Decimal}
comparable_types: set[type[Comparable | Array]] = {
    int,
    float,
    Decimal,
    str,
    date,
    time,
    datetime,
    timedelta,
    bool,
    UUID,
    IntArray,
    StrArray,
    FloatArray,
}
primitive_types: set[type[Comparable | Array | bytes]] = comparable_types | {buffer}
function_types: set[_FuncType | type] = {
    type,
    types.FunctionType,
    types.BuiltinFunctionType,
}
type_normalization_dict: dict[type, type] = {}

array_types: dict[type[int | str | float], type[Array]] = {
    int: IntArray,
    float: FloatArray,
    str: StrArray,
}
