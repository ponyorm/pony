from __future__ import annotations

import datetime
import json
import os
import os.path
import re
import sqlite3 as sqlite
import sys
import time
from binascii import hexlify
from decimal import Decimal
from functools import wraps
from random import random
from threading import Lock
from typing import TYPE_CHECKING, Any, ClassVar, Literal, NoReturn, TypeVar, cast
from uuid import UUID

from pony.orm import core, dbapiprovider, dbschema
from pony.orm.core import log_orm

if TYPE_CHECKING:
    from pony.orm.core import (
        Database,
        DbConnection,
        Entity,
        OptAttrValue,
        ParamKey,
        PyType,
        SessionCache,
        TableName,
    )
from pony.orm.dbapiprovider import (
    ArrayConverter,
    Converter,
    DBAPIProvider,
    IntConverter,
    Pool,
    wrap_dbapi_exceptions,
)
from pony.orm.ormtypes import Json, TrackedArray, TrackedValue
from pony.orm.sqlbuilding import (
    Param,
    SqlAst,
    SQLBuilder,
    Value,
    join,
    make_unary_func,
)
from pony.orm.sqltranslation import SQLTranslator, StringExprMonad
from pony.py23compat import buffer, int_types
from pony.utils import (
    absolutize_path,
    cut_traceback_depth,
    datetime2timestamp,
    localbase,
    reraise,
    throw,
    timestamp2datetime,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Sequence
    from types import ModuleType

_T = TypeVar("_T")


class SqliteExtensionUnavailable(Exception):
    pass


NoneType = type(None)


class SQLiteForeignKey(dbschema.ForeignKey):
    def get_create_command(self) -> NoReturn:
        assert False  # pragma: no cover


class SQLiteSchema(dbschema.DBSchema):
    dialect: ClassVar[str] = "SQLite"
    named_foreign_keys = False
    fk_class = SQLiteForeignKey


def make_overriden_string_func(sqlop: str) -> Callable:
    def func(translator, monad):
        sql = monad.getsql()
        assert len(sql) == 1
        return StringExprMonad(monad.type, [sqlop, sql[0]])

    func.__name__ = sqlop
    return func


class SQLiteTranslator(SQLTranslator):
    dialect: ClassVar[str] = "SQLite"
    sqlite_version: ClassVar[tuple[int, int, int]] = sqlite.sqlite_version_info
    row_value_syntax: ClassVar[bool] = False
    rowid_support: ClassVar[bool] = True

    StringMixin_UPPER: ClassVar[Callable[[str], str]] = make_overriden_string_func(
        "PY_UPPER"
    )
    StringMixin_LOWER: ClassVar[Callable[[str], str]] = make_overriden_string_func(
        "PY_LOWER"
    )


class SQLiteValue(Value):
    __slots__: list[str] = []

    def __str__(self):
        value = self.value
        if isinstance(value, datetime.datetime):
            return self.quote_str(datetime2timestamp(value))
        if isinstance(value, datetime.date):
            return self.quote_str(str(value))
        if isinstance(value, datetime.timedelta):
            return repr(value.total_seconds() / (24 * 60 * 60))
        return Value.__str__(self)


class SQLiteBuilder(SQLBuilder):
    dialect: ClassVar[str] = "SQLite"
    least_func_name = "min"
    greatest_func_name = "max"
    value_class: type[Value] = SQLiteValue

    def __init__(self, provider: SQLiteProvider, ast: list[str | list | None]) -> None:
        self.json1_available = provider.json1_available
        SQLBuilder.__init__(self, provider, ast)

    def SELECT_FOR_UPDATE(
        self, nowait: bool, skip_locked: bool, *sections: SqlAst
    ) -> SqlAst:
        assert not self.indent
        return self.SELECT(*sections)

    def INSERT(
        self,
        table_name: str,
        columns: Iterable[str | Iterable],
        values: Iterable[Sequence[str | tuple[int, None, None] | Converter]],
        returning: str | None = None,
    ) -> SqlAst:
        if not values:
            return "INSERT INTO %s DEFAULT VALUES" % self.quote_name(table_name)
        return SQLBuilder.INSERT(self, table_name, columns, values, returning)

    def STRING_SLICE(
        self,
        expr: Sequence[str],
        start: Sequence[str | int | Sequence | None] | None,
        stop: Sequence[str | int | Sequence | None] | None,
    ) -> tuple[
        str,
        list[str],
        str,
        Value | list[str] | tuple,
        str,
        Value | list[str] | tuple,
        str,
    ]:
        if start is None:
            start = ["VALUE", None]
        if stop is None:
            stop = ["VALUE", None]
        return (
            "py_string_slice(",
            self(expr),
            ", ",
            self(start),
            ", ",
            self(stop),
            ")",
        )

    def IN(
        self,
        expr1: list[str | int | ParamKey | Converter | list[str] | None],
        x: list,
    ) -> str | tuple[Any, str, Any] | tuple[Any, str, list, str]:
        if not x:
            return "0 = 1"
        if len(x) >= 1 and x[0] == "SELECT":
            return self(expr1), " IN ", self(x)
        op = " IN (VALUES " if expr1[0] == "ROW" else " IN ("
        expr_list = [self(expr) for expr in x]
        return self(expr1), op, join(", ", expr_list), ")"

    def NOT_IN(
        self, expr1: list[str | int | ParamKey | Converter | list[str]], x: list
    ) -> str | tuple[Any, str, Any] | tuple[Any, str, list, str]:
        if not x:
            return "1 = 1"
        if len(x) >= 1 and x[0] == "SELECT":
            return self(expr1), " NOT IN ", self(x)
        op = " NOT IN (VALUES " if expr1[0] == "ROW" else " NOT IN ("
        expr_list = [self(expr) for expr in x]
        return self(expr1), op, join(", ", expr_list), ")"

    def TODAY(self) -> str:
        return "date('now', 'localtime')"

    def NOW(self) -> str:
        return "datetime('now', 'localtime')"

    def YEAR(self, expr: list[str]) -> tuple[str, Any, str]:
        return "cast(substr(", self(expr), ", 1, 4) as integer)"

    def MONTH(self, expr: list[str]) -> tuple[str, Any, str]:
        return "cast(substr(", self(expr), ", 6, 2) as integer)"

    def DAY(self, expr: list[str]) -> tuple[str, Any, str]:
        return "cast(substr(", self(expr), ", 9, 2) as integer)"

    def HOUR(self, expr: list[str]) -> tuple[str, Any, str]:
        return "cast(substr(", self(expr), ", 12, 2) as integer)"

    def MINUTE(self, expr: list[str]) -> tuple[str, Any, str]:
        return "cast(substr(", self(expr), ", 15, 2) as integer)"

    def SECOND(self, expr: list[str]) -> tuple[str, Any, str]:
        return "cast(substr(", self(expr), ", 18, 2) as integer)"

    def datetime_add(
        self, funcname: str, expr: list[str], td: datetime.timedelta
    ) -> tuple[str, str, Any, list[str], str]:
        assert isinstance(td, datetime.timedelta)
        modifiers = []
        seconds = td.seconds + td.days * 24 * 3600
        sign = "+" if seconds > 0 else "-"
        seconds = abs(seconds)
        if seconds >= (24 * 3600):
            days = seconds // (24 * 3600)
            modifiers.append(", '%s%d days'" % (sign, days))
            seconds -= days * 24 * 3600
        if seconds >= 3600:
            hours = seconds // 3600
            modifiers.append(", '%s%d hours'" % (sign, hours))
            seconds -= hours * 3600
        if seconds >= 60:
            minutes = seconds // 60
            modifiers.append(", '%s%d minutes'" % (sign, minutes))
            seconds -= minutes * 60
        if seconds:
            modifiers.append(", '%s%d seconds'" % (sign, seconds))
        if not modifiers:
            return self(expr)
        return funcname, "(", self(expr), modifiers, ")"

    def DATE_ADD(
        self,
        expr: list[str],
        delta: list[(str | datetime.timedelta | ParamKey | SQLiteTimedeltaConverter)],
    ) -> tuple[str, Any, str, Any, str]:
        if delta[0] == "VALUE" and isinstance(delta[1], datetime.timedelta):
            return self.datetime_add("date", expr, delta[1])
        return "datetime(julianday(", self(expr), ") + ", self(delta), ")"

    def DATE_SUB(
        self,
        expr: list[str],
        delta: list[(str | datetime.timedelta | ParamKey | SQLiteTimedeltaConverter)],
    ) -> tuple[str, Any, str, Any, str]:
        if delta[0] == "VALUE" and isinstance(delta[1], datetime.timedelta):
            return self.datetime_add("date", expr, -delta[1])
        return "datetime(julianday(", self(expr), ") - ", self(delta), ")"

    def DATE_DIFF(
        self, expr1: list[str | ParamKey | SQLiteDateConverter], expr2: list[str]
    ) -> tuple[str, Any, str, Any, str]:
        return "julianday(", self(expr1), ") - julianday(", self(expr2), ")"

    def DATETIME_ADD(
        self,
        expr: list[str],
        delta: list[(str | datetime.timedelta | ParamKey | SQLiteTimedeltaConverter)],
    ) -> tuple[str, Any, str, Any, str]:
        if delta[0] == "VALUE" and isinstance(delta[1], datetime.timedelta):
            return self.datetime_add("datetime", expr, delta[1])
        return "datetime(julianday(", self(expr), ") + ", self(delta), ")"

    def DATETIME_SUB(
        self,
        expr: list[str],
        delta: list[(str | datetime.timedelta | ParamKey | SQLiteTimedeltaConverter)],
    ) -> tuple[str, Any, str, Any, str]:
        if delta[0] == "VALUE" and isinstance(delta[1], datetime.timedelta):
            return self.datetime_add("datetime", expr, -delta[1])
        return "datetime(julianday(", self(expr), ") - ", self(delta), ")"

    def DATETIME_DIFF(
        self, expr1: list[str | ParamKey | SQLiteDatetimeConverter], expr2: list[str]
    ) -> tuple[str, Any, str, Any, str]:
        return "julianday(", self(expr1), ") - julianday(", self(expr2), ")"

    def RANDOM(self) -> str:
        return "rand()"  # return '(random() / 9223372036854775807.0 + 1.0) / 2.0'

    PY_UPPER: Callable = make_unary_func("py_upper")
    PY_LOWER: Callable = make_unary_func("py_lower")

    def FLOAT_EQ(self, a, b):
        a, b = self(a), self(b)
        return (
            "abs(",
            a,
            " - ",
            b,
            ") / coalesce(nullif(max(abs(",
            a,
            "), abs(",
            b,
            ")), 0), 1) <= 1e-14",
        )

    def FLOAT_NE(self, a, b):
        a, b = self(a), self(b)
        return (
            "abs(",
            a,
            " - ",
            b,
            ") / coalesce(nullif(max(abs(",
            a,
            "), abs(",
            b,
            ")), 0), 1) > 1e-14",
        )

    def JSON_QUERY(
        self,
        expr: Sequence[str],
        path: Sequence[Sequence[(str | int | ParamKey | Converter)]],
    ) -> tuple[str, str, str, Any, str, Param | Value, str]:
        fname = "json_extract" if self.json1_available else "py_json_extract"
        path_sql, has_params, has_wildcards = self.build_json_path(path)
        return (
            "py_json_unwrap(",
            fname,
            "(",
            self(expr),
            ", '$.__non_existent_json_attr_name__', ",
            path_sql,
            "))",
        )

    json_value_type_mapping: dict[type[str | bool | float], str] = {
        str: "text",
        bool: "integer",
        int: "integer",
        float: "real",
    }

    def JSON_VALUE(
        self,
        expr: Sequence[str],
        path: Sequence[Sequence[str | int | ParamKey | Converter]],
        type: type[OptAttrValue] | None = None,
    ) -> (
        tuple[str, str, list[str], str, Value | Param, str]
        | tuple[str, tuple[str, str, list[str], str, Value | Param, str], str, str, str]
    ):
        func_name = "json_extract" if self.json1_available else "py_json_extract"
        path_sql, has_params, has_wildcards = self.build_json_path(path)
        type_name = self.json_value_type_mapping.get(type)  # type: ignore[arg-type]
        result = func_name, "(", self(expr), ", ", path_sql, ")"
        if type_name is not None:
            return "CAST(", result, " as ", type_name, ")"
        return result

    def JSON_NONZERO(
        self, expr: SqlAst
    ) -> tuple[tuple[str, str, str, list[str], str, Value, str], str]:
        return self(expr), """ NOT IN ('null', 'false', '0', '""', '[]', '{}')"""

    def JSON_ARRAY_LENGTH(
        self, value: SqlAst
    ) -> (
        tuple[str, str, tuple[str, str, str, list[str], str, Value, str], str]
        | tuple[str, str, list[str], str]
    ):
        func_name = (
            "json_array_length" if self.json1_available else "py_json_array_length"
        )
        return func_name, "(", self(value), ")"

    def JSON_CONTAINS(
        self,
        expr: Sequence[str],
        path: Sequence[Sequence[str]],
        key: Sequence[str | ParamKey | Converter],
    ) -> tuple[str, list[str], str, Param | Value, str, Param | Value, str]:
        path_sql, has_params, has_wildcards = self.build_json_path(path)
        return (
            "py_json_contains(",
            self(expr),
            ", ",
            path_sql,
            ",  ",
            self(key),
            ")",
        )

    def ARRAY_INDEX(
        self,
        col: Sequence[str],
        index: Sequence[
            str
            | int
            | Sequence[
                str | int | Sequence[str | int | ParamKey | IntConverter | Sequence]
            ]
            | None
        ],
    ) -> tuple[str, Any, str, Any, str]:
        return "py_array_index(", self(col), ", ", self(index), ")"

    def ARRAY_CONTAINS(
        self, key: Sequence[str | float], not_in: bool, col: Sequence[str]
    ) -> tuple[str, str, Any, str, Any, str]:
        return (
            ("NOT " if not_in else ""),
            "py_array_contains(",
            self(col),
            ", ",
            self(key),
            ")",
        )

    def ARRAY_SUBSET(
        self,
        array1: Sequence[(str | ParamKey | ArrayConverter | Sequence[str | int])],
        not_in: bool,
        array2: Sequence[str],
    ) -> tuple[str, str, Any, str, Any, str]:
        return (
            ("NOT " if not_in else ""),
            "py_array_subset(",
            self(array2),
            ", ",
            self(array1),
            ")",
        )

    def ARRAY_LENGTH(self, array: Sequence[str]) -> tuple[str, Any, str]:
        return "py_array_length(", self(array), ")"

    def ARRAY_SLICE(
        self,
        array: Sequence[str],
        start: Sequence[str | int | Sequence | None] | None,
        stop: Sequence[str | int | Sequence | None] | None,
    ) -> tuple[str, Any, str, Any | Literal["null"], str, Any | Literal["null"], str]:
        return (
            "py_array_slice(",
            self(array),
            ", ",
            self(start) if start else "null",
            ",",
            self(stop) if stop else "null",
            ")",
        )

    def MAKE_ARRAY(self, *items) -> tuple[str, list, str]:
        return "py_make_array(", join(", ", (self(item) for item in items)), ")"


class SQLiteIntConverter(dbapiprovider.IntConverter):
    def sql_type(self) -> str:
        attr = self.attr
        if attr is not None and attr.auto:
            return "INTEGER"  # Only this type can have AUTOINCREMENT option
        return dbapiprovider.IntConverter.sql_type(self)


class SQLiteDecimalConverter(dbapiprovider.DecimalConverter):
    inf = Decimal("infinity")
    neg_inf = Decimal("-infinity")
    NaN = Decimal("NaN")

    def sql2py(self, val: Decimal | float | str) -> Decimal:
        try:
            val = Decimal(str(val))
        except BaseException:
            return cast("Decimal", val)
        exp = self.exp
        if exp is not None:
            val = val.quantize(exp)
        return val

    def py2sql(self, val: Decimal) -> str:
        if type(val) is not Decimal:
            val = Decimal(val)
        exp = self.exp
        if exp is not None:
            if val in (self.inf, self.neg_inf, self.NaN):
                throw(ValueError, "Cannot store %s Decimal value in database" % val)
            val = val.quantize(exp)
        return str(val)


class SQLiteDateConverter(dbapiprovider.DateConverter):
    def sql2py(self, val: str | datetime.date) -> datetime.date:
        try:
            time_tuple = time.strptime(cast("str", val)[:10], "%Y-%m-%d")
            return datetime.date(*time_tuple[:3])
        except BaseException:
            return cast("datetime.date", val)

    def py2sql(self, val: datetime.date) -> str:
        return val.strftime("%Y-%m-%d")


class SQLiteTimeConverter(dbapiprovider.TimeConverter):
    def sql2py(self, val):
        try:
            if len(val) <= 8:  # pyright: ignore[reportArgumentType]
                dt = datetime.datetime.strptime(val, "%H:%M:%S")  # pyright: ignore[reportArgumentType]
            else:
                dt = datetime.datetime.strptime(val, "%H:%M:%S.%f")  # pyright: ignore[reportArgumentType]
            return dt.time()
        except BaseException:
            return cast("datetime.time", val)

    def py2sql(self, val):
        return val.isoformat()


class SQLiteTimedeltaConverter(dbapiprovider.TimedeltaConverter):
    def sql2py(self, val):
        return datetime.timedelta(days=val)  # pyright: ignore[reportArgumentType]

    def py2sql(self, val: datetime.timedelta) -> float:
        return val.days + (val.seconds + val.microseconds / 1000000.0) / 86400.0


class SQLiteDatetimeConverter(dbapiprovider.DatetimeConverter):
    def sql2py(self, val: str | datetime.datetime) -> datetime.datetime:
        try:
            return timestamp2datetime(val)  # type: ignore[arg-type]
        except BaseException:
            return cast("datetime.datetime", val)

    def py2sql(self, val: datetime.datetime) -> str:
        return datetime2timestamp(val)


class SQLiteJsonConverter(dbapiprovider.JsonConverter):
    json_kwargs = {"separators": (",", ":"), "sort_keys": True, "ensure_ascii": False}


def dumps(items: object) -> str:
    return json.dumps(items, **SQLiteJsonConverter.json_kwargs)


class SQLiteArrayConverter(dbapiprovider.ArrayConverter):
    array_types: ClassVar[dict[type[str | float], tuple[str, type[Converter]]]] = {
        int: ("int", SQLiteIntConverter),
        str: ("text", dbapiprovider.StrConverter),
        float: ("real", dbapiprovider.RealConverter),
    }

    def dbval2val(
        self, dbval: OptAttrValue | Entity, obj: Entity | None = None
    ) -> TrackedArray | list[str | int | float] | None:
        if not dbval:
            return None
        items = json.loads(dbval)  # type: ignore[arg-type]
        if obj is None:
            return items
        return TrackedArray(obj, self.attr, items)  # type: ignore[arg-type]

    def val2dbval(
        self, val: TrackedValue | OptAttrValue, obj: Entity | None = None
    ) -> str:
        return dumps(val)


class LocalExceptions(localbase):
    exc_info: tuple | None
    keep_traceback: bool

    def __init__(self) -> None:
        self.exc_info = None
        self.keep_traceback = False


local_exceptions: LocalExceptions = LocalExceptions()


def keep_exception(func: Callable[..., _T]) -> Callable[..., _T]:
    @wraps(func)
    def new_func(*args) -> _T:
        local_exceptions.exc_info = None
        try:
            return func(*args)
        except Exception:
            local_exceptions.exc_info = sys.exc_info()
            if not local_exceptions.keep_traceback:
                local_exceptions.exc_info = local_exceptions.exc_info[:2] + (None,)
            raise
        finally:
            local_exceptions.keep_traceback = False

    return new_func


class SQLiteProvider(DBAPIProvider):
    dialect: str = "SQLite"
    local_exceptions: LocalExceptions = local_exceptions
    max_name_len: int = 1024

    dbapi_module: ModuleType = sqlite
    dbschema_cls: type[SQLiteSchema] = SQLiteSchema
    translator_cls: type[SQLiteTranslator] = SQLiteTranslator
    sqlbuilder_cls: type[SQLiteBuilder] = SQLiteBuilder
    array_converter_cls: type[SQLiteArrayConverter] = SQLiteArrayConverter

    name_before_table: str = "db_name"

    server_version: int | tuple[int, ...] | None = sqlite.sqlite_version_info

    converter_classes: list[
        tuple[PyType | type[None] | tuple[type[int]], type[Converter]]  # noqa: Y090
    ] = [
        (NoneType, dbapiprovider.NoneConverter),
        (bool, dbapiprovider.BoolConverter),
        (str, dbapiprovider.StrConverter),
        (int_types, SQLiteIntConverter),
        (float, dbapiprovider.RealConverter),
        (Decimal, SQLiteDecimalConverter),
        (datetime.datetime, SQLiteDatetimeConverter),
        (datetime.date, SQLiteDateConverter),
        (datetime.time, SQLiteTimeConverter),
        (datetime.timedelta, SQLiteTimedeltaConverter),
        (UUID, dbapiprovider.UuidConverter),
        (buffer, dbapiprovider.BlobConverter),
        (Json, SQLiteJsonConverter),
    ]

    def __init__(self, database: Database, filename: str, **kwargs) -> None:
        is_shared_memory_db = filename == ":sharedmemory:"
        if is_shared_memory_db:
            filename = "file:memdb%d_%s?mode=memory&cache=shared" % (
                database.id,
                os.urandom(8).hex(),
            )
            kwargs["uri"] = True
        DBAPIProvider.__init__(self, database, is_shared_memory_db, filename, **kwargs)
        self.pre_transaction_lock = Lock()
        self.transaction_lock = Lock()

    @wrap_dbapi_exceptions
    def inspect_connection(self, connection: DbConnection) -> None:
        DBAPIProvider.inspect_connection(self, connection)
        self.json1_available = self.check_json1(connection)

    def restore_exception(self) -> None:
        if self.local_exceptions.exc_info is not None:
            try:
                reraise(*self.local_exceptions.exc_info)
            finally:
                self.local_exceptions.exc_info = None

    def acquire_lock(self) -> None:
        self.pre_transaction_lock.acquire()
        try:
            self.transaction_lock.acquire()
        finally:
            self.pre_transaction_lock.release()

    def release_lock(self) -> None:
        self.transaction_lock.release()

    @wrap_dbapi_exceptions
    def set_transaction_mode(
        self, connection: DbConnection, cache: SessionCache
    ) -> None:
        assert not cache.in_transaction
        if cache.immediate:
            self.acquire_lock()
        try:
            cursor = connection.cursor()

            db_session = cache.db_session
            if db_session is not None and db_session.ddl:
                cursor.execute("PRAGMA foreign_keys")
                fk_row = cursor.fetchone()
                fk = fk_row[0] if fk_row is not None else None
                if fk:
                    sql = "PRAGMA foreign_keys = false"
                    if core.local.debug:
                        log_orm(sql)
                    cursor.execute(sql)
                cache.saved_fk_state = bool(fk)
                assert cache.immediate

            if cache.immediate:
                sql = "BEGIN IMMEDIATE TRANSACTION"
                if core.local.debug:
                    log_orm(sql)
                cursor.execute(sql)
                cache.in_transaction = True
            elif core.local.debug:
                log_orm("SWITCH TO AUTOCOMMIT MODE")
        finally:
            if cache.immediate and not cache.in_transaction:
                self.release_lock()

    def commit(
        self, connection: DbConnection, cache: SessionCache | None = None
    ) -> None:
        in_transaction = cache is not None and cache.in_transaction
        try:
            DBAPIProvider.commit(self, connection, cache)
        finally:
            if cache is not None and in_transaction:
                cache.in_transaction = False
                self.release_lock()

    def rollback(
        self, connection: DbConnection, cache: SessionCache | None = None
    ) -> None:
        in_transaction = cache is not None and cache.in_transaction
        try:
            DBAPIProvider.rollback(self, connection, cache)
        finally:
            if cache is not None and in_transaction:
                cache.in_transaction = False
                self.release_lock()

    def drop(self, connection: DbConnection, cache: SessionCache | None = None) -> None:
        in_transaction = cache is not None and cache.in_transaction
        try:
            DBAPIProvider.drop(self, connection, cache)
        finally:
            if cache is not None and in_transaction:
                cache.in_transaction = False
                self.release_lock()

    @wrap_dbapi_exceptions
    def release(
        self, connection: DbConnection, cache: SessionCache | None = None
    ) -> None:
        if cache is not None:
            db_session = cache.db_session
            if db_session is not None and db_session.ddl and cache.saved_fk_state:
                try:
                    cursor = connection.cursor()
                    sql = "PRAGMA foreign_keys = true"
                    if core.local.debug:
                        log_orm(sql)
                    cursor.execute(sql)
                except:
                    self.pool.drop(connection)
                    raise
        DBAPIProvider.release(self, connection, cache)

    def get_pool(
        self,
        is_shared_memory_db: bool,
        filename: str,
        create_db: bool = False,
        **kwargs,
    ) -> SQLitePool:
        if is_shared_memory_db or filename == ":memory:":
            pass
        else:
            # When relative filename is specified, it is considered
            # not relative to cwd, but to user module where
            # Database instance is created

            # the list of frames:
            # 7 - user code: db = Database(...)
            # 6 - cut_traceback decorator wrapper
            # 5 - cut_traceback decorator
            # 4 - pony.orm.Database.__init__() / .bind()
            # 3 - pony.orm.Database._bind()
            # 2 - pony.dbapiprovider.DBAPIProvider.__init__()
            # 1 - SQLiteProvider.__init__()
            # 0 - pony.dbproviders.sqlite.get_pool()
            filename = absolutize_path(filename, frame_depth=cut_traceback_depth + 5)
        return SQLitePool(is_shared_memory_db, filename, create_db, **kwargs)

    def table_exists(
        self,
        connection: DbConnection,
        table_name: TableName,
        case_sensitive: bool = True,
    ) -> str | None:
        return self._exists(connection, table_name, None, case_sensitive)  # type: ignore[arg-type]

    def index_exists(
        self,
        connection: DbConnection,
        table_name: TableName,
        index_name: str,
        case_sensitive: bool = True,
    ) -> str | None:
        return self._exists(connection, table_name, index_name, case_sensitive)  # type: ignore[arg-type]

    def _exists(
        self,
        connection: DbConnection,
        table_name: str,
        index_name: str | None = None,
        case_sensitive: bool = True,
    ) -> str | None:
        db_name, table_name = self.split_table_name(table_name)

        catalog_name: str | tuple[str, str]
        if db_name is None:
            catalog_name = "sqlite_master"
        else:
            catalog_name = (db_name, "sqlite_master")
        catalog_name = self.quote_name(catalog_name)

        cursor = connection.cursor()
        if index_name is not None:
            sql = "SELECT name FROM %s WHERE type='index' AND name=?" % catalog_name
            if not case_sensitive:
                sql += " COLLATE NOCASE"
            cursor.execute(sql, [index_name])
        else:
            sql = "SELECT name FROM %s WHERE type='table' AND name=?" % catalog_name
            if not case_sensitive:
                sql += " COLLATE NOCASE"
            cursor.execute(sql, [table_name])
        row = cursor.fetchone()
        return cast("str | None", row[0] if row is not None else None)

    def fk_exists(self, connection, table_name, fk_name, case_sensitive: bool = True):
        assert False  # pragma: no cover

    def check_json1(self, connection: DbConnection) -> bool:
        cursor = connection.cursor()
        sql = """
            select json('{"this": "is", "a": ["test"]}')"""
        try:
            cursor.execute(sql)
            return True
        except sqlite.OperationalError:
            return False


provider_cls = SQLiteProvider


def _text_factory(s: bytes) -> str:
    return s.decode("utf8", "replace")


def make_string_function(
    name: str, base_func: Callable[[object], _T]
) -> Callable[[str], _T | None]:
    def func(value) -> _T | None:
        if value is None:
            return None
        t = type(value)
        if t is not str:
            if t is buffer:
                value = hexlify(value).decode("ascii")
            else:
                value = str(value)
        result = base_func(value)
        return result

    func.__name__ = name
    return func


py_upper: Callable[[str], Any] = make_string_function("py_upper", str.upper)  # type: ignore[arg-type]
py_lower: Callable[[str], Any] = make_string_function("py_lower", str.lower)  # type: ignore[arg-type]


def py_json_unwrap(value: str) -> str | None:
    # "[null,some_json]" -> "some_json"
    if isinstance(value, str) and value.startswith("[null,"):
        return value[6:-1]
    return None


path_cache: dict[str, tuple[int | str, ...] | None] = {}

json_path_re: re.Pattern[str] = re.compile(
    r'\[(-?\d+)\]|\.(?:(\w+)|"([^"]*)")', re.UNICODE
)


def _parse_path(path: str) -> tuple[int | str, ...] | None:
    if path in path_cache:
        return path_cache[path]
    keys: tuple[int | str, ...] | None = None
    if isinstance(path, str) and path.startswith("$"):
        parsed: list[int | str] = []
        pos = 1
        path_len = len(path)
        while pos < path_len:
            match = json_path_re.match(path, pos)
            if match is not None:
                g1, g2, g3 = match.groups()
                parsed.append(int(g1) if g1 else g2 or g3)
                pos = match.end()
            else:
                break
        else:
            keys = tuple(parsed)
    path_cache[path] = keys
    return keys


def _traverse(obj: object, keys: Iterable[str | int] | None) -> Any:
    if keys is None:
        return None
    list_or_dict = (list, dict)
    for key in keys:
        if type(obj) not in list_or_dict:
            return None
        try:
            obj = cast("Any", obj)[key]
        except (KeyError, IndexError):
            return None
    return obj


def _extract(expr, *paths):
    expr = json.loads(expr) if isinstance(expr, str) else expr
    result = []
    for path in paths:
        keys = _parse_path(path)
        result.append(_traverse(expr, keys))
    return result[0] if len(paths) == 1 else result


def py_json_extract(expr, *paths):
    result = _extract(expr, *paths)
    if type(result) in (list, dict):
        result = json.dumps(result, **SQLiteJsonConverter.json_kwargs)
    return result


def py_json_query(expr, path, with_wrapper):
    result = _extract(expr, path)
    if type(result) not in (list, dict):
        if not with_wrapper:
            return None
        result = [result]
    return json.dumps(result, **SQLiteJsonConverter.json_kwargs)


def py_json_value(expr, path):
    result = _extract(expr, path)
    return result if type(result) not in (list, dict) else None


def py_json_contains(expr: str, path: str, key: str) -> bool:
    expr = json.loads(expr) if isinstance(expr, str) else expr
    keys = _parse_path(path)
    expr = _traverse(expr, keys)
    return type(expr) in (list, dict) and key in expr


def py_json_nonzero(expr, path):
    expr = json.loads(expr) if isinstance(expr, str) else expr
    keys = _parse_path(path)
    expr = _traverse(expr, keys)
    return bool(expr)


def py_json_array_length(expr, path=None):
    expr = json.loads(expr) if isinstance(expr, str) else expr
    if path:
        keys = _parse_path(path)
        expr = _traverse(expr, keys)
    return len(expr) if type(expr) is list else 0


def wrap_array_func(func: Callable[..., _T]) -> Callable[..., _T | None]:
    @wraps(func)
    def new_func(array, *args) -> _T | None:
        if array is None:
            return None
        array = json.loads(array)
        return func(array, *args)

    return new_func


@wrap_array_func
def py_array_index(array: list, index: int) -> float | None:
    try:
        return array[index]
    except IndexError:
        return None


@wrap_array_func
def py_array_contains(array: list, item: object) -> bool:
    return item in array


@wrap_array_func
def py_array_subset(array: list, items: str) -> bool:
    if items is None:
        return None
    items = json.loads(items)
    return set(items).issubset(set(array))


@wrap_array_func
def py_array_length(array: list) -> int:
    return len(array)


@wrap_array_func
def py_array_slice(array: list, start: int | None, stop: int | None) -> str:
    return dumps(array[start:stop])


def py_make_array(*items) -> str:
    return dumps(items)


def py_string_slice(s: str, start: int | None, end: int | None) -> str:
    if s is None:
        return None
    if isinstance(start, str):
        start = int(start)
    if isinstance(end, str):
        end = int(end)
    return s[start:end]


class SQLitePool(Pool):
    def __init__(
        self, is_shared_memory_db: bool, filename: str, create_db: bool, **kwargs
    ) -> None:  # called separately in each thread
        self.is_shared_memory_db = is_shared_memory_db
        self.filename = filename
        self.create_db = create_db
        self.kwargs = kwargs
        self.con = None

    def _connect(self) -> None:
        filename = self.filename
        if self.is_shared_memory_db or self.filename == ":memory:":
            pass
        elif not self.create_db and not os.path.exists(filename):
            throw(IOError, "Database file is not found: %r" % filename)

        self.con = con = sqlite.connect(filename, isolation_level=None, **self.kwargs)
        con.text_factory = _text_factory

        def create_function(name, num_params, func):
            func = keep_exception(func)
            con.create_function(name, num_params, func)

        create_function("power", 2, pow)
        create_function("rand", 0, random)
        create_function("py_upper", 1, py_upper)
        create_function("py_lower", 1, py_lower)
        create_function("py_json_unwrap", 1, py_json_unwrap)
        create_function("py_json_extract", -1, py_json_extract)
        create_function("py_json_contains", 3, py_json_contains)
        create_function("py_json_nonzero", 2, py_json_nonzero)
        create_function("py_json_array_length", -1, py_json_array_length)

        create_function("py_array_index", 2, py_array_index)
        create_function("py_array_contains", 2, py_array_contains)
        create_function("py_array_subset", 2, py_array_subset)
        create_function("py_array_length", 1, py_array_length)
        create_function("py_array_slice", 3, py_array_slice)
        create_function("py_make_array", -1, py_make_array)

        create_function("py_string_slice", 3, py_string_slice)

        if sqlite.sqlite_version_info >= (3, 6, 19):
            con.execute("PRAGMA foreign_keys = true")

        con.execute("PRAGMA case_sensitive_like = true")

    def disconnect(self) -> None:
        if self.is_shared_memory_db or self.filename == ":memory:":
            pass
        else:
            Pool.disconnect(self)

    def drop(self, con: DbConnection) -> None:
        if self.is_shared_memory_db or self.filename == ":memory:":
            con.rollback()
        else:
            Pool.drop(self, con)
