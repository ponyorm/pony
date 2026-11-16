from __future__ import annotations

import json
import warnings
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any, ClassVar, cast
from uuid import UUID

from pony.converting import str2timedelta, timedelta2str
from pony.orm import core, dbapiprovider, dbschema, ormtypes
from pony.orm.core import ParamKey, TranslationError, log_orm
from pony.orm.dbapiprovider import (
    DBAPIProvider,
    Pool,
    get_version_tuple,
    wrap_dbapi_exceptions,
)
from pony.orm.sqlbuilding import CompositeParam, Param, SQLBuilder, Value, join
from pony.orm.sqltranslation import SQLTranslator
from pony.py23compat import buffer, int_types
from pony.utils import throw

if TYPE_CHECKING:
    from collections.abc import Sequence

    from typing_extensions import LiteralString

try:
    import MySQLdb as mysql_module
    import MySQLdb.converters as mysql_converters
    from MySQLdb import string_literal
    from MySQLdb.constants import CLIENT, FIELD_TYPE, FLAG

    mysql_module_name = "MySQLdb"
except ImportError:
    try:
        import pymysql as mysql_module
    except ImportError:
        raise ImportError(
            "In order to use PonyORM with MySQL please install MySQLdb or pymysql"
        )
    import pymysql.converters as mysql_converters
    from pymysql.constants import CLIENT, FIELD_TYPE, FLAG
    from pymysql.converters import escape_str as string_literal

    mysql_module_name = "pymysql"

NoneType = type(None)

warnings.filterwarnings(
    "ignore", "^Table.+already exists$", Warning, "^pony\\.orm\\.dbapiprovider$"
)


class MySQLColumn(dbschema.Column):
    auto_template: ClassVar[str | None] = "%(type)s PRIMARY KEY AUTO_INCREMENT"


class MySQLSchema(dbschema.DBSchema):
    dialect: ClassVar[str] = "MySQL"
    inline_fk_syntax: ClassVar[bool] = False
    column_class: ClassVar[type[dbschema.Column]] = MySQLColumn


class MySQLTranslator(SQLTranslator):
    dialect: ClassVar[str] = "MySQL"
    json_path_wildcard_syntax: ClassVar[bool] = True


class MySQLValue(Value):
    __slots__: list[str] = []

    def __str__(self):
        value = self.value
        if isinstance(value, timedelta):
            if value.microseconds:
                return "INTERVAL '%s' HOUR_MICROSECOND" % timedelta2str(value)
            return "INTERVAL '%s' HOUR_SECOND" % timedelta2str(value)
        return Value.__str__(self)


class MySQLBuilder(SQLBuilder):
    dialect: ClassVar[str] = "MySQL"
    value_class: type[Value] = MySQLValue

    def CONCAT(self, *args) -> tuple[str, list[str], str]:
        return "concat(", join(", ", map(self, args)), ")"

    def TRIM(self, expr, chars=None):
        if chars is None:
            return "trim(", self(expr), ")"
        return "trim(both ", self(chars), " from ", self(expr), ")"

    def LTRIM(self, expr, chars=None):
        if chars is None:
            return "ltrim(", self(expr), ")"
        return "trim(leading ", self(chars), " from ", self(expr), ")"

    def RTRIM(self, expr, chars=None):
        if chars is None:
            return "rtrim(", self(expr), ")"
        return "trim(trailing ", self(chars), " from ", self(expr), ")"

    def TO_INT(self, expr):
        return "CAST(", self(expr), " AS SIGNED)"

    def TO_REAL(self, expr):
        return "CAST(", self(expr), " AS DOUBLE)"

    def TO_STR(self, expr):
        return "CAST(", self(expr), " AS CHAR)"

    def YEAR(self, expr):
        return "year(", self(expr), ")"

    def MONTH(self, expr):
        return "month(", self(expr), ")"

    def DAY(self, expr):
        return "day(", self(expr), ")"

    def HOUR(self, expr):
        return "hour(", self(expr), ")"

    def MINUTE(self, expr):
        return "minute(", self(expr), ")"

    def SECOND(self, expr):
        return "second(", self(expr), ")"

    def DATE_ADD(
        self,
        expr: list[str],
        delta: list[(str | timedelta | ParamKey | MySQLTimedeltaConverter)],
    ) -> tuple[str, Any, str, Any, str]:
        if delta[0] == "VALUE" and isinstance(delta[1], time):
            return "ADDTIME(", self(expr), ", ", self(delta), ")"
        return "ADDDATE(", self(expr), ", ", self(delta), ")"

    def DATE_SUB(self, expr, delta) -> tuple[str, Any, str, Any, str]:
        if delta[0] == "VALUE" and isinstance(delta[1], time):
            return "SUBTIME(", self(expr), ", ", self(delta), ")"
        return "SUBDATE(", self(expr), ", ", self(delta), ")"

    def DATE_DIFF(self, expr1, expr2) -> tuple[str, Any, str, Any, str]:
        return "TIMEDIFF(", self(expr1), ", ", self(expr2), ")"

    def DATETIME_ADD(self, expr, delta) -> tuple[str, Any, str, Any, str]:
        return self.DATE_ADD(expr, delta)

    def DATETIME_SUB(self, expr, delta) -> tuple[str, Any, str, Any, str]:
        return self.DATE_SUB(expr, delta)

    def DATETIME_DIFF(self, expr1, expr2) -> tuple[str, Any, str, Any, str]:
        return "TIMEDIFF(", self(expr1), ", ", self(expr2), ")"

    def JSON_QUERY(self, expr, path) -> tuple[str, Any, str, Param | Value, str]:
        path_sql, has_params, has_wildcards = self.build_json_path(path)
        return "json_extract(", self(expr), ", ", path_sql, ")"

    def JSON_VALUE(
        self, expr, path, type
    ) -> tuple[str, tuple[str, Any, str, Param | Value, str], str]:
        path_sql, has_params, has_wildcards = self.build_json_path(path)
        result = "json_extract(", self(expr), ", ", path_sql, ")"
        if type is NoneType:
            return "NULLIF(", result, ", CAST('null' as JSON))"
        if type in (bool, int):
            return "CAST(", result, " AS SIGNED)"
        if type is float:
            return "CAST(", result, " AS DOUBLE)"
        return "json_unquote(", result, ")"

    def JSON_NONZERO(self, expr):
        return (
            "COALESCE(CAST(",
            self(expr),
            """ as CHAR), 'null') NOT IN ('null', 'false', '0', '""', '[]', '{}')""",
        )

    def JSON_ARRAY_LENGTH(self, value):
        return "json_length(", self(value), ")"

    def JSON_EQ(self, left, right):
        return "(", self(left), " = CAST(", self(right), " AS JSON))"

    def JSON_NE(self, left, right):
        return "(", self(left), " != CAST(", self(right), " AS JSON))"

    def JSON_CONTAINS(
        self, expr: Sequence[str], path: list[Sequence[str]], key: Sequence
    ) -> list[str | Param | Value | Any]:
        key_sql = self(key)
        wrapped_key: Value | CompositeParam
        if isinstance(key_sql, Value):
            wrapped_key = self.value_class(self.paramstyle, json.dumps([key_sql.value]))
        elif isinstance(key_sql, Param):
            wrapped_key = self.make_composite_param(
                (key_sql.paramkey,), [key_sql], self.wrap_param_to_json_array
            )
        else:
            assert False
        expr_sql = self(expr)
        result = ["(json_contains(", expr_sql, ", ", wrapped_key]
        path_sql, has_params, has_wildcards = self.build_json_path(path)
        if has_wildcards:
            throw(TranslationError, "Wildcards are not allowed in json_contains()")
        path_with_key_sql, _, _ = self.build_json_path(path + [key])
        result += [
            ", ",
            path_sql,
            ") or json_contains_path(",
            expr_sql,
            ", 'one', ",
            path_with_key_sql,
            "))",
        ]
        return result

    @classmethod
    def wrap_param_to_json_array(cls, values):
        return json.dumps(values)

    def JSON_PARAM(self, expr):
        return "CAST(", self(expr), " AS JSON)"


class MySQLStrConverter(dbapiprovider.StrConverter):
    def sql_type(self) -> str:
        result = "VARCHAR(%d)" % self.max_len if self.max_len else "LONGTEXT"
        if self.db_encoding:
            result += " CHARACTER SET %s" % self.db_encoding
        return result


class MySQLRealConverter(dbapiprovider.RealConverter):
    def sql_type(self) -> str:
        return "DOUBLE"


class MySQLBlobConverter(dbapiprovider.BlobConverter):
    def sql_type(self) -> str:
        return "LONGBLOB"


class MySQLTimeConverter(dbapiprovider.TimeConverter):
    def sql2py(self, val):
        if isinstance(val, timedelta):  # MySQLdb returns timedeltas instead of times
            total_seconds = val.days * (24 * 60 * 60) + val.seconds
            if 0 <= total_seconds <= 24 * 60 * 60:
                minutes, seconds = divmod(total_seconds, 60)
                hours, minutes = divmod(minutes, 60)
                return time(hours, minutes, seconds, val.microseconds)
        elif not isinstance(val, time):
            throw(
                ValueError,
                "Value of unexpected type received from database%s: instead of time or timedelta got %s"
                % (
                    "for attribute %s" % self.attr if self.attr else "",
                    type(val),
                ),
            )
        assert isinstance(val, time)
        return val


class MySQLTimedeltaConverter(dbapiprovider.TimedeltaConverter):
    sql_type_name: LiteralString = "TIME"


class MySQLUuidConverter(dbapiprovider.UuidConverter):
    def sql_type(self) -> str:
        return "BINARY(16)"


class MySQLJsonConverter(dbapiprovider.JsonConverter):
    EQ = "JSON_EQ"
    NE = "JSON_NE"

    def init(self, kwargs):
        assert isinstance(self.provider.server_version, tuple)
        if self.provider.server_version < (5, 7, 8):
            version = ".".join(map(str, self.provider.server_version))
            raise NotImplementedError("MySQL %s has no JSON support" % version)


class MySQLProvider(DBAPIProvider):
    dialect: str = "MySQL"
    paramstyle: str = "format"
    quote_char = "`"
    max_name_len = 64
    max_params_count = 10000
    table_if_not_exists_syntax = True
    index_if_not_exists_syntax = False
    max_time_precision = default_time_precision = 0
    varchar_default_max_len = 255
    uint64_support = True

    dbapi_module = mysql_module
    dbschema_cls = MySQLSchema
    translator_cls = MySQLTranslator
    sqlbuilder_cls = MySQLBuilder

    fk_types = {"SERIAL": "BIGINT UNSIGNED"}

    converter_classes = [
        (NoneType, dbapiprovider.NoneConverter),
        (bool, dbapiprovider.BoolConverter),
        (str, MySQLStrConverter),
        (int_types, dbapiprovider.IntConverter),
        (float, MySQLRealConverter),
        (Decimal, dbapiprovider.DecimalConverter),
        (datetime, dbapiprovider.DatetimeConverter),
        (date, dbapiprovider.DateConverter),
        (time, MySQLTimeConverter),
        (timedelta, MySQLTimedeltaConverter),
        (UUID, MySQLUuidConverter),
        (buffer, MySQLBlobConverter),
        (ormtypes.Json, MySQLJsonConverter),
    ]

    def normalize_name(self, name):
        return name[: self.max_name_len].lower()

    @wrap_dbapi_exceptions
    def inspect_connection(self, connection):
        cursor = connection.cursor()
        cursor.execute("select version()")
        row = cursor.fetchone()
        assert row is not None
        self.server_version = get_version_tuple(row[0])  # pyright: ignore[reportArgumentType]
        assert self.server_version is not None
        if self.server_version >= (5, 6, 4):
            self.max_time_precision = 6
        cursor.execute("select database()")
        db_name = cursor.fetchone()
        assert db_name is not None
        self.default_schema_name = cast("str", db_name[0])
        cursor.execute("set session group_concat_max_len = 4294967295")

    def should_reconnect(self, exc):
        return isinstance(exc, mysql_module.OperationalError) and exc.args[0] in (
            2006,
            2013,
        )

    def get_pool(self, *args, **kwargs):
        if "conv" not in kwargs:
            conv = mysql_converters.conversions.copy()
            if mysql_module_name == "MySQLdb":
                conv[FIELD_TYPE.BLOB] = [(FLAG.BINARY, buffer)]  # pyright: ignore[reportArgumentType]

            def encode_timedelta(val, encoders=None):
                # MySQLdb's string_literal takes exactly one argument
                return string_literal(timedelta2str(val))

            conv[timedelta] = encode_timedelta
            conv[FIELD_TYPE.TIMESTAMP] = str2datetime
            conv[FIELD_TYPE.DATETIME] = str2datetime
            conv[FIELD_TYPE.TIME] = str2timedelta
            kwargs["conv"] = conv
        if "charset" not in kwargs:
            kwargs["charset"] = "utf8"
        kwargs["client_flag"] = kwargs.get("client_flag", 0) | CLIENT.FOUND_ROWS
        return Pool(mysql_module, *args, **kwargs)

    @wrap_dbapi_exceptions
    def set_transaction_mode(self, connection, cache):
        assert not cache.in_transaction
        db_session = cache.db_session
        if db_session is not None and db_session.ddl:
            cursor = connection.cursor()
            cursor.execute("SHOW VARIABLES LIKE 'foreign_key_checks'")
            fk = cursor.fetchone()
            if fk is not None:
                fk = fk[1] == "ON"
            if fk:
                sql = "SET foreign_key_checks = 0"
                if core.local.debug:
                    log_orm(sql)
                cursor.execute(sql)
            cache.saved_fk_state = bool(fk)
            cache.in_transaction = True
        cache.immediate = True
        if db_session is not None and db_session.serializable:
            cursor = connection.cursor()
            sql = "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"
            if core.local.debug:
                log_orm(sql)
            cursor.execute(sql)
            cache.in_transaction = True

    @wrap_dbapi_exceptions
    def release(self, connection, cache=None):
        if cache is not None:
            db_session = cache.db_session
            if db_session is not None and db_session.ddl and cache.saved_fk_state:
                try:
                    cursor = connection.cursor()
                    sql = "SET foreign_key_checks = 1"
                    if core.local.debug:
                        log_orm(sql)
                    cursor.execute(sql)
                except:
                    self.pool.drop(connection)
                    raise
        DBAPIProvider.release(self, connection, cache)

    def table_exists(self, connection, table_name, case_sensitive=True):
        db_name, table_name = self.split_table_name(table_name)
        cursor = connection.cursor()
        if case_sensitive:
            sql = (
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema=%s and table_name=%s"
            )
        else:
            sql = (
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema=%s and UPPER(table_name)=UPPER(%s)"
            )
        cursor.execute(sql, [db_name, table_name])
        row = cursor.fetchone()
        return cast("str", row[0]) if row is not None else None

    def index_exists(self, connection, table_name, index_name, case_sensitive=True):
        db_name, table_name = self.split_table_name(table_name)
        if case_sensitive:
            sql = (
                "SELECT index_name FROM information_schema.statistics "
                "WHERE table_schema=%s and table_name=%s and index_name=%s"
            )
        else:
            sql = (
                "SELECT index_name FROM information_schema.statistics "
                "WHERE table_schema=%s and table_name=%s and UPPER(index_name)=UPPER(%s)"
            )
        cursor = connection.cursor()
        cursor.execute(sql, [db_name, table_name, index_name])
        row = cursor.fetchone()
        return cast("str", row[0]) if row is not None else None

    def fk_exists(self, connection, table_name, fk_name, case_sensitive=True):
        db_name, table_name = self.split_table_name(table_name)
        if case_sensitive:
            sql = (
                "SELECT constraint_name FROM information_schema.table_constraints "
                "WHERE table_schema=%s and table_name=%s "
                "and constraint_type='FOREIGN KEY' and constraint_name=%s"
            )
        else:
            sql = (
                "SELECT constraint_name FROM information_schema.table_constraints "
                "WHERE table_schema=%s and table_name=%s "
                "and constraint_type='FOREIGN KEY' and UPPER(constraint_name)=UPPER(%s)"
            )
        cursor = connection.cursor()
        cursor.execute(sql, [db_name, table_name, fk_name])
        row = cursor.fetchone()
        return cast("str | None", row[0] if row is not None else None)


provider_cls = MySQLProvider


def str2datetime(s):
    if 19 < len(s) < 26:
        s += "000000"[: 26 - len(s)]
    s = s.replace("-", " ").replace(":", " ").replace(".", " ").replace("T", " ")
    try:
        return datetime(*map(int, s.split()))  # pyright: ignore[reportArgumentType]
    except ValueError:
        return None  # for incorrect values like 0000-00-00 00:00:00
