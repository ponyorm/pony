from __future__ import annotations

import json
import os
import re
from datetime import date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Concatenate,
    Generic,
    ParamSpec,
    SupportsFloat,
    SupportsIndex,
    SupportsInt,
    TypeAlias,
    TypeVar,
    cast,
)
from uuid import UUID, uuid4

from pony.converting import str2date, str2datetime, str2time, str2timedelta
from pony.orm import core
from pony.orm.ormtypes import (
    Array,
    Json,
    LongStr,
    LongUnicode,
    QueryType,
    RawSQLType,
    TrackedArray,
    TrackedValue,
)
from pony.py23compat import buffer, int_types
from pony.utils import decorator, deprecated, localbase, throw

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Sequence
    from types import ModuleType

    import _typeshed
    from typing_extensions import LiteralString, Self

    from pony.orm.core import (
        Attribute,
        CachedSqlSimple,
        Database,
        DbConnection,
        DbCursor,
        Entity,
        EntityMeta,
        OptAttrValue,
        PyType,
        QueryVars,
        SessionCache,
        Set,
        TableName,
        VarTypes,
    )
    from pony.orm.dbproviders.sqlite import SQLiteProvider
    from pony.orm.dbschema import DBSchema
    from pony.orm.sqlbuilding import SQLBuilder
    from pony.orm.sqltranslation import SQLTranslator

_ExtPyType: TypeAlias = "PyType | RawSQLType | type[None]"

_T = TypeVar("_T")
_P = ParamSpec("_P")
_OAT_co = TypeVar("_OAT_co", bound="OptAttrValue", covariant=True)


class DBException(Exception):
    def __init__(self, original_exc: Exception, *args) -> None:
        args = args or getattr(original_exc, "args", ())
        Exception.__init__(self, *args)
        self.original_exc = original_exc


# Exception inheritance layout of DBAPI 2.0-compatible provider:
#
# Exception
#   Warning
#   Error
#     InterfaceError
#     DatabaseError
#       DataError
#       OperationalError
#       IntegrityError
#       InternalError
#       ProgrammingError
#       NotSupportedError


class Warning(DBException):
    pass


class Error(DBException):
    pass


class InterfaceError(Error):
    pass


class DatabaseError(Error):
    pass


class DataError(DatabaseError):
    pass


class OperationalError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class InternalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    pass


@decorator
def wrap_dbapi_exceptions(
    func: Callable[Concatenate[DBAPIProvider, _P], _T],
    provider: DBAPIProvider,
    /,
    *args: _P.args,
    **kwargs: _P.kwargs,
) -> _T:
    dbapi_module = provider.dbapi_module
    should_retry = False
    try:
        try:
            if provider.dialect != "SQLite":
                return func(provider, *args, **kwargs)
            else:
                provider = cast("SQLiteProvider", provider)
                provider.local_exceptions.keep_traceback = True
                try:
                    return func(provider, *args, **kwargs)
                finally:
                    provider.local_exceptions.keep_traceback = False
        except dbapi_module.NotSupportedError as e:
            raise NotSupportedError(e)
        except dbapi_module.ProgrammingError as e:
            if provider.dialect == "PostgreSQL":
                msg = str(e)
                if msg.startswith("operator does not exist:") and " json " in msg:
                    msg += " (Note: use column type `jsonb` instead of `json`)"
                    raise ProgrammingError(e, msg, *e.args[1:])
            raise ProgrammingError(e)
        except dbapi_module.InternalError as e:
            raise InternalError(e)
        except dbapi_module.IntegrityError as e:
            raise IntegrityError(e)
        except dbapi_module.OperationalError as e:
            if provider.dialect == "PostgreSQL" and e.pgcode == "40001":
                should_retry = True
            if provider.dialect == "SQLite":
                provider = cast("SQLiteProvider", provider)
                provider.restore_exception()
            raise OperationalError(e)
        except dbapi_module.DataError as e:
            raise DataError(e)
        except dbapi_module.DatabaseError as e:
            raise DatabaseError(e)
        except dbapi_module.InterfaceError as e:
            if (
                e.args == (0, "")
                and getattr(dbapi_module, "__name__", None) == "MySQLdb"
            ):
                throw(InterfaceError, e, "MySQL server misconfiguration")
            raise InterfaceError(e)
        except dbapi_module.Error as e:
            raise Error(e)
        except dbapi_module.Warning as e:
            raise Warning(e)
    except Exception as e:
        if should_retry:
            setattr(e, "should_retry", True)  # noqa: B010
        raise


def unexpected_args(attr, args):
    throw(
        TypeError,
        "Unexpected positional argument{} for attribute {}: {}".format(
            (len(args) > 1 and "s") or "", attr, ", ".join(repr(arg) for arg in args)
        ),
    )


version_re = re.compile(r"[0-9\.]+")


def get_version_tuple(s: str) -> tuple[int, ...] | None:
    m = version_re.match(s)
    if m is not None:
        components = m.group(0).split(".")
        return tuple(int(component) for component in components)
    return None


class DBAPIProvider:
    database: Database | None
    pool: Pool
    server_version: int | tuple[int, ...] | None

    paramstyle: str = "qmark"
    quote_char: str = '"'
    max_params_count: int = 999
    max_name_len: int = 128
    table_if_not_exists_syntax: bool = True
    index_if_not_exists_syntax: bool = True
    max_time_precision: int = 6
    default_time_precision: int = 6
    uint64_support: bool = False

    # SQLite and PostgreSQL does not limit varchar max length.
    varchar_default_max_len: int | None = None

    dialect: str | _typeshed.MaybeNone = None
    dbapi_module: ModuleType | _typeshed.MaybeNone = None
    dbschema_cls: type[DBSchema] | _typeshed.MaybeNone = None
    translator_cls: type[SQLTranslator] | _typeshed.MaybeNone = None
    sqlbuilder_cls: type[SQLBuilder] | _typeshed.MaybeNone = None
    array_converter_cls: type[ArrayConverter] | _typeshed.MaybeNone = None

    name_before_table: str = "schema_name"
    default_schema_name: str | _typeshed.MaybeNone = None

    fk_types: dict[str, str] = {"SERIAL": "INTEGER", "BIGSERIAL": "BIGINT"}

    def __init__(self, _database: Database | None, *args, **kwargs) -> None:
        self.database = _database
        pool_mockup = kwargs.pop("pony_pool_mockup", None)
        call_on_connect = kwargs.pop("pony_call_on_connect", None)
        if pool_mockup:
            self.pool = pool_mockup
        else:
            self.pool = self.get_pool(*args, **kwargs)
        connection, is_new_connection = self.connect()
        if call_on_connect:
            call_on_connect(connection)
        self.inspect_connection(connection)
        self.release(connection)

    @wrap_dbapi_exceptions
    def inspect_connection(self, connection: DbConnection) -> None:
        pass

    def normalize_name(self, name: str) -> str:
        return name[: self.max_name_len]

    def get_default_entity_table_name(self, entity: EntityMeta) -> str:
        return self.normalize_name(entity.__name__)

    def get_default_m2m_table_name(self, attr: Set, reverse: Set) -> str:
        if attr.symmetric:
            assert reverse is attr
            name = attr.entity.__name__ + "_" + attr.name
        else:
            name = attr.entity.__name__ + "_" + reverse.entity.__name__
        return self.normalize_name(name)

    def get_default_column_names(
        self, attr: Attribute, reverse_pk_columns: list[str] | None = None
    ) -> list[str]:
        normalize_name = self.normalize_name
        if reverse_pk_columns is None:
            return [normalize_name(attr.name)]
        elif len(reverse_pk_columns) == 1:
            return [normalize_name(attr.name)]
        else:
            prefix = attr.name + "_"
            return [normalize_name(prefix + column) for column in reverse_pk_columns]

    def get_default_m2m_column_names(self, entity: EntityMeta) -> list[str]:
        normalize_name = self.normalize_name
        columns = entity._get_pk_columns_()
        if len(columns) == 1:
            return [normalize_name(entity.__name__.lower())]
        else:
            prefix = entity.__name__.lower() + "_"
            return [normalize_name(prefix + column) for column in columns]

    def get_default_index_name(
        self,
        table_name: TableName,
        column_names: Iterable[str],
        is_pk: bool = False,
        is_unique: bool | None = False,
        m2m: bool = False,
    ) -> str:
        if is_pk:
            index_name = "pk_%s" % self.base_name(table_name)
        else:
            if is_unique:
                template = "unq_%(tname)s__%(cnames)s"
            elif m2m:
                template = "idx_%(tname)s"
            else:
                template = "idx_%(tname)s__%(cnames)s"
            index_name = template % dict(
                tname=self.base_name(table_name),
                cnames="_".join(name for name in column_names),
            )
        return self.normalize_name(index_name.lower())

    def get_default_fk_name(
        self,
        child_table_name: TableName,
        parent_table_name: TableName,
        child_column_names: Iterable[str],
    ) -> str:
        fk_name = "fk_%s__%s" % (
            self.base_name(child_table_name),
            "__".join(child_column_names),
        )
        return self.normalize_name(fk_name.lower())

    def split_table_name(self, table_name: TableName) -> tuple[str, str]:
        if isinstance(table_name, str):
            return self.default_schema_name, table_name
        if not table_name:
            throw(TypeError, "Invalid table name: %r" % table_name)
        if len(table_name) != 2:
            size = len(table_name)
            throw(
                TypeError,
                "%s qualified table name must have two components: "
                "%s and table_name. Got %d component%s: %s"
                % (
                    self.dialect,
                    self.name_before_table,
                    size,
                    "s" if size != 1 else "",
                    table_name,
                ),
            )
        return table_name[0], table_name[1]

    def base_name(self, name: TableName) -> str:
        if not isinstance(name, str):
            assert type(name) is tuple
            name = name[-1]
            assert isinstance(name, str)
        return name

    def quote_name(self, name: str | Iterable[str]) -> str:
        quote_char = self.quote_char
        if isinstance(name, str):
            name = name.replace(quote_char, quote_char + quote_char)
            return quote_char + name + quote_char
        return ".".join(self.quote_name(item) for item in name)

    def format_table_name(self, name: TableName) -> str:
        return self.quote_name(name)

    def normalize_vars(
        self,
        vars: QueryVars,
        vartypes: VarTypes,
    ) -> None:
        for key, value in vars.items():
            vartype = vartypes[key]
            if isinstance(vartype, QueryType):
                vartypes[key], vars[key] = cast("Any", value)._normalize_var(vartype)

    def ast2sql(self, ast: list) -> CachedSqlSimple:
        builder = self.sqlbuilder_cls(self, ast)
        return builder.sql, builder.adapter

    def should_reconnect(self, exc: Exception) -> bool:
        return False

    @wrap_dbapi_exceptions
    def connect(self) -> tuple[DbConnection, bool]:
        return self.pool.connect()

    @wrap_dbapi_exceptions
    def set_transaction_mode(self, connection, cache):
        pass

    @wrap_dbapi_exceptions
    def commit(
        self, connection: DbConnection, cache: SessionCache | None = None
    ) -> None:
        if core.local.debug:
            core.log_orm("COMMIT")
        connection.commit()
        if cache is not None:
            cache.in_transaction = False

    @wrap_dbapi_exceptions
    def rollback(
        self, connection: DbConnection, cache: SessionCache | None = None
    ) -> None:
        if core.local.debug:
            core.log_orm("ROLLBACK")
        connection.rollback()
        if cache is not None:
            cache.in_transaction = False

    @wrap_dbapi_exceptions
    def release(
        self, connection: DbConnection, cache: SessionCache | None = None
    ) -> None:
        if cache is not None and cache.db_session is not None and cache.db_session.ddl:
            self.drop(connection, cache)
        else:
            if core.local.debug:
                core.log_orm("RELEASE CONNECTION")
            self.pool.release(connection)

    @wrap_dbapi_exceptions
    def drop(self, connection: DbConnection, cache: SessionCache | None = None) -> None:
        if core.local.debug:
            core.log_orm("CLOSE CONNECTION")
        self.pool.drop(connection)
        if cache is not None:
            cache.in_transaction = False

    @wrap_dbapi_exceptions
    def disconnect(self) -> None:
        if core.local.debug:
            core.log_orm("DISCONNECT")
        self.pool.disconnect()

    @wrap_dbapi_exceptions
    def execute(
        self,
        cursor: DbCursor,
        sql: str,
        arguments: Sequence | None = None,
        returning_id: bool = False,
    ) -> int | _typeshed.MaybeNone:
        if type(arguments) is list:
            assert arguments and not returning_id
            cursor.executemany(sql, arguments)
        else:
            if arguments is None:
                cursor.execute(sql)
            else:
                cursor.execute(sql, arguments)
            if returning_id:
                return cursor.lastrowid
        return None

    converter_classes: list[
        tuple[PyType | type[None] | tuple[type[int]], type[Converter]]  # noqa: Y090
    ] = []

    def _get_converter_type_by_py_type(self, py_type: _ExtPyType) -> type[Converter]:
        if isinstance(py_type, type):
            for t, converter_cls in self.converter_classes:
                if issubclass(py_type, t):
                    return converter_cls
            if issubclass(py_type, Array):
                converter_cls = self.array_converter_cls
                if converter_cls is None:
                    throw(
                        NotImplementedError,
                        "Array type is not supported for %r" % self.dialect,
                    )
                assert converter_cls is not None and issubclass(
                    converter_cls, ArrayConverter
                )
                return converter_cls
        if isinstance(py_type, RawSQLType):
            return Converter  # for cases like select(raw_sql(...) for x in X)
        throw(TypeError, "No database converter found for type %s" % py_type)

    def get_converter_by_py_type(self, py_type: _ExtPyType) -> Converter:
        converter_cls = self._get_converter_type_by_py_type(py_type)
        return converter_cls(self, py_type)  # type: ignore[arg-type]

    def get_converter_by_attr(self, attr: Attribute) -> Converter:
        py_type = attr.py_type
        converter_cls = self._get_converter_type_by_py_type(py_type)
        return converter_cls(self, py_type, attr)

    def get_pool(self, *args, **kwargs) -> Pool:
        return Pool(self.dbapi_module, *args, **kwargs)

    def table_exists(
        self,
        connection: DbConnection,
        table_name: TableName,
        case_sensitive: bool = True,
    ) -> str | None:
        throw(NotImplementedError)

    def index_exists(
        self,
        connection: DbConnection,
        table_name: TableName,
        index_name: str,
        case_sensitive: bool = True,
    ) -> str | None:
        throw(NotImplementedError)

    def fk_exists(
        self,
        connection: DbConnection,
        table_name: TableName,
        fk_name: str,
        case_sensitive: bool = True,
    ) -> str | None:
        throw(NotImplementedError)

    def table_has_data(self, connection: DbConnection, table_name: TableName) -> bool:
        cursor = connection.cursor()
        cursor.execute("SELECT 1 FROM %s LIMIT 1" % self.quote_name(table_name))
        return cursor.fetchone() is not None

    def disable_fk_checks(self, connection):
        pass

    def enable_fk_checks(self, connection, prev_state):
        pass

    def drop_table(self, connection: DbConnection, table_name: TableName) -> None:
        cursor = connection.cursor()
        sql = "DROP TABLE %s" % self.quote_name(table_name)
        cursor.execute(sql)


class Pool(localbase):
    args: tuple
    con: DbConnection | _typeshed.MaybeNone
    dbapi_module: ModuleType
    kwargs: dict[str, Any]
    pid: int | _typeshed.MaybeNone

    forked_connections: ClassVar[list[tuple[DbConnection, int]]] = []

    def __init__(
        self, dbapi_module: ModuleType, *args, **kwargs
    ) -> None:  # called separately in each thread
        self.dbapi_module = dbapi_module
        self.args = args
        self.kwargs = kwargs
        self.con = self.pid = None

    def connect(self) -> tuple[DbConnection, bool]:
        pid = os.getpid()
        if self.con is not None and self.pid != pid:
            self.forked_connections.append((self.con, self.pid))
            self.con = self.pid = None
        is_new_connection = False
        if self.con is None:
            if core.local.debug:
                core.log_orm("GET NEW CONNECTION")
            is_new_connection = True
            self._connect()
            self.pid = pid
        elif core.local.debug:
            core.log_orm("GET CONNECTION FROM THE LOCAL POOL")
        connection = self.con
        assert connection is not None
        return connection, is_new_connection

    def _connect(self):
        self.con = self.dbapi_module.connect(*self.args, **self.kwargs)

    def release(self, con: DbConnection) -> None:
        assert con is self.con
        try:
            con.rollback()
        except:
            self.drop(con)
            raise

    def drop(self, con: DbConnection) -> None:
        assert con is self.con, (con, self.con)
        self.con = None
        con.close()

    def disconnect(self) -> None:
        con = self.con
        self.con = None
        if con is not None:
            con.close()


class Converter(Generic[_OAT_co]):
    provider: DBAPIProvider
    py_type: type

    EQ: ClassVar[LiteralString] = "EQ"
    NE: ClassVar[LiteralString] = "NE"
    optimistic: ClassVar[bool] = True

    def __deepcopy__(self, memo: dict[int, object]) -> Self:
        return self  # Converter instances are "immutable"

    def __init__(
        self,
        provider: DBAPIProvider,
        py_type: type,
        attr: Attribute[_OAT_co] | None = None,
    ) -> None:
        self.provider = provider
        self.py_type = py_type
        self.attr = attr
        if attr is None:
            return
        kwargs = attr.kwargs.copy()
        self.init(kwargs)
        for option in kwargs:
            throw(TypeError, "Attribute %s has unknown option %r" % (attr, option))

    def init(self, kwargs: dict[str, Any]) -> None:
        attr = self.attr
        if attr and attr.args:
            unexpected_args(attr, attr.args)

    def validate(
        self, val: Any, obj: Any | None = None
    ) -> OptAttrValue | TrackedValue | list:
        return val

    def py2sql(self, val: Any) -> OptAttrValue:
        return val

    def sql2py(self, val: str | Any, /) -> OptAttrValue:
        return val

    def val2dbval(
        self, val: OptAttrValue | TrackedValue, obj: Entity | None = None
    ) -> Any:
        return val

    def dbval2val(
        self, dbval: OptAttrValue | Entity, obj: Entity | _typeshed.MaybeNone = None
    ) -> Any:
        return dbval

    def dbvals_equal(self, x: Any, y: Any) -> bool:
        return x == y

    def get_sql_type(self, attr: Attribute | None = None) -> str:
        if attr is not None and attr.sql_type is not None:
            return attr.sql_type
        attr = self.attr
        if attr is not None and attr.sql_type is not None:
            assert len(attr.columns) == 1
            return self.get_fk_type(attr.sql_type)
        if attr is not None and attr.reverse and not attr.is_collection:
            i = attr.converters.index(self)
            rentity = attr.reverse.entity
            rpk_converters = rentity._pk_converters_
            assert rpk_converters is not None and len(attr.converters) == len(
                rpk_converters
            )
            rconverter = rpk_converters[i]
            return rconverter.sql_type()
        return self.sql_type()

    if TYPE_CHECKING:

        def sql_type(self) -> Any: ...

    def get_fk_type(self, sql_type: str) -> str:
        fk_types = self.provider.fk_types
        if sql_type.isupper():
            return fk_types.get(sql_type, sql_type)
        sql_type = sql_type.upper()
        return fk_types.get(sql_type, sql_type).lower()


class NoneConverter(Converter):  # used for raw_sql() parameters only
    py_type: type[None]

    def __init__(
        self, provider: DBAPIProvider, py_type: type[None], attr: None = None
    ) -> None:
        if attr is not None:
            throw(TypeError, "Attribute %s has invalid type NoneType" % attr)
        Converter.__init__(self, provider, py_type)

    def get_sql_type(self, attr=None):
        assert False

    def get_fk_type(self, sql_type):
        assert False


class BoolConverter(Converter):
    py_type: type[bool]

    def validate(self, val: bool | int, obj: Entity | None = None) -> bool:
        return bool(val)

    def sql2py(self, val: object) -> bool:
        return bool(val)

    def sql_type(self) -> str:
        return "BOOLEAN"


class StrConverter(Converter):
    autostrip: bool
    db_encoding: str | None
    max_len: int | None
    py_type: type[str | LongStr]

    def __init__(
        self,
        provider: DBAPIProvider,
        py_type: type[str | LongStr],
        attr: Attribute | None = None,
    ) -> None:
        self.max_len = None
        self.db_encoding = None
        Converter.__init__(self, provider, py_type, attr)

    def init(self, kwargs: dict[str, Any]) -> None:
        attr = self.attr
        assert attr is not None
        max_len = kwargs.pop("max_len", None)
        if len(attr.args) > 1:
            unexpected_args(attr, attr.args[1:])
        elif attr.args:
            if max_len is not None:
                throw(
                    TypeError,
                    "Max length option specified twice: as a positional argument and as a `max_len` named argument",
                )
            max_len = attr.args[0]
        if issubclass(attr.py_type, (LongStr, LongUnicode)):
            if max_len is not None:
                throw(TypeError, "Max length is not supported for CLOBs")
        elif max_len is None:
            max_len = self.provider.varchar_default_max_len
        elif not isinstance(max_len, int_types):
            throw(TypeError, "Max length argument must be int. Got: %r" % max_len)
        self.max_len = max_len
        self.db_encoding = kwargs.pop("db_encoding", None)
        self.autostrip = kwargs.pop("autostrip", True)

    def validate(
        self, val: str, obj: Entity | None = None
    ) -> str | _typeshed.MaybeNone:
        if not isinstance(val, str):
            throw(
                TypeError,
                "Value type for attribute %s must be str. Got: %r"
                % (self.attr, type(val)),
            )
        if self.autostrip:
            val = val.strip()
        max_len = self.max_len
        val_len = len(val)
        if max_len and val_len > max_len:
            throw(
                ValueError,
                "Value for attribute %s is too long. Max length is %d, value length is %d"
                % (self.attr, max_len, val_len),
            )
        return val

    def sql_type(self) -> str:
        if self.max_len:
            return "VARCHAR(%d)" % self.max_len
        return "TEXT"


class IntConverter(Converter):
    signed_types: dict[int | None, str] = {
        None: "INTEGER",
        8: "TINYINT",
        16: "SMALLINT",
        24: "MEDIUMINT",
        32: "INTEGER",
        64: "BIGINT",
    }
    unsigned_types: dict[int | None, str] | None = None
    py_type: type[int]

    def init(self, kwargs: dict[str, Any]) -> None:
        Converter.init(self, kwargs)
        attr = self.attr
        assert attr is not None

        min_val = kwargs.pop("min", None)
        if min_val is not None and not isinstance(min_val, int_types):
            throw(
                TypeError,
                "'min' argument for attribute %s must be int. Got: %r"
                % (attr, min_val),
            )

        max_val = kwargs.pop("max", None)
        if max_val is not None and not isinstance(max_val, int_types):
            throw(
                TypeError,
                "'max' argument for attribute %s must be int. Got: %r"
                % (attr, max_val),
            )

        size = kwargs.pop("size", None)
        if size is None:
            if attr.py_type.__name__ == "long":
                deprecated(
                    9,
                    "Attribute %s: 'long' attribute type is deprecated. "
                    "Please use 'int' type with size=64 option instead" % attr,
                )
                attr.py_type = int
                size = 64
        elif attr.py_type.__name__ == "long":
            throw(
                TypeError,
                "Attribute %s: 'size' option cannot be used with long type. Please use int type instead"
                % attr,
            )
        elif not isinstance(size, int_types):
            throw(
                TypeError,
                "'size' option for attribute %s must be of int type. Got: %r"
                % (attr, size),
            )
        elif size not in (8, 16, 24, 32, 64):
            throw(
                TypeError,
                "incorrect value of 'size' option for attribute %s. "
                "Should be 8, 16, 24, 32 or 64. Got: %d" % (attr, size),
            )

        unsigned = kwargs.pop("unsigned", False)
        if unsigned is not None and not isinstance(unsigned, bool):
            throw(
                TypeError,
                "'unsigned' option for attribute %s must be of bool type. Got: %r"
                % (attr, unsigned),
            )

        if size == 64 and unsigned and not self.provider.uint64_support:
            throw(
                TypeError,
                "Attribute %s: %s provider does not support unsigned bigint type"
                % (attr, self.provider.dialect),
            )

        if unsigned is not None and size is None:
            size = 32
        lowest = highest = None
        if size:
            highest = highest = 2**size - 1 if unsigned else 2 ** (size - 1) - 1
            lowest = 0 if unsigned else -(2 ** (size - 1))

        if highest is not None and max_val is not None and max_val > highest:
            throw(
                ValueError,
                "'max' argument should be less or equal to %d because of size=%d and unsigned=%s. "
                "Got: %d" % (highest, size, max_val, unsigned),
            )

        if lowest is not None and min_val is not None and min_val < lowest:
            throw(
                ValueError,
                "'min' argument should be greater or equal to %d because of size=%d and unsigned=%s. "
                "Got: %d" % (lowest, size, min_val, unsigned),
            )

        self.min_val = min_val or lowest
        self.max_val = max_val or highest
        self.size = size
        self.unsigned = unsigned

    def validate(
        self, val: int | SupportsIndex | str, obj: Entity | None = None
    ) -> int:
        if isinstance(val, int_types):
            pass
        elif hasattr(val, "__index__"):
            val = cast("SupportsIndex", val).__index__()
        elif isinstance(val, str):
            try:
                val = int(val)
            except ValueError:
                throw(
                    ValueError,
                    "Value type for attribute %s must be int. Got string %r"
                    % (self.attr, val),
                )
        else:
            throw(
                TypeError,
                "Value type for attribute %s must be int. Got: %r"
                % (self.attr, type(val)),
            )

        if self.min_val is not None and val < self.min_val:
            throw(
                ValueError,
                "Value %r of attr %s is less than the minimum allowed value %r"
                % (val, self.attr, self.min_val),
            )
        if self.max_val is not None and val > self.max_val:
            throw(
                ValueError,
                "Value %r of attr %s is greater than the maximum allowed value %r"
                % (val, self.attr, self.max_val),
            )
        return val

    def sql2py(self, val: int | str | SupportsInt | SupportsIndex) -> int:
        return int(val)

    def sql_type(self) -> str:
        if not self.unsigned:
            return cast("str", self.signed_types.get(self.size))
        if self.unsigned_types is None:
            return cast("str", self.signed_types.get(self.size)) + " UNSIGNED"
        return cast("str", self.unsigned_types.get(self.size))


class RealConverter(Converter):
    py_type: type[float]

    EQ = "FLOAT_EQ"
    NE = "FLOAT_NE"
    # The tolerance is necessary for Oracle, because it has different representation of float numbers.
    # For other databases the default tolerance is set because the precision can be lost during
    # Python -> JavaScript -> Python conversion
    default_tolerance: ClassVar[float] = 1e-14
    optimistic = False

    def init(self, kwargs: dict[str, Any]) -> None:
        Converter.init(self, kwargs)
        min_val = kwargs.pop("min", None)
        if min_val is not None:
            try:
                min_val = float(min_val)
            except ValueError:
                throw(
                    TypeError,
                    "Invalid value for 'min' argument for attribute %s: %r"
                    % (self.attr, min_val),
                )
        max_val = kwargs.pop("max", None)
        if max_val is not None:
            try:
                max_val = float(max_val)
            except ValueError:
                throw(
                    TypeError,
                    "Invalid value for 'max' argument for attribute %s: %r"
                    % (self.attr, max_val),
                )
        self.min_val = min_val
        self.max_val = max_val
        self.tolerance = kwargs.pop("tolerance", self.default_tolerance)

    def validate(
        self,
        val: float | str | SupportsFloat | SupportsIndex,
        obj: Entity | None = None,
    ) -> float:
        try:
            val = float(val)
        except ValueError:
            throw(TypeError, "Invalid value for attribute %s: %r" % (self.attr, val))
        if self.min_val and val < self.min_val:
            throw(
                ValueError,
                "Value %r of attr %s is less than the minimum allowed value %r"
                % (val, self.attr, self.min_val),
            )
        if self.max_val and val > self.max_val:
            throw(
                ValueError,
                "Value %r of attr %s is greater than the maximum allowed value %r"
                % (val, self.attr, self.max_val),
            )
        return val

    def dbvals_equal(self, x, y):
        tolerance = self.tolerance
        if tolerance is None or x is None or y is None:
            return x == y
        denominator = max(abs(x), abs(y))
        if not denominator:
            return True
        diff = abs(x - y) / denominator
        return diff <= tolerance

    def sql2py(self, val: float | str | SupportsFloat | SupportsIndex) -> float:
        return float(val)

    def sql_type(self) -> str:
        return "REAL"


class DecimalConverter(Converter):
    py_type: type[Decimal]

    def __init__(
        self,
        provider: DBAPIProvider,
        py_type: type[Decimal],
        attr: Attribute | None = None,
    ) -> None:
        self.exp: Decimal | None = None  # for the case when attr is None
        Converter.__init__(self, provider, py_type, attr)

    def init(self, kwargs: dict[str, Any]) -> None:
        attr = self.attr
        assert attr is not None
        args = attr.args
        if len(args) > 2:
            throw(
                TypeError,
                "Too many positional parameters for Decimal "
                "(expected: precision and scale), got: %s" % args,
            )
        if args:
            precision = args[0]
        else:
            precision = kwargs.pop("precision", 12)
        if not isinstance(precision, int_types):
            throw(
                TypeError,
                "'precision' positional argument for attribute %s must be int. Got: %r"
                % (attr, precision),
            )
        if precision <= 0:
            throw(
                TypeError,
                "'precision' positional argument for attribute %s must be positive. Got: %r"
                % (attr, precision),
            )

        if len(args) == 2:
            scale = args[1]
        else:
            scale = kwargs.pop("scale", 2)
        if not isinstance(scale, int_types):
            throw(
                TypeError,
                "'scale' positional argument for attribute %s must be int. Got: %r"
                % (attr, scale),
            )
        if scale <= 0:
            throw(
                TypeError,
                "'scale' positional argument for attribute %s must be positive. Got: %r"
                % (attr, scale),
            )

        if scale > precision:
            throw(ValueError, "'scale' must be less or equal 'precision'")
        self.precision = precision
        self.scale = scale
        self.exp = Decimal(10) ** -scale

        min_val = kwargs.pop("min", None)
        if min_val is not None:
            try:
                min_val = Decimal(min_val)
            except TypeError:
                throw(
                    TypeError,
                    "Invalid value for 'min' argument for attribute %s: %r"
                    % (attr, min_val),
                )

        max_val = kwargs.pop("max", None)
        if max_val is not None:
            try:
                max_val = Decimal(max_val)
            except TypeError:
                throw(
                    TypeError,
                    "Invalid value for 'max' argument for attribute %s: %r"
                    % (attr, max_val),
                )

        self.min_val = min_val
        self.max_val = max_val

    def validate(
        self,
        val: Decimal | float | str,
        obj: Entity | None = None,
    ) -> Decimal:
        if isinstance(val, float):
            s = str(val)
            if float(s) != val:
                s = repr(val)
            val = Decimal(s)
        try:
            val = Decimal(val)
        except InvalidOperation:
            throw(TypeError, "Invalid value for attribute %s: %r" % (self.attr, val))
        if self.min_val is not None and val < self.min_val:
            throw(
                ValueError,
                "Value %r of attr %s is less than the minimum allowed value %r"
                % (val, self.attr, self.min_val),
            )
        if self.max_val is not None and val > self.max_val:
            throw(
                ValueError,
                "Value %r of attr %s is greater than the maximum allowed value %r"
                % (val, self.attr, self.max_val),
            )
        return val

    def sql2py(self, val: Decimal | float | str) -> Decimal:
        return Decimal(val)

    def sql_type(self) -> str:
        return "DECIMAL(%d, %d)" % (self.precision, self.scale)


class BlobConverter(Converter):
    py_type: type[bytes]

    def validate(self, val: bytes | str, obj: Entity | None = None) -> bytes:
        if isinstance(val, buffer):
            return val
        if isinstance(val, str):
            return buffer(val)  # type: ignore[arg-type]
        throw(
            TypeError,
            "Attribute %r: expected type is 'buffer'. Got: %r" % (self.attr, type(val)),
        )

    def sql2py(self, val: bytes | str | Any) -> bytes:
        if not isinstance(val, buffer):
            try:
                val = buffer(val)  # type: ignore[arg-type]
            except BaseException:
                pass
        return cast("bytes", val)

    def sql_type(self) -> str:
        return "BLOB"


class DateConverter(Converter):
    py_type: type[date]

    def validate(self, val: date | str, obj: Entity | None = None) -> date:
        if isinstance(val, datetime):
            return val.date()
        if isinstance(val, date):
            return val
        if isinstance(val, str):
            return str2date(val)
        throw(
            TypeError,
            "Attribute %r: expected type is 'date'. Got: %r" % (self.attr, val),
        )

    def sql2py(self, val: date | str) -> date:
        if not isinstance(val, date):
            throw(
                ValueError,
                "Value of unexpected type received from database: instead of date got %s"
                % type(val),
            )
        return val

    def sql_type(self) -> str:
        return "DATE"


class ConverterWithMicroseconds(Converter):
    precision: int | None
    sql_type_name: LiteralString

    def __init__(
        self,
        provider: DBAPIProvider,
        py_type: type[datetime | time | timedelta],
        attr: Attribute | None = None,
    ) -> None:
        self.precision = None  # for the case when attr is None
        Converter.__init__(self, provider, py_type, attr)

    def init(self, kwargs: dict[str, Any]) -> None:
        attr = self.attr
        assert attr is not None
        args = attr.args
        if len(args) > 1:
            throw(
                TypeError,
                "Too many positional parameters for attribute %s. "
                "Expected: precision, got: %r" % (attr, args),
            )
        provider = attr.entity._database_.provider
        if args:
            precision = args[0]
            if "precision" in kwargs:
                throw(
                    TypeError,
                    "Precision for attribute %s has both positional and keyword value"
                    % attr,
                )
        else:
            precision = kwargs.pop("precision", provider.default_time_precision)
        if not isinstance(precision, int) or not 0 <= precision <= 6:
            throw(
                ValueError,
                "Precision value of attribute %s must be between 0 and 6. Got: %r"
                % (attr, precision),
            )
        if precision > provider.max_time_precision:
            throw(
                ValueError,
                "Precision value (%d) of attribute %s exceeds max datetime precision (%d) of %s %s"
                % (
                    precision,
                    attr,
                    provider.max_time_precision,
                    provider.dialect,
                    provider.server_version,
                ),
            )
        self.precision = precision

    def round_microseconds_to_precision(
        self, microseconds: int, precision: int
    ) -> int | None:
        # returns None if no change is required
        if not precision:
            result = 0
        elif precision < 6:
            rounding = 10 ** (6 - precision)
            result = (microseconds // rounding) * rounding
        else:
            return None
        return result if result != microseconds else None

    def sql_type(self) -> str:
        attr = self.attr
        precision = self.precision
        if (
            not attr
            or precision == attr.entity._database_.provider.default_time_precision
        ):
            return self.sql_type_name
        if TYPE_CHECKING:
            assert precision is not None
        return self.sql_type_name + "(%d)" % precision


class TimeConverter(ConverterWithMicroseconds):
    py_type: type[time]

    sql_type_name: LiteralString = "TIME"

    def validate(self, val: time | str, obj: Entity | None = None) -> time:
        if isinstance(val, time):
            pass
        elif isinstance(val, str):
            val = str2time(val)
        else:
            throw(
                TypeError,
                "Attribute %r: expected type is 'time'. Got: %r" % (self.attr, val),
            )
        mcs = self.round_microseconds_to_precision(val.microsecond, self.precision)  # type: ignore[arg-type]
        if mcs is not None:
            val = val.replace(microsecond=mcs)
        return val

    def sql2py(self, val: time | str) -> time:
        if not isinstance(val, time):
            throw(
                ValueError,
                "Value of unexpected type received from database: instead of time got %s"
                % type(val),
            )
        return val


class TimedeltaConverter(ConverterWithMicroseconds):
    py_type: type[timedelta]

    sql_type_name: LiteralString = "INTERVAL"

    def validate(self, val: timedelta | str, obj: Entity | None = None) -> timedelta:
        if isinstance(val, timedelta):
            pass
        elif isinstance(val, str):
            val = str2timedelta(val)
        else:
            throw(
                TypeError,
                "Attribute %r: expected type is 'timedelta'. Got: %r"
                % (self.attr, val),
            )
        mcs = self.round_microseconds_to_precision(val.microseconds, self.precision)  # type: ignore[arg-type]
        if mcs is not None:
            val = timedelta(val.days, val.seconds, mcs)
        return val

    def sql2py(self, val: timedelta | str) -> timedelta:
        if not isinstance(val, timedelta):
            throw(
                ValueError,
                "Value of unexpected type received from database: instead of time got %s"
                % type(val),
            )
        return val


class DatetimeConverter(ConverterWithMicroseconds):
    py_type: type[datetime]

    sql_type_name: LiteralString = "DATETIME"

    def validate(self, val: datetime | str, obj: Entity | None = None) -> datetime:
        if isinstance(val, datetime):
            pass
        elif isinstance(val, str):
            val = str2datetime(val)
        else:
            throw(
                TypeError,
                "Attribute %r: expected type is 'datetime'. Got: %r" % (self.attr, val),
            )
        mcs = self.round_microseconds_to_precision(val.microsecond, self.precision)  # type: ignore[arg-type]
        if mcs is not None:
            val = val.replace(microsecond=mcs)
        return val

    def sql2py(self, val: datetime | str) -> datetime:
        if not isinstance(val, datetime):
            throw(
                ValueError,
                "Value of unexpected type received from database: instead of datetime got %s"
                % type(val),
            )
        return val


class UuidConverter(Converter):
    py_type: type[UUID]

    def __init__(self, provider, py_type, attr=None):
        if attr is not None and attr.auto:
            attr.auto = False
            if not attr.default:
                attr.default = uuid4
        Converter.__init__(self, provider, py_type, attr)

    def validate(
        self, val: UUID | bytes | str | int, obj: Entity | None = None
    ) -> UUID:
        if isinstance(val, UUID):
            return val
        if isinstance(val, buffer):
            return UUID(bytes=val)
        if isinstance(val, str):
            if len(val) == 16:
                return UUID(bytes=val)  # type: ignore[arg-type]
            return UUID(hex=val)
        if isinstance(val, int):
            return UUID(int=val)
        if self.attr is not None:
            throw(
                ValueError,
                "Value type of attribute %s must be UUID. Got: %r"
                % (self.attr, type(val)),
            )
        else:
            throw(ValueError, "Expected UUID value, got: %r" % type(val))

    def py2sql(self, val):
        return buffer(val.bytes)

    sql2py = validate

    def sql_type(self) -> str:
        return "UUID"


class JsonConverter(Converter):
    py_type: type[Json]

    json_kwargs: ClassVar[dict[str, Any]] = {}

    class JsonEncoder(json.JSONEncoder):
        def default(self, o: Any) -> dict[str, str] | list[str]:
            if isinstance(o, Json):
                return o.wrapped
            return json.JSONEncoder.default(self, o)

    def validate(self, val: Any, obj: Entity | None = None) -> TrackedValue:
        if obj is None or self.attr is None:
            return val
        if (
            isinstance(val, TrackedValue)
            and val.obj_ref is not None
            and val.obj_ref() is obj
            and val.attr is self.attr
        ):
            return val
        return TrackedValue.make(obj, self.attr, val)

    def val2dbval(self, val: Any, obj: Entity | None = None) -> str:
        return json.dumps(val, cls=self.JsonEncoder, **self.json_kwargs)

    def dbval2val(self, dbval: OptAttrValue | Entity, obj: Entity | None = None) -> Any:
        if isinstance(dbval, (int, bool, float, type(None))):
            return dbval
        val = json.loads(cast("str | bytes", dbval))
        if obj is None:
            return val
        return TrackedValue.make(obj, self.attr, val)

    def dbvals_equal(self, x, y):
        if x == y:
            return True  # optimization
        if isinstance(x, str):
            x = json.loads(x)
        if isinstance(y, str):
            y = json.loads(y)
        return x == y

    def sql_type(self) -> str:
        return "JSON"


class ArrayConverter(Converter):
    array_types: ClassVar[dict[type[str | float], tuple[str, type[Converter]]]] = {
        int: ("int", IntConverter),
        str: ("text", StrConverter),
        float: ("real", RealConverter),
    }
    py_type: type[Array]

    def __init__(
        self,
        provider: DBAPIProvider,
        py_type: type[Array],
        attr: Attribute | None = None,
    ) -> None:
        Converter.__init__(self, provider, py_type, attr)
        self.item_converter = self.array_types[self.py_type.item_type][1]

    def validate(self, val: object, obj: Entity | None = None) -> TrackedValue | list:
        if (
            isinstance(val, TrackedValue)
            and val.obj_ref is not None
            and val.obj_ref() is obj
            and val.attr is self.attr
        ):
            return val

        if isinstance(val, str) or not hasattr(val, "__len__"):
            items = [val]
        else:
            items = list(val)  # type: ignore[call-overload]
        item_type = self.py_type.item_type
        if item_type == float:
            item_type = (float, int)
        for i, v in enumerate(items):
            if not isinstance(v, item_type):
                if hasattr(v, "__index__"):
                    items[i] = cast("SupportsIndex", v).__index__()
                else:
                    throw(
                        TypeError,
                        "Cannot store %s item in array of %s"
                        % (type(v).__name__, self.py_type.item_type.__name__),
                    )

        if obj is None or self.attr is None:
            return items
        return TrackedArray(obj, self.attr, items)

    def dbval2val(self, dbval: OptAttrValue | Entity, obj: Entity | None = None) -> Any:
        if obj is None or dbval is None:
            return dbval
        return TrackedArray(obj, self.attr, dbval)  # type: ignore[arg-type]

    def val2dbval(
        self, val: TrackedValue | OptAttrValue, obj: Entity | None = None
    ) -> str | list[OptAttrValue] | None:
        assert self.attr is not None
        if self.attr.nullable and val is None:
            return val
        return list(val)  # type: ignore[arg-type]

    def sql_type(self) -> str:
        return "%s[]" % self.array_types[self.py_type.item_type][0]
