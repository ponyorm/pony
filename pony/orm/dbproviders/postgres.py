from datetime import date, datetime, time, timedelta
from decimal import Decimal
from uuid import UUID

from pony.py23compat import buffer, int_types

_psycopg_import_error = None
try:
    import psycopg2
except ImportError as exc:
    try:
        from psycopg2cffi import compat
    except ImportError:
        psycopg2 = None
        _psycopg_import_error = exc
    else:
        compat.register()
        import psycopg2


if psycopg2 is not None:
    import psycopg2.extras

    psycopg2.extras.register_uuid()
    psycopg2.extras.register_default_json(loads=lambda x: x)
    psycopg2.extras.register_default_jsonb(loads=lambda x: x)

from pony.orm import core, dbapiprovider, dbschema, ormtypes
from pony.orm.core import log_orm
from pony.orm.dbapiprovider import DBAPIProvider, Pool, wrap_dbapi_exceptions
from pony.orm.sqlbuilding import SQLBuilder, Value, join
from pony.orm.sqltranslation import SQLTranslator
from pony.utils import is_ident

NoneType = type(None)


class PGColumn(dbschema.Column):
    auto_template = "SERIAL PRIMARY KEY"


class PGSchema(dbschema.DBSchema):
    dialect = "PostgreSQL"
    column_class = PGColumn


class PGTranslator(SQLTranslator):
    dialect = "PostgreSQL"


class PGValue(Value):
    __slots__ = []

    def __str__(self):
        value = self.value
        if isinstance(value, bool):
            return (value and "true") or "false"
        return Value.__str__(self)


class PGSQLBuilder(SQLBuilder):
    dialect = "PostgreSQL"
    value_class = PGValue

    def INSERT(self, table_name, columns, values, returning=None):
        if not values:
            result = ["INSERT INTO ", self.quote_name(table_name), " DEFAULT VALUES"]
        else:
            result = SQLBuilder.INSERT(self, table_name, columns, values)
        if returning is not None:
            result.extend([" RETURNING ", self.quote_name(returning)])
        return result

    def TO_INT(self, expr):
        return "(", self(expr), ")::int"

    def TO_STR(self, expr):
        return "(", self(expr), ")::text"

    def TO_REAL(self, expr):
        return "(", self(expr), ")::double precision"

    def DATE(self, expr):
        return "(", self(expr), ")::date"

    def RANDOM(self):
        return "random()"

    def DATE_ADD(self, expr, delta):
        return "(", self(expr), " + ", self(delta), ")"

    def DATE_SUB(self, expr, delta):
        return "(", self(expr), " - ", self(delta), ")"

    def DATE_DIFF(self, expr1, expr2):
        return "((", self(expr1), " - ", self(expr2), ") * interval '1 day')"

    def DATETIME_ADD(self, expr, delta):
        return "(", self(expr), " + ", self(delta), ")"

    def DATETIME_SUB(self, expr, delta):
        return "(", self(expr), " - ", self(delta), ")"

    def DATETIME_DIFF(self, expr1, expr2):
        return self(expr1), " - ", self(expr2)

    def eval_json_path(self, values):
        result = []
        for value in values:
            if isinstance(value, int):
                result.append(str(value))
            elif isinstance(value, str):
                result.append(
                    value if is_ident(value) else '"%s"' % value.replace('"', '\\"')
                )
            else:
                assert False, value
        return "{%s}" % ",".join(result)

    def JSON_QUERY(self, expr, path):
        path_sql, has_params, has_wildcards = self.build_json_path(path)
        return "(", self(expr), " #> ", path_sql, ")"

    json_value_type_mapping = {bool: "boolean", int: "int", float: "double precision"}

    def JSON_VALUE(self, expr, path, type):
        if type is ormtypes.Json:
            return self.JSON_QUERY(expr, path)
        path_sql, has_params, has_wildcards = self.build_json_path(path)
        sql = "(", self(expr), " #>> ", path_sql, ")"
        type_name = self.json_value_type_mapping.get(type, "text")
        return sql if type_name == "text" else (sql, "::", type_name)

    def JSON_NONZERO(self, expr):
        return (
            "coalesce(",
            self(expr),
            ", 'null'::jsonb) NOT IN ("
            "'null'::jsonb, 'false'::jsonb, '0'::jsonb, '\"\"'::jsonb, '[]'::jsonb, '{}'::jsonb)",
        )

    def JSON_CONCAT(self, left, right):
        return "(", self(left), "||", self(right), ")"

    def JSON_CONTAINS(self, expr, path, key):
        return (
            (self.JSON_QUERY(expr, path) if path else self(expr)),
            " ? ",
            self(key),
        )

    def JSON_ARRAY_LENGTH(self, value):
        return "jsonb_array_length(", self(value), ")"

    def GROUP_CONCAT(self, distinct, expr, sep=None):
        assert distinct in (None, True, False)
        result = (
            (distinct and "string_agg(distinct ") or "string_agg(",
            self(expr),
            "::text",
        )
        if sep is not None:
            result = result, ", ", self(sep)
        else:
            result = result, ", ','"
        return result, ")"

    def ARRAY_INDEX(self, col, index):
        return self(col), "[", self(index), "]"

    def ARRAY_CONTAINS(self, key, not_in, col):
        if not_in:
            return self(key), " <> ALL(", self(col), ")"
        return self(key), " = ANY(", self(col), ")"

    def ARRAY_SUBSET(self, array1, not_in, array2):
        result = self(array1), " <@ ", self(array2)
        if not_in:
            result = "NOT (", result, ")"
        return result

    def ARRAY_LENGTH(self, array):
        return "COALESCE(ARRAY_LENGTH(", self(array), ", 1), 0)"

    def ARRAY_SLICE(self, array, start, stop):
        return (
            self(array),
            "[",
            self(start) if start else "",
            ":",
            self(stop) if stop else "",
            "]",
        )

    def MAKE_ARRAY(self, *items):
        return "ARRAY[", join(", ", (self(item) for item in items)), "]"


class PGIntConverter(dbapiprovider.IntConverter):
    signed_types = {
        None: "INTEGER",
        8: "SMALLINT",
        16: "SMALLINT",
        24: "INTEGER",
        32: "INTEGER",
        64: "BIGINT",
    }
    unsigned_types = {
        None: "INTEGER",
        8: "SMALLINT",
        16: "INTEGER",
        24: "INTEGER",
        32: "BIGINT",
    }


class PGRealConverter(dbapiprovider.RealConverter):
    def sql_type(self):
        return "DOUBLE PRECISION"


class PGBlobConverter(dbapiprovider.BlobConverter):
    def sql_type(self):
        return "BYTEA"


class PGTimedeltaConverter(dbapiprovider.TimedeltaConverter):
    sql_type_name = "INTERVAL DAY TO SECOND"


class PGDatetimeConverter(dbapiprovider.DatetimeConverter):
    sql_type_name = "TIMESTAMP"


class PGUuidConverter(dbapiprovider.UuidConverter):
    def py2sql(self, val):
        return val


class PGJsonConverter(dbapiprovider.JsonConverter):
    def sql_type(self):
        return "JSONB"


class PGArrayConverter(dbapiprovider.ArrayConverter):
    array_types = {
        int: ("int", PGIntConverter),
        str: ("text", dbapiprovider.StrConverter),
        float: ("double precision", PGRealConverter),
    }


class PGPool(Pool):
    def _connect(self):
        self.con = self.dbapi_module.connect(*self.args, **self.kwargs)
        if "client_encoding" not in self.kwargs:
            self.con.set_client_encoding("UTF8")

    def release(self, con):
        assert con is self.con
        try:
            con.rollback()
            con.autocommit = True
            cursor = con.cursor()
            cursor.execute("DISCARD ALL")
            con.autocommit = False
        except:
            self.drop(con)
            raise


ADMIN_SHUTDOWN = "57P01"


class PGProvider(DBAPIProvider):
    dialect = "PostgreSQL"
    paramstyle = "pyformat"
    max_name_len = 63
    max_params_count = 10000
    index_if_not_exists_syntax = False

    dbapi_module = psycopg2
    dbschema_cls = PGSchema
    translator_cls = PGTranslator
    sqlbuilder_cls = PGSQLBuilder
    array_converter_cls = PGArrayConverter

    default_schema_name = "public"

    fk_types = {"SERIAL": "INTEGER", "BIGSERIAL": "BIGINT"}

    def normalize_name(self, name):
        return name[: self.max_name_len].lower()

    @wrap_dbapi_exceptions
    def inspect_connection(self, connection):
        self.server_version = connection.server_version
        self.table_if_not_exists_syntax = self.server_version >= 90100

    def should_reconnect(self, exc):
        return isinstance(exc, psycopg2.OperationalError) and exc.pgcode in (
            None,
            ADMIN_SHUTDOWN,
        )

    def get_pool(self, *args, **kwargs):
        if psycopg2 is None:
            raise ImportError(
                "In order to use PonyORM with PostgreSQL please install "
                "psycopg2 or psycopg2cffi"
            ) from _psycopg_import_error
        return PGPool(self.dbapi_module, *args, **kwargs)

    @wrap_dbapi_exceptions
    def set_transaction_mode(self, connection, cache):
        assert not cache.in_transaction
        if cache.immediate and connection.autocommit:
            connection.autocommit = False
            if core.local.debug:
                log_orm("SWITCH FROM AUTOCOMMIT TO TRANSACTION MODE")
        db_session = cache.db_session
        if db_session is not None and db_session.serializable:
            cursor = connection.cursor()
            sql = "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"
            if core.local.debug:
                log_orm(sql)
            cursor.execute(sql)
        elif not cache.immediate and not connection.autocommit:
            connection.autocommit = True
            if core.local.debug:
                log_orm("SWITCH TO AUTOCOMMIT MODE")
        if db_session is not None and (db_session.serializable or db_session.ddl):
            cache.in_transaction = True

    @wrap_dbapi_exceptions
    def execute(self, cursor, sql, arguments=None, returning_id=False):
        if type(arguments) is list:
            assert arguments and not returning_id
            cursor.executemany(sql, arguments)
        else:
            if arguments is None:
                cursor.execute(sql)
            else:
                cursor.execute(sql, arguments)
            if returning_id:
                return cursor.fetchone()[0]

    def table_exists(self, connection, table_name, case_sensitive=True):
        schema_name, table_name = self.split_table_name(table_name)
        cursor = connection.cursor()
        if case_sensitive:
            sql = (
                "SELECT tablename FROM pg_catalog.pg_tables "
                "WHERE schemaname = %s AND tablename = %s"
            )
        else:
            sql = (
                "SELECT tablename FROM pg_catalog.pg_tables "
                "WHERE schemaname = %s AND lower(tablename) = lower(%s)"
            )
        cursor.execute(sql, (schema_name, table_name))
        row = cursor.fetchone()
        return row[0] if row is not None else None

    def index_exists(self, connection, table_name, index_name, case_sensitive=True):
        schema_name, table_name = self.split_table_name(table_name)
        cursor = connection.cursor()
        if case_sensitive:
            sql = (
                "SELECT indexname FROM pg_catalog.pg_indexes "
                "WHERE schemaname = %s AND tablename = %s AND indexname = %s"
            )
        else:
            sql = (
                "SELECT indexname FROM pg_catalog.pg_indexes "
                "WHERE schemaname = %s AND tablename = %s AND lower(indexname) = lower(%s)"
            )
        cursor.execute(sql, [schema_name, table_name, index_name])
        row = cursor.fetchone()
        return row[0] if row is not None else None

    def fk_exists(self, connection, table_name, fk_name, case_sensitive=True):
        schema_name, table_name = self.split_table_name(table_name)
        if case_sensitive:
            sql = (
                "SELECT con.conname FROM pg_class cls "
                "JOIN pg_namespace ns ON cls.relnamespace = ns.oid "
                "JOIN pg_constraint con ON con.conrelid = cls.oid "
                "WHERE ns.nspname = %s AND cls.relname = %s "
                "AND con.contype = 'f' AND con.conname = %s"
            )
        else:
            sql = (
                "SELECT con.conname FROM pg_class cls "
                "JOIN pg_namespace ns ON cls.relnamespace = ns.oid "
                "JOIN pg_constraint con ON con.conrelid = cls.oid "
                "WHERE ns.nspname = %s AND cls.relname = %s "
                "AND con.contype = 'f' AND lower(con.conname) = lower(%s)"
            )
        cursor = connection.cursor()
        cursor.execute(sql, [schema_name, table_name, fk_name])
        row = cursor.fetchone()
        return row[0] if row is not None else None

    def drop_table(self, connection, table_name):
        cursor = connection.cursor()
        sql = "DROP TABLE %s CASCADE" % self.quote_name(table_name)
        cursor.execute(sql)

    converter_classes = [
        (NoneType, dbapiprovider.NoneConverter),
        (bool, dbapiprovider.BoolConverter),
        (str, dbapiprovider.StrConverter),
        (int_types, PGIntConverter),
        (float, PGRealConverter),
        (Decimal, dbapiprovider.DecimalConverter),
        (datetime, PGDatetimeConverter),
        (date, dbapiprovider.DateConverter),
        (time, dbapiprovider.TimeConverter),
        (timedelta, PGTimedeltaConverter),
        (UUID, PGUuidConverter),
        (buffer, PGBlobConverter),
        (ormtypes.Json, PGJsonConverter),
    ]


provider_cls = PGProvider
