import os

from pony.py23compat import buffer, int_types

os.environ["NLS_LANG"] = "AMERICAN_AMERICA.UTF8"

import re
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from uuid import UUID

try:
    import cx_Oracle
except ImportError:
    import oracledb as cx_Oracle

from pony.orm import core, dbapiprovider, sqltranslation
from pony.orm.core import TranslationError, log_orm
from pony.orm.dbapiprovider import (
    DBAPIProvider,
    get_version_tuple,
    wrap_dbapi_exceptions,
)
from pony.orm.dbschema import Column, DBObject, DBSchema, Table
from pony.orm.ormtypes import Json
from pony.orm.sqlbuilding import SQLBuilder
from pony.utils import throw

NoneType = type(None)


class OraTable(Table):
    def get_objects_to_create(self, created_tables=None):
        result = Table.get_objects_to_create(self, created_tables)
        for column in self.column_list:
            if column.is_pk == "auto":
                sequence_name = column.converter.attr.kwargs.get("sequence_name")
                sequence = OraSequence(self, sequence_name)
                trigger = OraTrigger(self, column, sequence)
                result.extend((sequence, trigger))
                break
        return result


class OraSequence(DBObject):
    typename = "Sequence"

    def __init__(self, table, name=None):
        self.table = table
        table_name = table.name
        if name is not None:
            self.name = name
        elif isinstance(table_name, str):
            self.name = table_name + "_SEQ"
        else:
            self.name = tuple(table_name[:-1]) + (table_name[0] + "_SEQ",)

    def exists(self, provider, connection, case_sensitive=True):
        if case_sensitive:
            sql = (
                "SELECT sequence_name FROM all_sequences "
                "WHERE sequence_owner = :so and sequence_name = :sn"
            )
        else:
            sql = (
                "SELECT sequence_name FROM all_sequences "
                "WHERE sequence_owner = :so and upper(sequence_name) = upper(:sn)"
            )
        owner_name, sequence_name = provider.split_table_name(self.name)
        cursor = connection.cursor()
        cursor.execute(sql, dict(so=owner_name, sn=sequence_name))
        row = cursor.fetchone()
        return row[0] if row is not None else None

    def get_create_command(self):
        schema = self.table.schema
        seq_name = schema.provider.quote_name(self.name)
        return schema.case("CREATE SEQUENCE %s NOCACHE") % seq_name


trigger_template = """
CREATE TRIGGER %s
  BEFORE INSERT ON %s
  FOR EACH ROW
BEGIN
  IF :new.%s IS NULL THEN
    SELECT %s.nextval INTO :new.%s FROM DUAL;
  END IF;
END;""".strip()


class OraTrigger(DBObject):
    typename = "Trigger"

    def __init__(self, table, column, sequence):
        self.table = table
        self.column = column
        self.sequence = sequence
        table_name = table.name
        if not isinstance(table_name, str):
            table_name = table_name[-1]
        self.name = table_name + "_BI"  # Before Insert

    def exists(self, provider, connection, case_sensitive=True):
        if case_sensitive:
            sql = (
                "SELECT trigger_name FROM all_triggers "
                "WHERE table_name = :tbn AND table_owner = :o "
                "AND trigger_name = :trn AND owner = :o"
            )
        else:
            sql = (
                "SELECT trigger_name FROM all_triggers "
                "WHERE table_name = :tbn AND table_owner = :o "
                "AND upper(trigger_name) = upper(:trn) AND owner = :o"
            )
        owner_name, table_name = provider.split_table_name(self.table.name)
        cursor = connection.cursor()
        cursor.execute(sql, dict(tbn=table_name, trn=self.name, o=owner_name))
        row = cursor.fetchone()
        return row[0] if row is not None else None

    def get_create_command(self):
        schema = self.table.schema
        quote_name = schema.provider.quote_name
        trigger_name = quote_name(self.name)
        table_name = quote_name(self.table.name)
        column_name = quote_name(self.column.name)
        seq_name = quote_name(self.sequence.name)
        return schema.case(trigger_template) % (
            trigger_name,
            table_name,
            column_name,
            seq_name,
            column_name,
        )


class OraColumn(Column):
    auto_template = None


class OraSchema(DBSchema):
    dialect = "Oracle"
    table_class = OraTable
    column_class = OraColumn


class OraNoneMonad(sqltranslation.NoneMonad):
    def __init__(self, value=None):
        assert value in (None, "")
        sqltranslation.ConstMonad.__init__(self, None)


class OraConstMonad(sqltranslation.ConstMonad):
    @staticmethod
    def new(value):
        if value == "":
            value = None
        return sqltranslation.ConstMonad.new(value)


class OraTranslator(sqltranslation.SQLTranslator):
    dialect = "Oracle"
    rowid_support = True
    json_path_wildcard_syntax = True
    json_values_are_comparable = False
    NoneMonad = OraNoneMonad
    ConstMonad = OraConstMonad


class OraBuilder(SQLBuilder):
    dialect = "Oracle"

    def INSERT(self, table_name, columns, values, returning=None):
        result = SQLBuilder.INSERT(self, table_name, columns, values)
        if returning is not None:
            result.extend((" RETURNING ", self.quote_name(returning), " INTO :new_id"))
        return result

    def SELECT_FOR_UPDATE(self, nowait, skip_locked, *sections):
        assert not self.indent
        nowait = " NOWAIT" if nowait else ""
        skip_locked = " SKIP LOCKED" if skip_locked else ""
        last_section = sections[-1]
        if last_section[0] != "LIMIT":
            return self.SELECT(*sections), "FOR UPDATE", nowait, skip_locked, "\n"

        from_section = sections[1]
        assert from_section[0] == "FROM"
        if len(from_section) > 2:
            throw(
                NotImplementedError,
                "Table joins are not supported for Oracle queries which have both FOR UPDATE and ROWNUM",
            )

        order_by_section = None
        for section in sections:
            if section[0] == "ORDER_BY":
                order_by_section = section

        table_ast = from_section[1]
        assert len(table_ast) == 3 and table_ast[1] == "TABLE"
        table_alias = table_ast[0]
        rowid = ["COLUMN", table_alias, "ROWID"]
        sql_ast = [
            "SELECT",
            sections[0],
            ["FROM", table_ast],
            [
                "WHERE",
                [
                    "IN",
                    rowid,
                    ("SELECT", ["ROWID", ["AS", rowid, "row-id"]]) + sections[1:],
                ],
            ],
        ]
        if order_by_section:
            sql_ast.append(order_by_section)
        result = self(sql_ast)
        return result, "FOR UPDATE", nowait, skip_locked, "\n"

    def SELECT(self, *sections):
        prev_suppress_aliases = self.suppress_aliases
        self.suppress_aliases = False
        try:
            last_section = sections[-1]
            limit = offset = None
            if last_section[0] == "LIMIT":
                limit = last_section[1]
                if len(last_section) > 2:
                    offset = last_section[2]
                sections = sections[:-1]
            result = self._subquery(*sections)
            indent = self.indent_spaces * self.indent

            if sections[0][0] == "ROWID":
                indent0 = self.indent_spaces
                x = 't."row-id"'
            else:
                indent0 = ""
                x = "t.*"

            if not limit and not offset:
                pass
            elif not offset:
                result = [indent0, "SELECT * FROM (\n"]
                self.indent += 1
                result.extend(self._subquery(*sections))
                self.indent -= 1
                result.extend((indent, ") WHERE ROWNUM <= %d\n" % limit))
            else:
                indent2 = indent + self.indent_spaces
                result = [
                    indent0,
                    "SELECT %s FROM (\n" % x,
                    indent2,
                    'SELECT t.*, ROWNUM "row-num" FROM (\n',
                ]
                self.indent += 2
                result.extend(self._subquery(*sections))
                self.indent -= 2
                if limit is None:
                    result.append("%s) t\n" % indent2)
                    result.append('%s) t WHERE "row-num" > %d\n' % (indent, offset))
                else:
                    result.append(
                        "%s) t WHERE ROWNUM <= %d\n" % (indent2, limit + offset)
                    )
                    result.append('%s) t WHERE "row-num" > %d\n' % (indent, offset))
            if self.indent:
                indent = self.indent_spaces * self.indent
                return "(\n", result, indent + ")"
            return result
        finally:
            self.suppress_aliases = prev_suppress_aliases

    def ROWID(self, *expr_list):
        return self.ALL(*expr_list)

    def LIMIT(self, limit, offset=None):
        assert False  # pragma: no cover

    def TO_REAL(self, expr):
        return "CAST(", self(expr), " AS NUMBER)"

    def TO_STR(self, expr):
        return "TO_CHAR(", self(expr), ")"

    def DATE(self, expr):
        return "TRUNC(", self(expr), ")"

    def RANDOM(self):
        return "dbms_random.value"

    def MOD(self, a, b):
        return "MOD(", self(a), ", ", self(b), ")"

    def DATE_ADD(self, expr, delta):
        return "(", self(expr), " + ", self(delta), ")"

    def DATE_SUB(self, expr, delta):
        return "(", self(expr), " - ", self(delta), ")"

    def DATE_DIFF(self, expr1, expr2):
        return self(expr1), " - ", self(expr2)

    def DATETIME_ADD(self, expr, delta):
        return "(", self(expr), " + ", self(delta), ")"

    def DATETIME_SUB(self, expr, delta):
        return "(", self(expr), " - ", self(delta), ")"

    def DATETIME_DIFF(self, expr1, expr2):
        return self(expr1), " - ", self(expr2)

    def build_json_path(self, path):
        path_sql, has_params, has_wildcards = SQLBuilder.build_json_path(self, path)
        if has_params:
            throw(TranslationError, "Oracle doesn't allow parameters in JSON paths")
        return path_sql, has_params, has_wildcards

    def JSON_QUERY(self, expr, path):
        expr_sql = self(expr)
        path_sql, has_params, has_wildcards = self.build_json_path(path)
        if has_wildcards:
            return "JSON_QUERY(", expr_sql, ", ", path_sql, " WITH WRAPPER)"
        return (
            "REGEXP_REPLACE(JSON_QUERY(",
            expr_sql,
            ", ",
            path_sql,
            " WITH WRAPPER), '(^\\[|\\]$)', '')",
        )

    json_value_type_mapping = {bool: "NUMBER", int: "NUMBER", float: "NUMBER"}

    def JSON_VALUE(self, expr, path, type):
        if type is Json:
            return self.JSON_QUERY(expr, path)
        path_sql, has_params, has_wildcards = self.build_json_path(path)
        type_name = self.json_value_type_mapping.get(type, "VARCHAR2")
        return (
            "JSON_VALUE(",
            self(expr),
            ", ",
            path_sql,
            " RETURNING ",
            type_name,
            ")",
        )

    def JSON_NONZERO(self, expr):
        return (
            "COALESCE(",
            self(expr),
            """, 'null') NOT IN ('null', 'false', '0', '""', '[]', '{}')""",
        )

    def JSON_CONTAINS(self, expr, path, key):
        assert key[0] == "VALUE" and isinstance(key[1], str)
        path_sql, has_params, has_wildcards = self.build_json_path(path)
        path_with_key_sql, _, _ = self.build_json_path(path + [key])
        expr_sql = self(expr)
        result = "JSON_EXISTS(", expr_sql, ", ", path_with_key_sql, ")"
        if json_item_re.match(key[1]):
            item = r'"([^"]|\\")*"'
            list_start = r"\[\s*(%s\s*,\s*)*" % item
            list_end = r"\s*(,\s*%s\s*)*\]" % item
            pattern = r'%s"%s"%s' % (list_start, key[1], list_end)
            if has_wildcards:
                sublist = r"\[[^]]*\]"
                item_or_sublist = "(%s|%s)" % (item, sublist)
                wrapper_list_start = r"^\[\s*(%s\s*,\s*)*" % item_or_sublist
                wrapper_list_end = r"\s*(,\s*%s\s*)*\]$" % item_or_sublist
                pattern = r"%s%s%s" % (wrapper_list_start, pattern, wrapper_list_end)
                result += (
                    " OR REGEXP_LIKE(JSON_QUERY(",
                    expr_sql,
                    ", ",
                    path_sql,
                    " WITH WRAPPER), '%s')" % pattern,
                )
            else:
                pattern = "^%s$" % pattern
                result += (
                    " OR REGEXP_LIKE(JSON_QUERY(",
                    expr_sql,
                    ", ",
                    path_sql,
                    "), '%s')" % pattern,
                )
        return result

    def JSON_ARRAY_LENGTH(self, value):
        throw(
            TranslationError,
            "Oracle does not provide `length` function for JSON arrays",
        )

    def GROUP_CONCAT(self, distinct, expr, sep=None):
        assert distinct in (None, True, False)
        if distinct and self.provider.server_version >= (19,):
            distinct = "DISTINCT "
        else:
            distinct = ""
        result = "LISTAGG(", distinct, self(expr)
        if sep is not None:
            result = result, ", ", self(sep)
        else:
            result = result, ", ','"
        return result, ") WITHIN GROUP(ORDER BY 1)"


json_item_re = re.compile(r"[\w\s]*")


class OraBoolConverter(dbapiprovider.BoolConverter):
    def py2sql(self, val):
        # Fixes cx_Oracle 5.1.3 Python 3 bug:
        # "DatabaseError: OCI-22062: invalid input string [True]"
        return int(val)

    def sql2py(self, val):
        return bool(val)  # TODO: True/False, T/F, Y/N, Yes/No, etc.

    def sql_type(self):
        return "NUMBER(1)"


class OraStrConverter(dbapiprovider.StrConverter):
    def validate(self, val, obj=None):
        if val == "":
            return None
        return dbapiprovider.StrConverter.validate(self, val)

    def sql2py(self, val):
        if isinstance(val, cx_Oracle.LOB):
            val = val.read()
        return val

    def sql_type(self):
        # TODO: Add support for NVARCHAR2 and NCLOB datatypes
        if self.max_len:
            return "VARCHAR2(%d CHAR)" % self.max_len
        return "CLOB"


class OraIntConverter(dbapiprovider.IntConverter):
    signed_types = {
        None: "NUMBER(38)",
        8: "NUMBER(3)",
        16: "NUMBER(5)",
        24: "NUMBER(7)",
        32: "NUMBER(10)",
        64: "NUMBER(19)",
    }
    unsigned_types = {
        None: "NUMBER(38)",
        8: "NUMBER(3)",
        16: "NUMBER(5)",
        24: "NUMBER(8)",
        32: "NUMBER(10)",
        64: "NUMBER(20)",
    }

    def init(self, kwargs):
        dbapiprovider.IntConverter.init(self, kwargs)
        sequence_name = kwargs.pop("sequence_name", None)
        if sequence_name is not None and not (self.attr.auto and self.attr.is_pk):
            throw(
                TypeError,
                "Parameter 'sequence_name' can be used only for PrimaryKey attributes with auto=True",
            )


class OraRealConverter(dbapiprovider.RealConverter):
    # Note that Oracle has different representation of float numbers
    def sql_type(self):
        return "NUMBER"


class OraDecimalConverter(dbapiprovider.DecimalConverter):
    def sql_type(self):
        return "NUMBER(%d, %d)" % (self.precision, self.scale)


class OraBlobConverter(dbapiprovider.BlobConverter):
    def sql2py(self, val):
        return buffer(val.read())


class OraDateConverter(dbapiprovider.DateConverter):
    def sql2py(self, val):
        if isinstance(val, datetime):
            return val.date()
        if not isinstance(val, date):
            throw(
                ValueError,
                "Value of unexpected type received from database: instead of date got %s",
                type(val),
            )
        return val


class OraTimeConverter(dbapiprovider.TimeConverter):
    sql_type_name = "INTERVAL DAY(0) TO SECOND"

    def __init__(self, provider, py_type, attr=None):
        dbapiprovider.TimeConverter.__init__(self, provider, py_type, attr)
        if attr is not None and self.precision > 0:
            # cx_Oracle 5.1.3 corrupts microseconds for values of DAY TO SECOND type
            self.precision = 0

    def sql2py(self, val):
        if isinstance(val, timedelta):
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
        return val

    def py2sql(self, val):
        return timedelta(
            hours=val.hour,
            minutes=val.minute,
            seconds=val.second,
            microseconds=val.microsecond,
        )


class OraTimedeltaConverter(dbapiprovider.TimedeltaConverter):
    sql_type_name = "INTERVAL DAY TO SECOND"

    def __init__(self, provider, py_type, attr=None):
        dbapiprovider.TimedeltaConverter.__init__(self, provider, py_type, attr)
        if attr is not None and self.precision > 0:
            # cx_Oracle 5.1.3 corrupts microseconds for values of DAY TO SECOND type
            self.precision = 0


class OraDatetimeConverter(dbapiprovider.DatetimeConverter):
    sql_type_name = "TIMESTAMP"


class OraUuidConverter(dbapiprovider.UuidConverter):
    def sql_type(self):
        return "RAW(16)"


class OraJsonConverter(dbapiprovider.JsonConverter):
    json_kwargs = {"separators": (",", ":"), "sort_keys": True, "ensure_ascii": False}
    optimistic = False  # CLOBs cannot be compared with strings, and TO_CHAR(CLOB) returns first 4000 chars only

    def sql2py(self, dbval):
        if hasattr(dbval, "read"):
            dbval = dbval.read()
        return dbapiprovider.JsonConverter.sql2py(self, dbval)

    def sql_type(self):
        return "CLOB"


class OraProvider(DBAPIProvider):
    dialect = "Oracle"
    paramstyle = "named"
    max_name_len = 30
    table_if_not_exists_syntax = False
    index_if_not_exists_syntax = False
    varchar_default_max_len = 1000
    uint64_support = True

    dbapi_module = cx_Oracle
    dbschema_cls = OraSchema
    translator_cls = OraTranslator
    sqlbuilder_cls = OraBuilder

    name_before_table = "owner"

    converter_classes = [
        (NoneType, dbapiprovider.NoneConverter),
        (bool, OraBoolConverter),
        (str, OraStrConverter),
        (int_types, OraIntConverter),
        (float, OraRealConverter),
        (Decimal, OraDecimalConverter),
        (datetime, OraDatetimeConverter),
        (date, OraDateConverter),
        (time, OraTimeConverter),
        (timedelta, OraTimedeltaConverter),
        (UUID, OraUuidConverter),
        (buffer, OraBlobConverter),
        (Json, OraJsonConverter),
    ]

    @wrap_dbapi_exceptions
    def inspect_connection(self, connection):
        cursor = connection.cursor()
        cursor.execute(
            "SELECT version FROM product_component_version "
            "WHERE product LIKE 'Oracle Database %'"
        )
        self.server_version = get_version_tuple(cursor.fetchone()[0])
        cursor.execute("SELECT sys_context( 'userenv', 'current_schema' ) FROM DUAL")
        self.default_schema_name = cursor.fetchone()[0]

    def should_reconnect(self, exc):
        reconnect_error_codes = (
            3113,  # ORA-03113: end-of-file on communication channel
            3114,  # ORA-03114: not connected to ORACLE
        )
        return (
            isinstance(exc, cx_Oracle.OperationalError)
            and exc.args[0].code in reconnect_error_codes
        )

    def normalize_name(self, name):
        return name[: self.max_name_len].upper()

    def normalize_vars(self, vars, vartypes):
        DBAPIProvider.normalize_vars(self, vars, vartypes)
        for key, value in vars.items():
            if value == "":
                vars[key] = None
                vartypes[key] = NoneType

    @wrap_dbapi_exceptions
    def set_transaction_mode(self, connection, cache):
        assert not cache.in_transaction
        db_session = cache.db_session
        if db_session is not None and db_session.serializable:
            cursor = connection.cursor()
            sql = "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"
            if core.local.debug:
                log_orm(sql)
            cursor.execute(sql)
        cache.immediate = True
        if db_session is not None and (db_session.serializable or db_session.ddl):
            cache.in_transaction = True

    @wrap_dbapi_exceptions
    def execute(self, cursor, sql, arguments=None, returning_id=False):
        if type(arguments) is list:
            assert arguments and not returning_id
            set_input_sizes(cursor, arguments[0])
            cursor.executemany(sql, arguments)
        else:
            if arguments is not None:
                set_input_sizes(cursor, arguments)
            if returning_id:
                var = cursor.var(
                    cx_Oracle.STRING, 40, cursor.arraysize, outconverter=int
                )
                arguments["new_id"] = var
                if arguments is None:
                    cursor.execute(sql)
                else:
                    cursor.execute(sql, arguments)
                value = var.getvalue()
                if isinstance(value, list):
                    assert len(value) == 1
                    value = value[0]
                return value
            if arguments is None:
                cursor.execute(sql)
            else:
                cursor.execute(sql, arguments)

    def get_pool(self, *args, **kwargs):
        user = password = dsn = None
        if len(args) == 1:
            conn_str = args[0]
            if "/" in conn_str:
                user, tail = conn_str.split("/", 1)
                if "@" in tail:
                    password, dsn = tail.split("@", 1)
            if None in (user, password, dsn):
                throw(
                    ValueError,
                    "Incorrect connection string (must be in form of 'user/password@dsn')",
                )
        elif len(args) == 2:
            user, password = args
        elif len(args) == 3:
            user, password, dsn = args
        elif args:
            throw(ValueError, "Invalid number of positional arguments")

        def setdefault(kwargs, key, value):
            kwargs_value = kwargs.setdefault(key, value)
            if value is not None and value != kwargs_value:
                throw(ValueError, "Ambiguous value for " + key)

        setdefault(kwargs, "user", user)
        setdefault(kwargs, "password", password)
        setdefault(kwargs, "dsn", dsn)

        if cx_Oracle.__name__ == "cx_Oracle":
            kwargs.setdefault("threaded", True)
        else:
            kwargs.pop("threaded", None)
        kwargs.setdefault("min", 1)
        kwargs.setdefault("max", 10)
        kwargs.setdefault("increment", 1)
        return OraPool(**kwargs)

    def table_exists(self, connection, table_name, case_sensitive=True):
        owner_name, table_name = self.split_table_name(table_name)
        cursor = connection.cursor()
        if case_sensitive:
            sql = "SELECT table_name FROM all_tables WHERE owner = :o AND table_name = :tn"
        else:
            sql = "SELECT table_name FROM all_tables WHERE owner = :o AND upper(table_name) = upper(:tn)"
        cursor.execute(sql, dict(o=owner_name, tn=table_name))
        row = cursor.fetchone()
        return row[0] if row is not None else None

    def index_exists(self, connection, table_name, index_name, case_sensitive=True):
        owner_name, table_name = self.split_table_name(table_name)
        if not isinstance(index_name, str):
            throw(NotImplementedError)
        if case_sensitive:
            sql = (
                "SELECT index_name FROM all_indexes WHERE owner = :o "
                "AND index_name = :i AND table_owner = :o AND table_name = :t"
            )
        else:
            sql = (
                "SELECT index_name FROM all_indexes WHERE owner = :o "
                "AND upper(index_name) = upper(:i) AND table_owner = :o AND table_name = :t"
            )
        cursor = connection.cursor()
        cursor.execute(sql, dict(o=owner_name, i=index_name, t=table_name))
        row = cursor.fetchone()
        return row[0] if row is not None else None

    def fk_exists(self, connection, table_name, fk_name, case_sensitive=True):
        owner_name, table_name = self.split_table_name(table_name)
        if not isinstance(fk_name, str):
            throw(NotImplementedError)
        if case_sensitive:
            sql = (
                "SELECT constraint_name FROM user_constraints WHERE constraint_type = 'R' "
                "AND table_name = :tn AND constraint_name = :cn AND owner = :o"
            )
        else:
            sql = (
                "SELECT constraint_name FROM user_constraints WHERE constraint_type = 'R' "
                "AND table_name = :tn AND upper(constraint_name) = upper(:cn) AND owner = :o"
            )
        cursor = connection.cursor()
        cursor.execute(sql, dict(tn=table_name, cn=fk_name, o=owner_name))
        row = cursor.fetchone()
        return row[0] if row is not None else None

    def table_has_data(self, connection, table_name):
        cursor = connection.cursor()
        cursor.execute(
            "SELECT 1 FROM %s WHERE ROWNUM = 1" % self.quote_name(table_name)
        )
        return cursor.fetchone() is not None

    def drop_table(self, connection, table_name):
        cursor = connection.cursor()
        sql = "DROP TABLE %s CASCADE CONSTRAINTS" % self.quote_name(table_name)
        cursor.execute(sql)


provider_cls = OraProvider


def to_int_or_decimal(val):
    val = val.replace(",", ".")
    if "." in val:
        return Decimal(val)
    return int(val)


def to_decimal(val):
    return Decimal(val.replace(",", "."))


def output_type_handler(cursor, name, defaultType, size, precision, scale):
    if defaultType == cx_Oracle.NUMBER:
        if scale == 0:
            if precision:
                return cursor.var(
                    cx_Oracle.STRING, 40, cursor.arraysize, outconverter=int
                )
            return cursor.var(
                cx_Oracle.STRING, 40, cursor.arraysize, outconverter=to_int_or_decimal
            )
        if scale != -127:
            return cursor.var(
                cx_Oracle.STRING, 100, cursor.arraysize, outconverter=to_decimal
            )
    elif defaultType in (cx_Oracle.STRING, cx_Oracle.FIXED_CHAR):
        return cursor.var(str, size, cursor.arraysize)  # from cx_Oracle example
    return None


class OraPool:
    forked_pools = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.cx_pool = cx_Oracle.SessionPool(**kwargs)
        self.pid = os.getpid()

    def connect(self):
        pid = os.getpid()
        if self.pid != pid:
            self.forked_pools.append((self.cx_pool, self.pid))
            self.cx_pool = cx_Oracle.SessionPool(**self.kwargs)
            self.pid = os.getpid()
        if core.local.debug:
            log_orm("GET CONNECTION")
        con = self.cx_pool.acquire()
        con.outputtypehandler = output_type_handler
        return con, True

    def release(self, con):
        self.cx_pool.release(con)

    def drop(self, con):
        self.cx_pool.drop(con)

    def disconnect(self):
        pass


def get_inputsize(arg):
    if isinstance(arg, datetime):
        return cx_Oracle.TIMESTAMP
    return None


def set_input_sizes(cursor, arguments):
    if type(arguments) is dict:
        input_sizes = {}
        for name, arg in arguments.items():
            size = get_inputsize(arg)
            if size is not None:
                input_sizes[name] = size
        cursor.setinputsizes(**input_sizes)
    elif type(arguments) is tuple:
        input_sizes = map(get_inputsize, arguments)
        cursor.setinputsizes(*input_sizes)
    else:
        assert False, type(arguments)  # pragma: no cover
