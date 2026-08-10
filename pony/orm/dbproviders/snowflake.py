from __future__ import absolute_import
from pony.py23compat import buffer, int_types

from decimal import Decimal
from datetime import datetime, date, time, timedelta
from uuid import UUID

from snowflake import connector

from pony.orm import dbschema, dbapiprovider, ormtypes
from pony.utils import throw
from pony.orm.dbapiprovider import DBAPIProvider, wrap_dbapi_exceptions
from pony.orm.sqltranslation import SQLTranslator
from pony.orm.sqlbuilding import Value, SQLBuilder

NoneType = type(None)

class SnowflakeColumn(dbschema.Column):
    auto_template = 'IDENTITY(1,1) PRIMARY KEY'

class SnowflakeSchema(dbschema.DBSchema):
    dialect = 'Snowflake'
    column_class = SnowflakeColumn
    def create_tables(schema, provider, connection):
        for table in schema.order_tables_to_create():
            for db_object in table.get_objects_to_create():
                name = db_object.exists(provider, connection, case_sensitive=False)
                if name is None:
                    throw(dbapiprovider.NotSupportedError, 'Snowflake: table creation is not supported')
    def check_tables(schema, provider, connection):
        pass

class SnowflakeTranslator(SQLTranslator):
    dialect = 'Snowflake'

class SnowflakeValue(Value):
    __slots__ = []
    def __str__(self):
        value = self.value
        if isinstance(value, bool):
            return value and 'true' or 'false'
        return Value.__str__(self)

class SnowflakeSQLBuilder(SQLBuilder):
    dialect = 'Snowflake'
    value_class = SnowflakeValue
    def INSERT(builder, table_name, columns, values, returning=None):
        throw(dbapiprovider.NotSupportedError, 'Snowflake: data modification is not supported')
    def UPDATE(builder, table_name, pairs, where=None):
        throw(dbapiprovider.NotSupportedError, 'Snowflake: data modification is not supported')
    def DELETE(builder, alias, from_ast, where=None):
        throw(dbapiprovider.NotSupportedError, 'Snowflake: data modification is not supported')
    def TO_INT(builder, expr):
        return 'CAST(', builder(expr), ' AS INT)'
    def TO_STR(builder, expr):
        return 'CAST(', builder(expr), ' AS VARCHAR)'
    def TO_REAL(builder, expr):
        return 'CAST(', builder(expr), ' AS DOUBLE PRECISION)'
    def DATE(builder, expr):
        return 'CAST(', builder(expr), ' AS DATE)'
    def RANDOM(builder):
        return 'random()'
    def DATE_ADD(builder, expr, delta):
        return 'DATEADD(day, ', builder(delta), ', ', builder(expr), ')'
    def DATE_SUB(builder, expr, delta):
        return 'DATEADD(day, -', builder(delta), ', ', builder(expr), ')'
    def DATE_DIFF(builder, expr1, expr2):
        return 'DATEDIFF(day, ', builder(expr2), ', ', builder(expr1), ')'
    def DATETIME_ADD(builder, expr, delta):
        return 'DATEADD(second, ', builder(delta), ', ', builder(expr), ')'
    def DATETIME_SUB(builder, expr, delta):
        return 'DATEADD(second, -', builder(delta), ', ', builder(expr), ')'
    def DATETIME_DIFF(builder, expr1, expr2):
        return 'DATEDIFF(second, ', builder(expr2), ', ', builder(expr1), ')'

class SnowflakeIntConverter(dbapiprovider.IntConverter):
    signed_types = {None: 'NUMBER', 8: 'NUMBER', 16: 'NUMBER', 24: 'NUMBER', 32: 'NUMBER', 64: 'NUMBER'}
    unsigned_types = {None: 'NUMBER', 8: 'NUMBER', 16: 'NUMBER', 24: 'NUMBER', 32: 'NUMBER'}

class SnowflakeRealConverter(dbapiprovider.RealConverter):
    def sql_type(converter):
        return 'DOUBLE PRECISION'

class SnowflakeBlobConverter(dbapiprovider.BlobConverter):
    def sql_type(converter):
        return 'BINARY'

class SnowflakeTimedeltaConverter(dbapiprovider.TimedeltaConverter):
    sql_type_name = 'VARCHAR' # Snowflake doesn't have a direct INTERVAL type that maps easily to timedelta

class SnowflakeDatetimeConverter(dbapiprovider.DatetimeConverter):
    sql_type_name = 'TIMESTAMP'

class SnowflakeUuidConverter(dbapiprovider.UuidConverter):
    def py2sql(converter, val):
        return str(val)
    def sql_type(converter):
        return 'VARCHAR(36)'

class SnowflakeJsonConverter(dbapiprovider.JsonConverter):
    def sql_type(self):
        return "VARIANT"

class SnowflakeProvider(DBAPIProvider):
    dialect = 'Snowflake'
    paramstyle = 'pyformat'
    max_name_len = 255
    max_params_count = 10000

    dbapi_module = connector
    dbschema_cls = SnowflakeSchema
    translator_cls = SnowflakeTranslator
    sqlbuilder_cls = SnowflakeSQLBuilder

    default_schema_name = 'PUBLIC'

    def normalize_name(provider, name):
        return name[:provider.max_name_len].upper()

    def inspect_connection(provider, connection):
        pass

    def should_reconnect(provider, exc):
        return False # TODO: implement proper reconnection logic for snowflake

    def set_transaction_mode(provider, connection, cache):
        pass

    def drop_table(provider, connection, table_name):
        throw(dbapiprovider.NotSupportedError, 'Snowflake: table dropping is not supported')

    @wrap_dbapi_exceptions
    def execute(provider, cursor, sql, arguments=None, returning_id=False):
        if returning_id:
            throw(dbapiprovider.NotSupportedError, 'Snowflake: data modification is not supported')
        if type(arguments) is list:
            throw(dbapiprovider.NotSupportedError, 'Snowflake: data modification is not supported')
        
        sql_upper = sql.upper().lstrip()
        if sql_upper.startswith(('INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER')):
            throw(dbapiprovider.NotSupportedError, 'Snowflake: data modification is not supported')

        if arguments is None: cursor.execute(sql)
        else: cursor.execute(sql, arguments)

    def table_exists(provider, connection, table_name, case_sensitive=True):
        schema_name, table_name = provider.split_table_name(table_name)
        cursor = connection.cursor()
        if case_sensitive:
            sql = 'SHOW TABLES LIKE %s IN SCHEMA ' + schema_name
            cursor.execute(sql, (table_name,))
        else:
            sql = 'SHOW TABLES IN SCHEMA ' + schema_name
            cursor.execute(sql)
            # Need to filter manually for case-insensitive
        row = cursor.fetchone()
        return row is not None

    converter_classes = [
        (NoneType, dbapiprovider.NoneConverter),
        (bool, dbapiprovider.BoolConverter),
        (str, dbapiprovider.StrConverter),
        (int_types, SnowflakeIntConverter),
        (float, SnowflakeRealConverter),
        (Decimal, dbapiprovider.DecimalConverter),
        (datetime, SnowflakeDatetimeConverter),
        (date, dbapiprovider.DateConverter),
        (time, dbapiprovider.TimeConverter),
        (timedelta, SnowflakeTimedeltaConverter),
        (UUID, SnowflakeUuidConverter),
        (buffer, SnowflakeBlobConverter),
        (ormtypes.Json, SnowflakeJsonConverter),
    ]

provider_cls = SnowflakeProvider
