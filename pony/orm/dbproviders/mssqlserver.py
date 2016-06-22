from __future__ import absolute_import
from pony.py23compat import PY2, imap, basestring, buffer, int_types

from decimal import Decimal
from datetime import datetime, date, time, timedelta
from uuid import UUID

NoneType = type(None)

import warnings
warnings.filterwarnings('ignore', '^Table.+already exists$', Warning, '^pony\\.orm\\.dbapiprovider$')

import pyodbc

from pony.orm import core, dbschema, dbapiprovider
from pony.orm.core import log_orm, OperationalError
from pony.orm.dbapiprovider import DBAPIProvider, Pool, get_version_tuple, wrap_dbapi_exceptions
from pony.orm.sqltranslation import SQLTranslator
from pony.orm.sqlbuilding import SQLBuilder, join, indentable, make_unary_func
from pony.utils import throw
from pony.converting import str2timedelta, timedelta2str

class MSColumn(dbschema.Column):
    auto_template = '%(type)s PRIMARY KEY IDENTITY(1, 1)'

class MSSchema(dbschema.DBSchema):
    dialect = 'MSSQL'
    inline_fk_syntax = False
    column_class = MSColumn

class MSTranslator(SQLTranslator):
    dialect = 'MSSQL'

class MSBuilder(SQLBuilder):
    dialect = 'MSSQL'

    def INSERT(builder, table_name, columns, values, returning=None):
        return [
            'INSERT INTO ', builder.quote_name(table_name), ' (',
            join(', ', [builder.quote_name(column) for column in columns ]),
            ') OUTPUT inserted.', builder.quote_name(returning),
            ' VALUES (',
            join(', ', [builder(value) for value in values]), ')'
        ]

    LENGTH = make_unary_func('LEN')


    def CONCAT(builder, *args):
        return 'concat(',  join(', ', imap(builder, args)), ')'
    def TRIM(builder, expr, chars=None):
        if chars is None: return 'trim(', builder(expr), ')'
        return 'trim(both ', builder(chars), ' from ' ,builder(expr), ')'
    def LTRIM(builder, expr, chars=None):
        if chars is None: return 'ltrim(', builder(expr), ')'
        return 'trim(leading ', builder(chars), ' from ' ,builder(expr), ')'
    def RTRIM(builder, expr, chars=None):
        if chars is None: return 'rtrim(', builder(expr), ')'
        return 'trim(trailing ', builder(chars), ' from ' ,builder(expr), ')'
    def YEAR(builder, expr):
        return 'year(', builder(expr), ')'
    def MONTH(builder, expr):
        return 'month(', builder(expr), ')'
    def DAY(builder, expr):
        return 'day(', builder(expr), ')'
    def HOUR(builder, expr):
        return 'hour(', builder(expr), ')'
    def MINUTE(builder, expr):
        return 'minute(', builder(expr), ')'
    def SECOND(builder, expr):
        return 'second(', builder(expr), ')'
    def DATE_ADD(builder, expr, delta):
        if isinstance(delta, timedelta):
            return 'DATE_ADD(', builder(expr), ", INTERVAL '", timedelta2str(delta), "' HOUR_SECOND)"
        return 'ADDTIME(', builder(expr), ', ', builder(delta), ')'
    def DATE_SUB(builder, expr, delta):
        if isinstance(delta, timedelta):
            return 'DATE_SUB(', builder(expr), ", INTERVAL '", timedelta2str(delta), "' HOUR_SECOND)"
        return 'SUBTIME(', builder(expr), ', ', builder(delta), ')'
    def DATETIME_ADD(builder, expr, delta):
        if isinstance(delta, timedelta):
            return 'DATE_ADD(', builder(expr), ", INTERVAL '", timedelta2str(delta), "' HOUR_SECOND)"
        return 'ADDTIME(', builder(expr), ', ', builder(delta), ')'
    def DATETIME_SUB(builder, expr, delta):
        if isinstance(delta, timedelta):
            return 'DATE_SUB(', builder(expr), ", INTERVAL '", timedelta2str(delta), "' HOUR_SECOND)"
        return 'SUBTIME(', builder(expr), ', ', builder(delta), ')'

    @indentable
    def LIMIT(builder, limit, offset=None):
        if offset is None:
            offset = ('VALUE', 0)
        return 'OFFSET ', builder(offset), ' ROWS FETCH NEXT ', builder(limit),  ' ROWS ONLY\n'


class MSBoolConverter(dbapiprovider.BoolConverter):
    def sql_type(converter):
        return "BIT"


class MSIntConverter(dbapiprovider.IntConverter):
    signed_types = {None: 'INTEGER', 8: 'SMALLINT', 16: 'SMALLINT', 24: 'INTEGER', 32: 'INTEGER', 64: 'BIGINT'}
    unsigned_types = {None: 'INTEGER', 8: 'TINYINT', 16: 'INTEGER', 24: 'INTEGER', 32: 'BIGINT'}

class MSStrConverter(dbapiprovider.StrConverter):
    def sql_type(converter):
        if converter.max_len:
            return 'VARCHAR(%d)' % converter.max_len
        attr = converter.attr
        if attr is not None and (attr.is_unique or attr.composite_keys):
            return 'VARCHAR(8000)'
        return 'VARCHAR(MAX)'
        # result = 'VARCHAR(%d)' % converter.max_len if converter.max_len else 'TEXT'
        # if converter.db_encoding: result += ' CHARACTER SET %s' % converter.db_encoding
        # return result

class MSRealConverter(dbapiprovider.RealConverter):
    def sql_type(converter):
        return 'FLOAT'

class MSBlobConverter(dbapiprovider.BlobConverter):
    def sql_type(converter):
        return 'VARBINARY(MAX)'

class MSTimeConverter(dbapiprovider.TimeConverter):
    def sql2py(converter, val):
        if isinstance(val, timedelta):  # MySQLdb returns timedeltas instead of times
            total_seconds = val.days * (24 * 60 * 60) + val.seconds
            if 0 <= total_seconds <= 24 * 60 * 60:
                minutes, seconds = divmod(total_seconds, 60)
                hours, minutes = divmod(minutes, 60)
                return time(hours, minutes, seconds, val.microseconds)
        elif not isinstance(val, time): throw(ValueError,
            'Value of unexpected type received from database%s: instead of time or timedelta got %s'
            % ('for attribute %s' % converter.attr if converter.attr else '', type(val)))
        return val

class MSTimedeltaConverter(dbapiprovider.TimedeltaConverter):
    sql_type_name = 'TIME'

class MSUuidConverter(dbapiprovider.UuidConverter):
    def sql_type(converter):
        return 'BINARY(16)'

class MSDateConverter(dbapiprovider.DateConverter):
    def py2sql(converter, val):
        val = dbapiprovider.DateConverter.py2sql(converter, val)
        if isinstance(val, date):
            val = val.strftime('%Y-%m-%d')
        return val
    def sql_type(converter):
        return 'DATE'

class MSProvider(DBAPIProvider):
    dialect = 'MSSQL'
    paramstyle = 'qmark'
    quote_char = '"'
    max_name_len = 128

    table_if_not_exists_syntax = True
    index_if_not_exists_syntax = False
    select_for_update_nowait_syntax = False
    max_time_precision = default_time_precision = 0
    varchar_default_max_len = 255
    uint64_support = True

    dbapi_module = pyodbc

    dbschema_cls = MSSchema
    translator_cls = MSTranslator
    sqlbuilder_cls = MSBuilder


    default_schema_name = 'dbo'
    name_before_table = 'db_name'

    converter_classes = [
        (NoneType, dbapiprovider.NoneConverter),
        (bool, MSBoolConverter),
        (basestring, MSStrConverter),
        (int_types, MSIntConverter),
        (float, MSRealConverter),
        (Decimal, dbapiprovider.DecimalConverter),
        (datetime, dbapiprovider.DatetimeConverter),
        (date, MSDateConverter),
        (time, MSTimeConverter),
        (timedelta, MSTimedeltaConverter),
        (UUID, MSUuidConverter),
        (buffer, MSBlobConverter),
    ]

    def __init__(provider, *args, **kwargs):
        provider.connect_args = args
        provider.connect_kwargs = kwargs
        connection = provider.connect()
        provider.connection = connection
        provider.inspect_connection(connection)
        provider.release(connection)

    def normalize_name(provider, name):
        return name[:provider.max_name_len].lower()

    @wrap_dbapi_exceptions
    def connect(provider):
        args = provider.connect_args
        kwargs = provider.connect_kwargs
        return pyodbc.connect(*args, **kwargs)

    @wrap_dbapi_exceptions
    def release(provider, connection, cache=None):
        connection.close()

    @wrap_dbapi_exceptions
    def disconnect(provider):
        provider.release(provider.connection)

    @wrap_dbapi_exceptions
    def inspect_connection(provider, connection):
        cursor = connection.cursor()
        cursor.execute('select @@version')
        row = cursor.fetchone()
        assert row is not None
        cursor.execute('select DB_NAME()')
        provider.default_schema_name = cursor.fetchone()[0]

    def should_reconnect(provider, exc):
        return isinstance(exc, pyodbc.OperationalError)

    @wrap_dbapi_exceptions
    def set_transaction_mode(provider, connection, cache):
        assert not cache.in_transaction
        db_session = cache.db_session
        if db_session is not None and db_session.serializable:
            cursor = connection.cursor()
            sql = 'SET TRANSACTION ISOLATION LEVEL SERIALIZABLE'
            if core.debug: log_orm(sql)
            cursor.execute(sql)
        cache.immediate = True
        if db_session is not None and (db_session.serializable or db_session.ddl):
            cache.in_transaction = True

    @wrap_dbapi_exceptions
    def execute(provider, cursor, sql, arguments=None, returning_id=False):
        if type(arguments) is list:
            assert arguments and not returning_id
            cursor.executemany(sql, arguments)
        else:
            if arguments is None: cursor.execute(sql)
            else: cursor.execute(sql, arguments)
            if returning_id:
                return cursor.fetchone()[0]


    def table_exists(provider, connection, table_name, case_sensitive=True):
        db_name, table_name = provider.split_table_name(table_name)
        cursor = connection.cursor()
        if case_sensitive: sql = 'SELECT table_name FROM information_schema.tables ' \
                                 'WHERE table_schema=? and table_name=?'
        else: sql = 'SELECT table_name FROM information_schema.tables ' \
                    'WHERE table_schema=? and UPPER(table_name)=UPPER(?)'
        cursor.execute(sql, [ db_name, table_name ])
        row = cursor.fetchone()
        return row[0] if row is not None else None

    def index_exists(provider, connection, table_name, index_name, case_sensitive=True):
        table_name = provider.quote_name(table_name)
        if case_sensitive: sql = "SELECT top 1 1 FROM sys.indexes WHERE name=? AND object_id=OBJECT_ID(?)"
        else: sql = "SELECT top 1 1 FROM sys.indexes WHERE lower(name)=lower(?) AND object_id=OBJECT_ID(?)"
        cursor = connection.cursor()
        cursor.execute(sql, [ index_name, table_name ])
        row = cursor.fetchone()
        return row[0] if row is not None else None

    def fk_exists(provider, connection, table_name, fk_name, case_sensitive=True):
        schema_name, table_name = provider.split_table_name(table_name)
        table_name = provider.quote_name(table_name)
        fk_name = provider.quote_name([ schema_name, fk_name ])
        # if case_sensitive: ???
        sql = "SELECT name FROM sys.foreign_keys " \
              "WHERE object_id = OBJECT_ID(?) AND parent_object_id=OBJECT_ID(?)";
        cursor = connection.cursor()
        cursor.execute(sql, [ fk_name, table_name ])
        row = cursor.fetchone()
        return row[0] if row is not None else None

    def table_has_data(provider, connection, table_name):
        table_name = provider.quote_name(table_name)
        cursor = connection.cursor()
        cursor.execute('SELECT TOP 1 1 FROM %s' % table_name)
        return cursor.fetchone() is not None

    def drop_table(provider, connection, table_name):
        table_name = provider.quote_name(table_name)
        cursor = connection.cursor()
        sql = '''
        DECLARE @sql nvarchar(1000)

        WHILE EXISTS(
            SELECT *
            FROM sys.foreign_keys
            WHERE referenced_object_id = object_id('%(table)s')
        )
        BEGIN
            SELECT
                @sql = 'ALTER TABLE ' +  OBJECT_SCHEMA_NAME(parent_object_id) +
                '.[' + OBJECT_NAME(parent_object_id) +
                '] DROP CONSTRAINT ' + name
                FROM sys.foreign_keys
                WHERE referenced_object_id = object_id('%(table)s')
            exec  sp_executesql @sql
        END

        DROP TABLE %(table)s
        ''' % { 'table' : table_name }
        cursor.execute(sql)

provider_cls = MSProvider
