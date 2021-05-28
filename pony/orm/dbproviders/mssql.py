from __future__ import absolute_import
from pony.py23compat import PY2, imap, basestring, buffer, int_types

import json
from decimal import Decimal
from datetime import datetime, date, time, timedelta
from uuid import UUID
import re
NoneType = type(None)

import warnings
warnings.filterwarnings('ignore', '^Table.+already exists$', Warning, '^pony\\.orm\\.dbapiprovider$')

try:
    import pyodbc  as mssql_module
    MSSQL_module_name = 'pyodbc'
except ImportError:
        raise ImportError('In order to use PonyORM with MSSQL please install pyodbc')

from pony.orm import core, dbschema, dbapiprovider, ormtypes, sqltranslation
from pony.orm.core import log_orm
from pony.orm.dbapiprovider import DBAPIProvider, Pool, get_version_tuple, wrap_dbapi_exceptions
from pony.orm.sqltranslation import SQLTranslator, TranslationError
from pony.orm.sqlbuilding import Value, Param, SQLBuilder, join
from pony.utils import throw
from pony.converting import str2timedelta, timedelta2str

PYODBC_VAR_REGEX = re.compile(r'(?<!%)[%]s')

class MSSQLColumn(dbschema.Column):
    auto_template = '%(type)s IDENTITY(1,1) PRIMARY KEY'

class MSSQLSchema(dbschema.DBSchema):
    dialect = 'MSSQL'
    inline_fk_syntax = False
    column_class = MSSQLColumn

class MSSQLTranslator(SQLTranslator):
    dialect = 'MSSQL'
    json_path_wildcard_syntax = True

class MSSQLValue(Value):
    __slots__ = []
    def __unicode__(self):
        value = self.value
        if isinstance(value, timedelta):
            if value.microseconds:
                return "INTERVAL '%s' HOUR_MICROSECOND" % timedelta2str(value)
            return "INTERVAL '%s' HOUR_SECOND" % timedelta2str(value)
        
        if isinstance(value, datetime):
            result = value.isoformat(' ')
            return self.quote_str(result)
        
        return Value.__unicode__(self)
    if not PY2: __str__ = __unicode__

class MSSQLBuilder(SQLBuilder):
    dialect = 'MSSQL'
    value_class = MSSQLValue
    
    def CONCAT(builder, *args):
        return 'CONCAT(',  join(', ', imap(builder, args)), ')'
        
    def TRIM(builder, expr, chars=None):
        if chars is None: return 'TRIM(', builder(expr), ')'
        return 'TRIM(', builder(chars), ' FROM ' ,builder(expr), ')'

    def LTRIM(builder, expr, chars=None):
        if chars is None: return 'ltrim(', builder(expr), ')'
        return 'LTRIM(', builder(chars), ' FROM ' ,builder(expr), ')'

    def RTRIM(builder, expr, chars=None):
        if chars is None: return 'RTRIM(', builder(expr), ')'
        return 'RTRIM(', builder(chars), ' FROM ' ,builder(expr), ')'

    def TO_INT(builder, expr):
        return 'CAST(', builder(expr), ' AS int)'

    def TO_REAL(builder, expr):
        return 'CAST(', builder(expr), ' AS float)'

    def TO_STR(builder, expr):
        return 'CAST(', builder(expr), ' AS nvarchar)'

    def YEAR(builder, expr):
        return 'YEAR(', builder(expr), ')'

    def MONTH(builder, expr):
        return 'MONTH(', builder(expr), ')'

    def DAY(builder, expr):
        return 'DAY(', builder(expr), ')'

    def HOUR(builder, expr):
        return 'DATEPART(hh, ', builder(expr), ')'

    def MINUTE(builder, expr):
        return 'DATEPART(n, ', builder(expr), ')'

    def SECOND(builder, expr):
        return 'DATEPART(ss, ', builder(expr), ')'
        
    def LIMIT(builder, limit, offset=0):
        order_by = [True for ast in builder.ast if 'ORDER_BY' in ast[0]]
        if not order_by or order_by[0] == False:
            return 'ORDER BY (SELECT NULL)' + f'OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY'
        
        return f'OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY'

# TODO: not fixed this yet
    def DATE_ADD(builder, expr, delta):
        if delta[0] == 'VALUE' and isinstance(delta[1], time):
            return 'ADDTIME(', builder(expr), ', ', builder(delta), ')'
        return 'ADDDATE(', builder(expr), ', ', builder(delta), ')'
    def DATE_SUB(builder, expr, delta):
        if delta[0] == 'VALUE' and isinstance(delta[1], time):
            return 'SUBTIME(', builder(expr), ', ', builder(delta), ')'
        return 'SUBDATE(', builder(expr), ', ', builder(delta), ')'
    def DATE_DIFF(builder, expr1, expr2):
        return 'TIMEDIFF(', builder(expr1), ', ', builder(expr2), ')'
    def DATETIME_ADD(builder, expr, delta):
        return builder.DATE_ADD(expr, delta)
    def DATETIME_SUB(builder, expr, delta):
        return builder.DATE_SUB(expr, delta)
    def DATETIME_DIFF(builder, expr1, expr2):
        return 'TIMEDIFF(', builder(expr1), ', ', builder(expr2), ')'
# End todo

    def JSON_VALUE(builder, expr, path, type):
        path_sql, has_params, has_wildcards = builder.build_json_path(path)
        escaped = escapify(builder(expr))
        result = 'JSON_VALUE(', escaped, ', ', path_sql, ')'
        return result

class MSSQLStrConverter(dbapiprovider.StrConverter):
    def sql_type(converter):
        if converter.max_len:
            return 'NVARCHAR(%d)' % converter.max_len
        return 'TEXT'

class MSSQLRealConverter(dbapiprovider.RealConverter):
    def sql_type(converter):
        return 'float'

class MSSQLBoolConverter(dbapiprovider.BoolConverter):
    def sql_type(converter):
        return 'BIT'

class MSSQLBlobConverter(dbapiprovider.BlobConverter):
    def sql_type(converter):
        return 'LONGBLOB'

class MSSQLTimeConverter(dbapiprovider.TimeConverter):
    def sql2py(converter, val):
        if isinstance(val, timedelta):  # MSSQLdb returns timedeltas instead of times
            total_seconds = val.days * (24 * 60 * 60) + val.seconds
            if 0 <= total_seconds <= 24 * 60 * 60:
                minutes, seconds = divmod(total_seconds, 60)
                hours, minutes = divmod(minutes, 60)
                return time(hours, minutes, seconds, val.microseconds)
        elif not isinstance(val, time): throw(ValueError,
            'Value of unexpected type received from database%s: instead of time or timedelta got %s'
            % ('for attribute %s' % converter.attr if converter.attr else '', type(val)))
        return val

class MSSQLTimedeltaConverter(dbapiprovider.TimedeltaConverter):
    sql_type_name = 'TIME'

class MSSQLUuidConverter(dbapiprovider.UuidConverter):
    def sql_type(converter):
        return 'BINARY(16)'

class MSSQLJsonConverter(dbapiprovider.JsonConverter):
    def sql_type(self):
        return 'NVARCHAR(MAX)'

def quotify(sql, arguments):
        for arg in arguments:
            if isinstance(arg, str):
                sql = sql.replace('{}', "'" + arg + "'" , 1)
            else:
                sql = sql.replace('{}', str(arg), 1)

        return sql

def escapify(sql):
    for escape in range(int(sql.count('|') / 2)):
        sql = sql.replace('|', '[', 1)
        sql = sql.replace('|', ']', 1)
    return sql

class MSSQLProvider(DBAPIProvider):
    dialect = 'MSSQL'
    paramstyle = 'format'
    quote_char = "|"
    max_name_len = 64
    max_params_count = 10000
    table_if_not_exists_syntax = True
    index_if_not_exists_syntax = False
    max_time_precision = default_time_precision = 0
    varchar_default_max_len = 255
    uint64_support = True

    dbapi_module = mssql_module
    dbschema_cls = MSSQLSchema
    translator_cls = MSSQLTranslator
    sqlbuilder_cls = MSSQLBuilder

    default_schema_name = 'dbo'

    fk_types = { 'SERIAL' : 'BIGINT UNSIGNED' }

    converter_classes = [
        (NoneType, dbapiprovider.NoneConverter),
        (bool, MSSQLBoolConverter),
        (basestring, MSSQLStrConverter),
        (int_types, dbapiprovider.IntConverter),
        (float, MSSQLRealConverter),
        (Decimal, dbapiprovider.DecimalConverter),
        (datetime, dbapiprovider.DatetimeConverter),
        (date, dbapiprovider.DateConverter),
        (time, MSSQLTimeConverter),
        (timedelta, MSSQLTimedeltaConverter),
        (UUID, MSSQLUuidConverter),
        (buffer, MSSQLBlobConverter),
        (ormtypes.Json, MSSQLJsonConverter),
    ]

    def normalize_name(provider, name):
        return name[:provider.max_name_len].lower()

    @wrap_dbapi_exceptions
    def inspect_connection(provider, connection):
        cursor = connection.cursor()
        cursor.execute('Select @@version')
        row = cursor.fetchone()
        assert row is not None
        provider.server_version = row[0]
        # cursor.execute('select database()')
        # provider.default_schema_name = cursor.fetchone()[0]
        # cursor.execute('set session group_concat_max_len = 4294967295')

    def should_reconnect(provider, exc):
        return isinstance(exc, mssql_module.OperationalError) and exc.args[0] in (2006, 2013)

    def get_pool(provider, *args, **kwargs):
        driver      = kwargs['driver']
        server      = kwargs['server']
        database    = kwargs['database']
        username    = kwargs['username']
        password    = kwargs['password']
        connection_string = f'Driver={driver};Server={server};Database={database};UID={username};PWD={password};MARS_Connection=Yes'
        return Pool(mssql_module, connection_string, **kwargs)

    @wrap_dbapi_exceptions
    def set_transaction_mode(provider, connection, cache):
        assert not cache.in_transaction
        db_session = cache.db_session
        if db_session is not None and db_session.ddl:
            cache.in_transaction = True
        if db_session is not None and db_session.serializable:
            cursor = connection.cursor()
            sql = 'SET TRANSACTION ISOLATION LEVEL SERIALIZABLE'
            if core.local.debug: log_orm(sql)
            cursor.execute(sql)
            cache.in_transaction = True

    @wrap_dbapi_exceptions
    def execute(provider, cursor, sql, arguments=None, returning_id=False):
        sql = sql.replace('`', "")
        sql = sql.replace('\n', ' ')
        sql = PYODBC_VAR_REGEX.sub('?', sql)
        sql = escapify(sql)
        if type(arguments) is list:
            assert arguments and not returning_id
            cursor.executemany(sql, arguments)
        else:
            if arguments is None: cursor.execute(sql)
            else: 
                cursor.execute(sql, arguments)
        if returning_id: 
            id = cursor.execute('SELECT @@Identity').fetchone()[0]
            # id = cursor.execute('SELECT SCOPE_IDENTITY() AS [SCOPE_IDENTITY]').fetchone()[0]
            if id:
                return int(id)
            else:
                return id


    @wrap_dbapi_exceptions
    def release(provider, connection, cache=None):
        if cache is not None:
            db_session = cache.db_session
            if db_session is not None and db_session.ddl and cache.saved_fk_state:
                try:
                    cursor = connection.cursor()
                    sql = 'SET foreign_key_checks = 1'
                    if core.local.debug: log_orm(sql)
                    cursor.execute(sql)
                except:
                    provider.pool.drop(connection)
                    raise
        DBAPIProvider.release(provider, connection, cache)

    def table_exists(provider, connection, table_name, case_sensitive=True):
        db_name, table_name = provider.split_table_name(table_name)
        cursor = connection.cursor()
        sql = """
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema=? and table_name=?
        """
        cursor.execute(sql, [ db_name, table_name ])
        row = cursor.fetchone()
        return row[0] if row is not None else None

    def index_exists(provider, connection, table_name, index_name, case_sensitive=True):
        #TODO: might not be ready, need to check this
        db_name, table_name = provider.split_table_name(table_name)
        sql = """
            SELECT name
            FROM sys.indexes 
            WHERE object_id = OBJECT_ID(?)
            AND name=?
        """
        cursor = connection.cursor()
        cursor.execute(sql, [f'{db_name}.{table_name}', index_name])
        row = cursor.fetchone()
        return row[0] if row is not None else None

    def fk_exists(provider, connection, table_name, fk_name, case_sensitive=True):
        db_name, table_name = provider.split_table_name(table_name)

        sql = """
            SELECT name
            FROM sys.foreign_keys 
            WHERE object_id = OBJECT_ID(?)
        """
        cursor = connection.cursor()
        cursor.execute(sql, [ f'{db_name}.{fk_name}'])
        row = cursor.fetchone()
        return row[0] if row is not None else None

    def table_has_data(provider, connection, table_name):
        cursor = connection.cursor()
        provider.execute(cursor, escapify('SELECT TOP 1 * FROM %s' % provider.quote_name(table_name)))
        return cursor.fetchone() is not None

    def drop_table(self, connection, table_name):
        
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
            
            DROP TABLE "%(table)s"
            
        ''' % { 'table' : table_name }
        
        cursor.execute(sql)

provider_cls = MSSQLProvider

def str2datetime(s):
    if 19 < len(s) < 26: s += '000000'[:26-len(s)]
    s = s.replace('-', ' ').replace(':', ' ').replace('.', ' ').replace('T', ' ')
    try:
        return datetime(*imap(int, s.split()))
    except ValueError:
        return None  # for incorrect values like 0000-00-00 00:00:00
