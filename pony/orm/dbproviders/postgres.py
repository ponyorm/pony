from __future__ import absolute_import
from pony.py23compat import PY2, basestring, unicode, buffer, int_types

from decimal import Decimal
from datetime import datetime, date, time, timedelta
from uuid import UUID

try:
    import psycopg2
except ImportError:
    from psycopg2cffi import compat
    compat.register()

from psycopg2 import extensions

import psycopg2.extras
psycopg2.extras.register_uuid()

psycopg2.extras.register_default_json(loads=lambda x: x)
psycopg2.extras.register_default_jsonb(loads=lambda x: x)

from pony.orm import core, dbschema, dbapiprovider, sqltranslation, ormtypes
from pony.orm.core import log_orm
from pony.orm.dbapiprovider import DBAPIProvider, Pool, wrap_dbapi_exceptions
from pony.orm.sqltranslation import SQLTranslator
from pony.orm.sqlbuilding import Value, SQLBuilder
from pony.converting import timedelta2str
from pony.utils import is_ident

NoneType = type(None)

class PGColumn(dbschema.Column):
    auto_template = 'SERIAL PRIMARY KEY'

class PGSchema(dbschema.DBSchema):
    dialect = 'PostgreSQL'
    column_class = PGColumn

class PGTranslator(SQLTranslator):
    dialect = 'PostgreSQL'

class PGValue(Value):
    __slots__ = []
    def __unicode__(self):
        value = self.value
        if isinstance(value, bool): return value and 'true' or 'false'
        return Value.__unicode__(self)
    if not PY2: __str__ = __unicode__

class PGSQLBuilder(SQLBuilder):
    dialect = 'PostgreSQL'
    value_class = PGValue
    def INSERT(builder, table_name, columns, values, returning=None):
        if not values: result = [ 'INSERT INTO ', builder.quote_name(table_name) ,' DEFAULT VALUES' ]
        else: result = SQLBuilder.INSERT(builder, table_name, columns, values)
        if returning is not None: result.extend([' RETURNING ', builder.quote_name(returning) ])
        return result
    def TO_INT(builder, expr):
        return '(', builder(expr), ')::int'
    def DATE(builder, expr):
        return '(', builder(expr), ')::date'
    def RANDOM(builder):
        return 'random()'
    def DATE_ADD(builder, expr, delta):
        if isinstance(delta, timedelta):
            return '(', builder(expr), " + INTERVAL '", timedelta2str(delta), "' DAY TO SECOND)"
        return '(', builder(expr), ' + ', builder(delta), ')'
    def DATE_SUB(builder, expr, delta):
        if isinstance(delta, timedelta):
            return '(', builder(expr), " - INTERVAL '", timedelta2str(delta), "' DAY TO SECOND)"
        return '(', builder(expr), ' - ', builder(delta), ')'
    def DATETIME_ADD(builder, expr, delta):
        if isinstance(delta, timedelta):
            return '(', builder(expr), " + INTERVAL '", timedelta2str(delta), "' DAY TO SECOND)"
        return '(', builder(expr), ' + ', builder(delta), ')'
    def DATETIME_SUB(builder, expr, delta):
        if isinstance(delta, timedelta):
            return '(', builder(expr), " - INTERVAL '", timedelta2str(delta), "' DAY TO SECOND)"
        return '(', builder(expr), ' - ', builder(delta), ')'
    def eval_json_path(builder, values):
        result = []
        for value in values:
            if isinstance(value, int):
                result.append(str(value))
            elif isinstance(value, basestring):
                result.append(value if is_ident(value) else '"%s"' % value.replace('"', '\\"'))
            else: assert False, value
        return '{%s}' % ','.join(result)
    def JSON_QUERY(builder, expr, path):
        path_sql, has_params, has_wildcards = builder.build_json_path(path)
        return '(', builder(expr), " #> ", path_sql, ')'
    json_value_type_mapping = {bool: 'boolean', int: 'integer', float: 'real'}
    def JSON_VALUE(builder, expr, path, type):
        if type is ormtypes.Json: return builder.JSON_QUERY(expr, path)
        path_sql, has_params, has_wildcards = builder.build_json_path(path)
        sql = '(', builder(expr), " #>> ", path_sql, ')'
        type_name = builder.json_value_type_mapping.get(type, 'text')
        return sql if type_name == 'text' else (sql, '::', type_name)
    def JSON_NONZERO(builder, expr):
        return 'coalesce(', builder(expr), ", 'null'::jsonb) NOT IN (" \
               "'null'::jsonb, 'false'::jsonb, '0'::jsonb, '\"\"'::jsonb, '[]'::jsonb, '{}'::jsonb)"
    def JSON_CONCAT(builder, left, right):
        return '(', builder(left), '||', builder(right), ')'
    def JSON_CONTAINS(builder, expr, path, key):
        return (builder.JSON_QUERY(expr, path) if path else builder(expr)), ' ? ', builder(key)
    def JSON_ARRAY_LENGTH(builder, value):
        return 'jsonb_array_length(', builder(value), ')'

class PGStrConverter(dbapiprovider.StrConverter):
    if PY2:
        def py2sql(converter, val):
            return val.encode('utf-8')
        def sql2py(converter, val):
            if isinstance(val, unicode): return val
            return val.decode('utf-8')

class PGIntConverter(dbapiprovider.IntConverter):
    signed_types = {None: 'INTEGER', 8: 'SMALLINT', 16: 'SMALLINT', 24: 'INTEGER', 32: 'INTEGER', 64: 'BIGINT'}
    unsigned_types = {None: 'INTEGER', 8: 'SMALLINT', 16: 'INTEGER', 24: 'INTEGER', 32: 'BIGINT'}

class PGRealConverter(dbapiprovider.RealConverter):
    def sql_type(converter):
        return 'DOUBLE PRECISION'

class PGBlobConverter(dbapiprovider.BlobConverter):
    def sql_type(converter):
        return 'BYTEA'

class PGTimedeltaConverter(dbapiprovider.TimedeltaConverter):
    sql_type_name = 'INTERVAL DAY TO SECOND'

class PGDatetimeConverter(dbapiprovider.DatetimeConverter):
    sql_type_name = 'TIMESTAMP'

class PGUuidConverter(dbapiprovider.UuidConverter):
    def py2sql(converter, val):
        return val

class PGJsonConverter(dbapiprovider.JsonConverter):
    def sql_type(self):
        return "JSONB"

class PGPool(Pool):
    def _connect(pool):
        pool.con = pool.dbapi_module.connect(*pool.args, **pool.kwargs)
        if 'client_encoding' not in pool.kwargs:
            pool.con.set_client_encoding('UTF8')
    def release(pool, con):
        assert con is pool.con
        try:
            con.rollback()
            con.autocommit = True
            cursor = con.cursor()
            cursor.execute('DISCARD ALL')
            con.autocommit = False
        except:
            pool.drop(con)
            raise

class PGProvider(DBAPIProvider):
    dialect = 'PostgreSQL'
    paramstyle = 'pyformat'
    max_name_len = 63
    index_if_not_exists_syntax = False

    dbapi_module = psycopg2
    dbschema_cls = PGSchema
    translator_cls = PGTranslator
    sqlbuilder_cls = PGSQLBuilder

    default_schema_name = 'public'

    fk_types = { 'SERIAL' : 'INTEGER', 'BIGSERIAL' : 'BIGINT' }

    def normalize_name(provider, name):
        return name[:provider.max_name_len].lower()

    @wrap_dbapi_exceptions
    def inspect_connection(provider, connection):
        provider.server_version = connection.server_version
        provider.table_if_not_exists_syntax = provider.server_version >= 90100

    def should_reconnect(provider, exc):
        return isinstance(exc, psycopg2.OperationalError) \
               and exc.pgcode is exc.pgerror is exc.cursor is None

    def get_pool(provider, *args, **kwargs):
        return PGPool(provider.dbapi_module, *args, **kwargs)

    @wrap_dbapi_exceptions
    def set_transaction_mode(provider, connection, cache):
        assert not cache.in_transaction
        if cache.immediate and connection.autocommit:
            connection.autocommit = False
            if core.debug: log_orm('SWITCH FROM AUTOCOMMIT TO TRANSACTION MODE')
        db_session = cache.db_session
        if db_session is not None and db_session.serializable:
            cursor = connection.cursor()
            sql = 'SET TRANSACTION ISOLATION LEVEL SERIALIZABLE'
            if core.debug: log_orm(sql)
            cursor.execute(sql)
        elif not cache.immediate and not connection.autocommit:
            connection.autocommit = True
            if core.debug: log_orm('SWITCH TO AUTOCOMMIT MODE')
        if db_session is not None and (db_session.serializable or db_session.ddl):
            cache.in_transaction = True

    @wrap_dbapi_exceptions
    def execute(provider, cursor, sql, arguments=None, returning_id=False):
        if PY2 and isinstance(sql, unicode): sql = sql.encode('utf8')
        if type(arguments) is list:
            assert arguments and not returning_id
            cursor.executemany(sql, arguments)
        else:
            if arguments is None: cursor.execute(sql)
            else: cursor.execute(sql, arguments)
            if returning_id: return cursor.fetchone()[0]

    def table_exists(provider, connection, table_name, case_sensitive=True):
        schema_name, table_name = provider.split_table_name(table_name)
        cursor = connection.cursor()
        if case_sensitive: sql = 'SELECT tablename FROM pg_catalog.pg_tables ' \
                                 'WHERE schemaname = %s AND tablename = %s'
        else: sql = 'SELECT tablename FROM pg_catalog.pg_tables ' \
                    'WHERE schemaname = %s AND lower(tablename) = lower(%s)'
        cursor.execute(sql, (schema_name, table_name))
        row = cursor.fetchone()
        return row[0] if row is not None else None

    def index_exists(provider, connection, table_name, index_name, case_sensitive=True):
        schema_name, table_name = provider.split_table_name(table_name)
        cursor = connection.cursor()
        if case_sensitive: sql = 'SELECT indexname FROM pg_catalog.pg_indexes ' \
                                'WHERE schemaname = %s AND tablename = %s AND indexname = %s'
        else: sql = 'SELECT indexname FROM pg_catalog.pg_indexes ' \
                    'WHERE schemaname = %s AND tablename = %s AND lower(indexname) = lower(%s)'
        cursor.execute(sql, [ schema_name, table_name, index_name ])
        row = cursor.fetchone()
        return row

    def fk_exists(provider, connection, table_name, fk_name, case_sensitive=True):
        schema_name, table_name = provider.split_table_name(table_name)
        if case_sensitive: sql = 'SELECT con.conname FROM pg_class cls ' \
                                 'JOIN pg_namespace ns ON cls.relnamespace = ns.oid ' \
                                 'JOIN pg_constraint con ON con.conrelid = cls.oid ' \
                                 'WHERE ns.nspname = %s AND cls.relname = %s ' \
                                 "AND con.contype = 'f' AND con.conname = %s"
        else: sql = 'SELECT con.conname FROM pg_class cls ' \
                    'JOIN pg_namespace ns ON cls.relnamespace = ns.oid ' \
                    'JOIN pg_constraint con ON con.conrelid = cls.oid ' \
                    'WHERE ns.nspname = %s AND cls.relname = %s ' \
                    "AND con.contype = 'f' AND lower(con.conname) = lower(%s)"
        cursor = connection.cursor()
        cursor.execute(sql, [ schema_name, table_name, fk_name ])
        row = cursor.fetchone()
        return row

    def table_has_data(provider, connection, table_name):
        table_name = provider.quote_name(table_name)
        cursor = connection.cursor()
        cursor.execute('SELECT 1 FROM %s LIMIT 1' % table_name)
        return cursor.fetchone() is not None

    def drop_table(provider, connection, table_name):
        table_name = provider.quote_name(table_name)
        cursor = connection.cursor()
        sql = 'DROP TABLE %s CASCADE' % table_name
        cursor.execute(sql)

    converter_classes = [
        (NoneType, dbapiprovider.NoneConverter),
        (bool, dbapiprovider.BoolConverter),
        (basestring, PGStrConverter),
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
