import os.path
import sqlite3 as sqlite
from decimal import Decimal
from datetime import datetime, date
from random import random
from time import strptime
from uuid import UUID

from pony.orm import core, dbschema, sqltranslation, dbapiprovider
from pony.orm.core import log_orm
from pony.orm.sqlbuilding import SQLBuilder, join
from pony.orm.dbapiprovider import DBAPIProvider, Pool, wrap_dbapi_exceptions
from pony.utils import localbase, datetime2timestamp, timestamp2datetime, decorator, absolutize_path, throw

class SQLiteForeignKey(dbschema.ForeignKey):
    def get_create_command(foreign_key):
        assert False

class SQLiteSchema(dbschema.DBSchema):
    dialect = 'SQLite'
    named_foreign_keys = False
    fk_class = SQLiteForeignKey

class SQLiteTranslator(sqltranslation.SQLTranslator):
    dialect = 'SQLite'
    sqlite_version = sqlite.sqlite_version_info
    row_value_syntax = False

class SQLiteBuilder(SQLBuilder):
    dialect = 'SQLite'
    def SELECT_FOR_UPDATE(builder, nowait, *sections):
        assert not builder.indent and not nowait
        return builder.SELECT(*sections)
    def INSERT(builder, table_name, columns, values, returning=None):
        if not values: return 'INSERT INTO %s DEFAULT VALUES' % builder.quote_name(table_name)
        return SQLBuilder.INSERT(builder, table_name, columns, values, returning)
    def TODAY(builder):
        return "date('now', 'localtime')"
    def NOW(builder):
        return "datetime('now', 'localtime')"
    def YEAR(builder, expr):
        return 'cast(substr(', builder(expr), ', 1, 4) as integer)'
    def MONTH(builder, expr):
        return 'cast(substr(', builder(expr), ', 6, 2) as integer)'
    def DAY(builder, expr):
        return 'cast(substr(', builder(expr), ', 9, 2) as integer)'
    def HOUR(builder, expr):
        return 'cast(substr(', builder(expr), ', 12, 2) as integer)'
    def MINUTE(builder, expr):
        return 'cast(substr(', builder(expr), ', 15, 2) as integer)'
    def SECOND(builder, expr):
        return 'cast(substr(', builder(expr), ', 18, 2) as integer)'
    def MIN(builder, *args):
        if len(args) == 0: assert False
        elif len(args) == 1: fname = 'MIN'
        else: fname = 'min'
        return fname, '(',  join(', ', map(builder, args)), ')'
    def MAX(builder, *args):
        if len(args) == 0: assert False
        elif len(args) == 1: fname = 'MAX'
        else: fname = 'max'
        return fname, '(',  join(', ', map(builder, args)), ')'
    def RANDOM(builder):
        return 'rand()'  # return '(random() / 9223372036854775807.0 + 1.0) / 2.0'

class SQLiteStrConverter(dbapiprovider.StrConverter):
    def py2sql(converter, val):
        if converter.utf8: return val
        return val.decode(converter.encoding)

class SQLiteDecimalConverter(dbapiprovider.DecimalConverter):
    def sql2py(converter, val):
        try: val = Decimal(str(val))
        except: return val
        exp = converter.exp
        if exp is not None: val = val.quantize(exp)
        return val
    def py2sql(converter, val):
        if type(val) is not Decimal: val = Decimal(val)
        exp = converter.exp
        if exp is not None: val = val.quantize(exp)
        return str(val)

class SQLiteDateConverter(dbapiprovider.DateConverter):
    def sql2py(converter, val):
        try:
            time_tuple = strptime(val[:10], '%Y-%m-%d')
            return date(*time_tuple[:3])
        except: return val
    def py2sql(converter, val):
        return val.strftime('%Y-%m-%d')

class SQLiteDatetimeConverter(dbapiprovider.DatetimeConverter):
    def sql2py(converter, val):
        try: return timestamp2datetime(val)
        except: return val
    def py2sql(converter, val):
        return datetime2timestamp(val)

class SQLiteProvider(DBAPIProvider):
    dialect = 'SQLite'
    max_name_len = 1024
    select_for_update_nowait_syntax = False

    dbapi_module = sqlite
    dbschema_cls = SQLiteSchema
    translator_cls = SQLiteTranslator
    sqlbuilder_cls = SQLiteBuilder

    name_before_table = 'db_name'

    server_version = sqlite.sqlite_version_info

    converter_classes = [
        (bool, dbapiprovider.BoolConverter),
        (unicode, dbapiprovider.UnicodeConverter),
        (str, SQLiteStrConverter),
        ((int, long), dbapiprovider.IntConverter),
        (float, dbapiprovider.RealConverter),
        (Decimal, SQLiteDecimalConverter),
        (buffer, dbapiprovider.BlobConverter),
        (datetime, SQLiteDatetimeConverter),
        (date, SQLiteDateConverter),
        (UUID, dbapiprovider.UuidConverter),
    ]

    @wrap_dbapi_exceptions
    def set_transaction_mode(provider, connection, cache):
        assert not cache.in_transaction
        cursor = connection.cursor()

        db_session = cache.db_session
        if db_session is not None and db_session.ddl:
            cursor.execute('PRAGMA foreign_keys')
            fk = cursor.fetchone()
            if fk is not None: fk = fk[0]
            if fk:
                sql = 'PRAGMA foreign_keys = false'
                if core.debug: log_orm(sql)
                cursor.execute(sql)
            cache.saved_fk_state = bool(fk)
            cache.in_transaction = True

        if cache.immediate:
            sql = 'BEGIN IMMEDIATE TRANSACTION'
            if core.debug: log_orm(sql)
            cursor.execute(sql)
            cache.in_transaction = True
        elif core.debug: log_orm('SWITCH TO AUTOCOMMIT MODE')

    @wrap_dbapi_exceptions
    def release(provider, connection, cache=None):
        if cache is not None:
            db_session = cache.db_session
            if db_session is not None and db_session.ddl and cache.saved_fk_state:
                try:
                    cursor = connection.cursor()
                    sql = 'PRAGMA foreign_keys = true'
                    if core.debug: log_orm(sql)
                    cursor.execute(sql)
                except:
                    provider.pool.drop(connection)
                    raise
        DBAPIProvider.release(provider, connection, cache)

    def get_pool(provider, filename, create_db=False):
        if filename != ':memory:':
            # When relative filename is specified, it is considered
            # not relative to cwd, but to user module where
            # Database instance is created

            # the list of frames:
            # 6 - user code: db = Database(...)
            # 5 - cut_traceback decorator wrapper
            # 4 - cut_traceback decorator
            # 3 - pony.orm.Database.__init__() / .bind()
            # 2 - pony.orm.Database._bind()
            # 1 - pony.dbapiprovider.DBAPIProvider.__init__()
            # 0 - pony.dbproviders.sqlite.get_pool()
            filename = absolutize_path(filename, frame_depth=6)
        return SQLitePool(filename, create_db)

    def table_exists(provider, connection, table_name, case_sensitive=True):
        return provider._exists(connection, table_name, None, case_sensitive)

    def index_exists(provider, connection, table_name, index_name, case_sensitive=True):
        return provider._exists(connection, table_name, index_name, case_sensitive)

    def _exists(provider, connection, table_name, index_name=None, case_sensitive=True):
        db_name, table_name = provider.split_table_name(table_name)

        if db_name is None: catalog_name = 'sqlite_master'
        else: catalog_name = (db_name, 'sqlite_master')
        catalog_name = provider.quote_name(catalog_name)

        cursor = connection.cursor()
        if index_name is not None:
            sql = "SELECT name FROM %s WHERE type='index' AND name=?" % catalog_name
            if not case_sensitive: sql += ' COLLATE NOCASE'
            cursor.execute(sql, [ index_name ])
        else:
            sql = "SELECT name FROM %s WHERE type='table' AND name=?" % catalog_name
            if not case_sensitive: sql += ' COLLATE NOCASE'
            cursor.execute(sql, [ table_name ])
        row = cursor.fetchone()
        return row[0] if row is not None else None

    def fk_exists(provider, connection, table_name, fk_name):
        assert False

provider_cls = SQLiteProvider

def _text_factory(s):
    return s.decode('utf8', 'replace')

class SQLitePool(Pool):
    def __init__(pool, filename, create_db): # called separately in each thread
        pool.filename = filename
        pool.create_db = create_db
        pool.con = None
    def connect(pool):
        con = pool.con
        if con is not None:
            if core.debug: core.log_orm('GET CONNECTION FROM THE LOCAL POOL')
            return con
        filename = pool.filename
        if filename != ':memory:' and not pool.create_db and not os.path.exists(filename):
            throw(IOError, "Database file is not found: %r" % filename)
        if core.debug: log_orm('GET NEW CONNECTION')
        pool.con = con = sqlite.connect(filename, isolation_level=None)
        con.text_factory = _text_factory
        con.create_function('power', 2, pow)
        con.create_function('rand', 0, random)
        if sqlite.sqlite_version_info >= (3, 6, 19):
            con.execute('PRAGMA foreign_keys = true')
        return con
    def disconnect(pool):
        if pool.filename != ':memory:':
            Pool.disconnect(pool)
    def drop(pool, con):
        if pool.filename != ':memory:':
            Pool.drop(pool, con)
        else:
            con.rollback()
