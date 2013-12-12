import os.path
import sqlite3 as sqlite
from decimal import Decimal
from datetime import datetime, date
from random import random
from time import strptime
from uuid import UUID

from pony.orm import dbschema, sqltranslation, dbapiprovider
from pony.orm.sqlbuilding import SQLBuilder, join
from pony.orm.dbapiprovider import DBAPIProvider, Pool
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

    def get_pool(provider, filename, create_db=False):
        if filename != ':memory:':
            # When relative filename is specified, it is considered
            # not relative to cwd, but to user module where
            # Database instance is created

            # the list of frames:
            # 5 - user code: db = Database(...)
            # 4 - cut_traceback decorator wrapper
            # 3 - cut_traceback decorator
            # 2 - pony.orm.Database.__init__()
            # 1 - pony.dbapiprovider.DBAPIProvider.__init__()
            # 0 - pony.dbproviders.sqlite.get_pool()
            filename = absolutize_path(filename, frame_depth=5)
        return SQLitePool(filename, create_db)

    def table_exists(provider, connection, table_name):
        return provider._exists(connection, table_name)

    def index_exists(provider, connection, table_name, index_name):
        return provider._exists(connection, table_name, index_name)

    def _exists(provider, connection, table_name, index_name=None):
        db_name, table_name = provider.split_table_name(table_name)

        if db_name is None: catalog_name = 'sqlite_master'
        else: catalog_name = (db_name, 'sqlite_master')
        catalog_name = provider.quote_name(catalog_name)

        cursor = connection.cursor()
        if index_name is not None:
            sql = "SELECT 1 FROM %s WHERE type='index' AND name=?" % catalog_name
            cursor.execute(sql, [ index_name ])
        else:
            sql = "SELECT 1 FROM %s WHERE type='table' AND name=?" % catalog_name
            cursor.execute(sql, [ table_name ])
        return cursor.fetchone() is not None

    def fk_exists(provider, connection, table_name, fk_name):
        assert False

    def disable_fk_checks_if_necessary(provider, connection):
        cursor = connection.cursor()
        cursor.execute('PRAGMA foreign_keys')
        fk = cursor.fetchone()
        if fk is not None:
            fk = fk[0]
            if fk: cursor.execute('PRAGMA foreign_keys = false')
        return bool(fk)

    def enable_fk_checks_if_necessary(provider, connection, fk):
        assert type(fk) is bool, fk
        if fk:
            cursor = connection.cursor()
            cursor.execute('PRAGMA foreign_keys = true')

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
        if con is not None: return con
        filename = pool.filename
        if filename != ':memory:' and not pool.create_db and not os.path.exists(filename):
            throw(IOError, "Database file is not found: %r" % filename)
        pool.con = con = sqlite.connect(filename)
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
