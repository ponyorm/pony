from __future__ import absolute_import
from pony.py23compat import imap, basestring, buffer, int_types, unicode

import os.path, sys, re, json
import sqlite3 as sqlite
from decimal import Decimal
from datetime import datetime, date, time, timedelta
from random import random
from time import strptime
from threading import Lock
from uuid import UUID
from binascii import hexlify
from functools import wraps

from pony import orm
from pony.orm import core, dbschema, sqltranslation, dbapiprovider
from pony.orm.core import log_orm
from pony.orm.ormtypes import Json
from pony.orm.sqlbuilding import SQLBuilder, join, make_unary_func
from pony.orm.dbapiprovider import DBAPIProvider, Pool, wrap_dbapi_exceptions
from pony.utils import datetime2timestamp, timestamp2datetime, absolutize_path, localbase, throw, reraise, \
    cut_traceback_depth

from pony.migrate.operations import Op, OperationBatch


class SqliteExtensionUnavailable(Exception):
    pass

NoneType = type(None)

class SQLiteForeignKey(dbschema.ForeignKey):
    def get_create_command(fk):
        assert False  # pragma: no cover

class SQLiteTable(dbschema.Table):
    def get_rename_ops(table):
        ops = []
        ops.append(Op('PRAGMA foreign_keys = true', obj=None, type='pragma_foreign_keys'))
        ops.extend(dbschema.Table.get_rename_ops(table))
        ops.append(Op('PRAGMA foreign_keys = false', obj=None, type='pragma_foreign_keys'))
        return [ OperationBatch(ops, type='rename') ]

    def get_alter_ops(table):
        batch = OperationBatch(type='rename')
        index_ops = []

        original_table_name = table.name
        tmp_name = table.name + '__new'

        table.name = tmp_name
        batch.extend(table.get_create_ops())
        for index in table.prev.indexes.values():
            if index.is_pk or index.is_unique and len(index.col_names) > 1:
                continue
            sql = index.get_create_command()
            index_ops.append(Op(sql, obj=index, type='create'))
        table.name = original_table_name

        quote_name = table.schema.provider.quote_name

        new_col_names = []
        prev_values = []
        for c in table.column_list:
            new_col_names.append(quote_name(c.name))
            if c.prev is not None:
                prev_values.append(quote_name(c.prev.name))
            else:
                prev_values.append('NULL')

        insert_sql = 'INSERT INTO {} ({}) SELECT {} FROM {}'.format(
            quote_name(tmp_name), ', '.join(new_col_names), ', '.join(prev_values), quote_name(table.prev.name))
        batch.append(Op(insert_sql, obj=table, type='insert'))

        drop_sql = 'DROP TABLE {}'.format(quote_name(table.prev.name))
        batch.append(Op(drop_sql, obj=table, type='drop'))

        rename_sql = 'ALTER TABLE {} RENAME TO {}'.format(tmp_name, quote_name(table.prev.name))
        batch.append(Op(rename_sql, obj=table, type='rename'))

        batch.extend(index_ops)
        return [ batch ]

class SQLiteIndex(dbschema.DBIndex):
    def can_be_renamed(index):
        return False

class SQLiteColumn(dbschema.Column):
    def get_rename_ops(column):
        throw(NotImplementedError)

    def db_rename(column, cursor, table_name):
        throw(NotImplementedError)

class SQLiteSchema(dbschema.DBSchema):
    named_foreign_keys = False
    table_class = SQLiteTable
    column_class = SQLiteColumn
    index_class = SQLiteIndex
    fk_class = SQLiteForeignKey

def make_overriden_string_func(sqlop):
    def func(translator, monad):
        sql = monad.getsql()
        assert len(sql) == 1
        translator = monad.translator
        return translator.StringExprMonad(translator, monad.type, [ sqlop, sql[0] ])
    func.__name__ = sqlop
    return func


class SQLiteTranslator(sqltranslation.SQLTranslator):
    sqlite_version = sqlite.sqlite_version_info
    row_value_syntax = False
    rowid_support = True

    StringMixin_UPPER = make_overriden_string_func('PY_UPPER')
    StringMixin_LOWER = make_overriden_string_func('PY_LOWER')

class SQLiteBuilder(SQLBuilder):
    def __init__(builder, provider, ast):
        builder.json1_available = provider.json1_available
        SQLBuilder.__init__(builder, provider, ast)
    def ALTER_COLUMN_DEFAULT(builder, column):
        assert False
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
    def datetime_add(builder, funcname, expr, td):
        assert isinstance(td, timedelta)
        modifiers = []
        seconds = td.seconds + td.days * 24 * 3600
        sign = '+' if seconds > 0 else '-'
        seconds = abs(seconds)
        if seconds >= (24 * 3600):
            days = seconds // (24 * 3600)
            modifiers.append(", '%s%d days'" % (sign, days))
            seconds -= days * 24 * 3600
        if seconds >= 3600:
            hours = seconds // 3600
            modifiers.append(", '%s%d hours'" % (sign, hours))
            seconds -= hours * 3600
        if seconds >= 60:
            minutes = seconds // 60
            modifiers.append(", '%s%d minutes'" % (sign, minutes))
            seconds -= minutes * 60
        if seconds:
            modifiers.append(", '%s%d seconds'" % (sign, seconds))
        if not modifiers: return builder(expr)
        return funcname, '(', builder(expr), modifiers, ')'
    def DATE_ADD(builder, expr, delta):
        if isinstance(delta, timedelta):
            return builder.datetime_add('date', expr, delta)
        return 'datetime(julianday(', builder(expr), ') + ', builder(delta), ')'
    def DATE_SUB(builder, expr, delta):
        if isinstance(delta, timedelta):
            return builder.datetime_add('date', expr, -delta)
        return 'datetime(julianday(', builder(expr), ') - ', builder(delta), ')'
    def DATETIME_ADD(builder, expr, delta):
        if isinstance(delta, timedelta):
            return builder.datetime_add('datetime', expr, delta)
        return 'datetime(julianday(', builder(expr), ') + ', builder(delta), ')'
    def DATETIME_SUB(builder, expr, delta):
        if isinstance(delta, timedelta):
            return builder.datetime_add('datetime', expr, -delta)
        return 'datetime(julianday(', builder(expr), ') - ', builder(delta), ')'
    def MIN(builder, *args):
        if len(args) == 0: assert False  # pragma: no cover
        elif len(args) == 1: fname = 'MIN'
        else: fname = 'min'
        return fname, '(',  join(', ', imap(builder, args)), ')'
    def MAX(builder, *args):
        if len(args) == 0: assert False  # pragma: no cover
        elif len(args) == 1: fname = 'MAX'
        else: fname = 'max'
        return fname, '(',  join(', ', imap(builder, args)), ')'
    def RANDOM(builder):
        return 'rand()'  # return '(random() / 9223372036854775807.0 + 1.0) / 2.0'
    PY_UPPER = make_unary_func('py_upper')
    PY_LOWER = make_unary_func('py_lower')
    def FLOAT_EQ(builder, a, b):
        a, b = builder(a), builder(b)
        return 'abs(', a, ' - ', b, ') / coalesce(nullif(max(abs(', a, '), abs(', b, ')), 0), 1) <= 1e-14'
    def FLOAT_NE(builder, a, b):
        a, b = builder(a), builder(b)
        return 'abs(', a, ' - ', b, ') / coalesce(nullif(max(abs(', a, '), abs(', b, ')), 0), 1) > 1e-14'
    def JSON_QUERY(builder, expr, path):
        fname = 'json_extract' if builder.json1_available else 'py_json_extract'
        path_sql, has_params, has_wildcards = builder.build_json_path(path)
        return 'py_json_unwrap(', fname, '(', builder(expr), ', null, ', path_sql, '))'
    json_value_type_mapping = {unicode: 'text', bool: 'integer', int: 'integer', float: 'real'}
    def JSON_VALUE(builder, expr, path, type):
        func_name = 'json_extract' if builder.json1_available else 'py_json_extract'
        path_sql, has_params, has_wildcards = builder.build_json_path(path)
        type_name = builder.json_value_type_mapping.get(type)
        result = func_name, '(', builder(expr), ', ', path_sql, ')'
        if type_name is not None: result = 'CAST(', result, ' as ', type_name, ')'
        return result
    def JSON_NONZERO(builder, expr):
        return builder(expr), ''' NOT IN ('null', 'false', '0', '""', '[]', '{}')'''
    def JSON_ARRAY_LENGTH(builder, value):
        func_name = 'json_array_length' if builder.json1_available else 'py_json_array_length'
        return func_name, '(', builder(value), ')'
    def JSON_CONTAINS(builder, expr, path, key):
        path_sql, has_params, has_wildcards = builder.build_json_path(path)
        return 'py_json_contains(', builder(expr), ', ', path_sql, ',  ', builder(key), ')'

class SQLiteIntConverter(dbapiprovider.IntConverter):
    def sql_type(converter):
        attr = converter.attr
        if attr is not None and attr.auto: return 'INTEGER'  # Only this type can have AUTOINCREMENT option
        return dbapiprovider.IntConverter.sql_type(converter)

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

class SQLiteTimeConverter(dbapiprovider.TimeConverter):
    def sql2py(converter, val):
        try:
            if len(val) <= 8: dt = datetime.strptime(val, '%H:%M:%S')
            else: dt = datetime.strptime(val, '%H:%M:%S.%f')
            return dt.time()
        except: return val
    def py2sql(converter, val):
        return val.isoformat()

class SQLiteTimedeltaConverter(dbapiprovider.TimedeltaConverter):
    def sql2py(converter, val):
        return timedelta(days=val)
    def py2sql(converter, val):
        return val.days + (val.seconds + val.microseconds / 1000000.0) / 86400.0

class SQLiteDatetimeConverter(dbapiprovider.DatetimeConverter):
    def sql2py(converter, val):
        try: return timestamp2datetime(val)
        except: return val
    def py2sql(converter, val):
        return datetime2timestamp(val)

class SQLiteJsonConverter(dbapiprovider.JsonConverter):
    json_kwargs = {'separators': (',', ':'), 'sort_keys': True, 'ensure_ascii': False}


class LocalExceptions(localbase):
    def __init__(self):
        self.exc_info = None
        self.keep_traceback = False

local_exceptions = LocalExceptions()

def keep_exception(func):
    @wraps(func)
    def new_func(*args):
        local_exceptions.exc_info = None
        try:
            return func(*args)
        except Exception:
            local_exceptions.exc_info = sys.exc_info()
            if not local_exceptions.keep_traceback:
                local_exceptions.exc_info = local_exceptions.exc_info[:2] + (None,)
            raise
        finally:
            local_exceptions.keep_traceback = False
    return new_func


class SQLiteProvider(DBAPIProvider):
    dialect = 'SQLite'
    local_exceptions = local_exceptions
    max_name_len = 1024
    select_for_update_nowait_syntax = False

    dbapi_module = sqlite
    dbschema_cls = SQLiteSchema
    translator_cls = SQLiteTranslator
    sqlbuilder_cls = SQLiteBuilder

    name_before_table = 'db_name'

    server_version = sqlite.sqlite_version_info

    converter_classes = [
        (NoneType, dbapiprovider.NoneConverter),
        (bool, dbapiprovider.BoolConverter),
        (basestring, dbapiprovider.StrConverter),
        (int_types, SQLiteIntConverter),
        (float, dbapiprovider.RealConverter),
        (Decimal, SQLiteDecimalConverter),
        (datetime, SQLiteDatetimeConverter),
        (date, SQLiteDateConverter),
        (time, SQLiteTimeConverter),
        (timedelta, SQLiteTimedeltaConverter),
        (UUID, dbapiprovider.UuidConverter),
        (buffer, dbapiprovider.BlobConverter),
        (Json, SQLiteJsonConverter)
    ]

    def __init__(provider, *args, **kwargs):
        DBAPIProvider.__init__(provider, *args, **kwargs)
        provider.transaction_lock = Lock()

    @wrap_dbapi_exceptions
    def inspect_connection(provider, conn):
        provider.json1_available = provider.check_json1(conn)

    def restore_exception(provider):
        if provider.local_exceptions.exc_info is not None:
            try: reraise(*provider.local_exceptions.exc_info)
            finally: provider.local_exceptions.exc_info = None

    @wrap_dbapi_exceptions
    def set_transaction_mode(provider, connection, cache):
        assert not cache.in_transaction
        if cache.immediate:
            provider.transaction_lock.acquire()
        try:
            cursor = connection.cursor()

            db_session = cache.db_session
            if db_session is not None and db_session.ddl:
                cursor.execute('PRAGMA foreign_keys')
                row = cursor.fetchone()
                val = row and row[0]
                if val:
                    sql = 'PRAGMA foreign_keys = false'
                    if core.local.debug: log_orm(sql)
                    cursor.execute(sql)
                cache.saved_fk_state = bool(val)
                assert cache.immediate

            if cache.immediate:
                sql = 'BEGIN IMMEDIATE TRANSACTION'
                if core.local.debug: log_orm(sql)
                cursor.execute(sql)
                cache.in_transaction = True
            elif core.local.debug: log_orm('SWITCH TO AUTOCOMMIT MODE')
        finally:
            if cache.immediate and not cache.in_transaction:
                provider.transaction_lock.release()

    def commit(provider, connection, cache=None):
        in_transaction = cache is not None and cache.in_transaction
        try:
            DBAPIProvider.commit(provider, connection, cache)
        finally:
            if in_transaction:
                cache.in_transaction = False
                provider.transaction_lock.release()

    def rollback(provider, connection, cache=None):
        in_transaction = cache is not None and cache.in_transaction
        try:
            DBAPIProvider.rollback(provider, connection, cache)
        finally:
            if in_transaction:
                cache.in_transaction = False
                provider.transaction_lock.release()

    def drop(provider, connection, cache=None):
        in_transaction = cache is not None and cache.in_transaction
        try:
            DBAPIProvider.drop(provider, connection, cache)
        finally:
            if in_transaction:
                cache.in_transaction = False
                provider.transaction_lock.release()

    @wrap_dbapi_exceptions
    def release(provider, connection, cache=None):
        if cache is not None:
            db_session = cache.db_session
            if db_session is not None and db_session.ddl and cache.saved_fk_state:
                try:
                    cursor = connection.cursor()
                    sql = 'PRAGMA foreign_keys = true'
                    if core.local.debug: log_orm(sql)
                    cursor.execute(sql)
                except:
                    provider.pool.drop(connection)
                    raise
        DBAPIProvider.release(provider, connection, cache)

    def get_pool(provider, filename, create_db=False, **kwargs):
        if filename != ':memory:':
            # When relative filename is specified, it is considered
            # not relative to cwd, but to user module where
            # Database instance is created

            # the list of frames:
            # 7 - user code: db = Database(...)
            # 6 - cut_traceback decorator wrapper
            # 5 - cut_traceback decorator
            # 4 - pony.orm.Database.__init__() / .bind()
            # 3 - pony.orm.Database._bind()
            # 2 - pony.dbapiprovider.DBAPIProvider.__init__()
            # 1 - SQLiteProvider.__init__()
            # 0 - pony.dbproviders.sqlite.get_pool()
            filename = absolutize_path(filename, frame_depth=cut_traceback_depth+5)
        return SQLitePool(filename, create_db, **kwargs)

    def table_exists(provider, cursor, table_name, case_sensitive=True):
        return provider._exists(cursor, table_name, None, case_sensitive=False)

    def index_exists(provider, cursor, table_name, index_name, case_sensitive=True):
        return provider._exists(cursor, table_name, index_name, case_sensitive=False)

    def _exists(provider, cursor, table_name, index_name=None, case_sensitive=False):
        # Note on case-sensitivity: SQLite treats "Table1" and "table1" as the same name, even with quotes
        db_name, table_name = provider.split_table_name(table_name)

        if db_name is None: catalog_name = 'sqlite_master'
        else: catalog_name = (db_name, 'sqlite_master')
        catalog_name = provider.quote_name(catalog_name)

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

    def fk_exists(provider, cursor, table_name, fk_name):
        assert False  # pragma: no cover

    def check_json1(provider, cursor):
        sql = '''
            select json('{"this": "is", "a": ["test"]}')'''
        try:
            cursor.execute(sql)
            return True
        except sqlite.OperationalError:
            return False

provider_cls = SQLiteProvider

def _text_factory(s):
    return s.decode('utf8', 'replace')

def make_string_function(name, base_func):
    def func(value):
        if value is None:
            return None
        t = type(value)
        if t is not unicode:
            if t is buffer:
                value = hexlify(value).decode('ascii')
            else:
                value = unicode(value)
        result = base_func(value)
        return result
    func.__name__ = name
    return func

py_upper = make_string_function('py_upper', unicode.upper)
py_lower = make_string_function('py_lower', unicode.lower)

def py_json_unwrap(value):
    # [null,some-value] -> some-value
    assert value.startswith('[null,'), value
    return value[6:-1]

path_cache = {}

json_path_re = re.compile(r'\[(\d+)\]|\.(?:(\w+)|"([^"]*)")', re.UNICODE)

def _parse_path(path):
    if path in path_cache:
        return path_cache[path]
    keys = None
    if isinstance(path, basestring) and path.startswith('$'):
        keys = []
        pos = 1
        path_len = len(path)
        while pos < path_len:
            match = json_path_re.match(path, pos)
            if match is not None:
                g1, g2, g3 = match.groups()
                keys.append(int(g1) if g1 else g2 or g3)
                pos = match.end()
            else:
                keys = None
                break
        else: keys = tuple(keys)
    path_cache[path] = keys
    return keys

def _traverse(obj, keys):
    if keys is None: return None
    list_or_dict = (list, dict)
    for key in keys:
        if type(obj) not in list_or_dict: return None
        try: obj = obj[key]
        except (KeyError, IndexError): return None
    return obj

def _extract(expr, *paths):
    expr = json.loads(expr) if isinstance(expr, basestring) else expr
    result = []
    for path in paths:
        keys = _parse_path(path)
        result.append(_traverse(expr, keys))
    return result[0] if len(paths) == 1 else result

def py_json_extract(expr, *paths):
    result = _extract(expr, *paths)
    if type(result) in (list, dict):
        result = json.dumps(result, **SQLiteJsonConverter.json_kwargs)
    return result

def py_json_query(expr, path, with_wrapper):
    result = _extract(expr, path)
    if type(result) not in (list, dict):
        if not with_wrapper: return None
        result = [result]
    return json.dumps(result, **SQLiteJsonConverter.json_kwargs)

def py_json_value(expr, path):
    result = _extract(expr, path)
    return result if type(result) not in (list, dict) else None

def py_json_contains(expr, path, key):
    expr = json.loads(expr) if isinstance(expr, basestring) else expr
    keys = _parse_path(path)
    expr = _traverse(expr, keys)
    return type(expr) in (list, dict) and key in expr

def py_json_nonzero(expr, path):
    expr = json.loads(expr) if isinstance(expr, basestring) else expr
    keys = _parse_path(path)
    expr = _traverse(expr, keys)
    return bool(expr)

def py_json_array_length(expr, path=None):
    expr = json.loads(expr) if isinstance(expr, basestring) else expr
    if path:
        keys = _parse_path(path)
        expr = _traverse(expr, keys)
    return len(expr) if type(expr) is list else 0

class SQLitePool(Pool):
    def __init__(pool, filename, create_db, **kwargs): # called separately in each thread
        pool.filename = filename
        pool.create_db = create_db
        pool.kwargs = kwargs
        pool.con = None
    def _connect(pool):
        filename = pool.filename
        if filename != ':memory:' and not pool.create_db and not os.path.exists(filename):
            throw(IOError, "Database file is not found: %r" % filename)
        pool.con = con = sqlite.connect(filename, isolation_level=None, **pool.kwargs)
        con.text_factory = _text_factory

        def create_function(name, num_params, func):
            func = keep_exception(func)
            con.create_function(name, num_params, func)

        create_function('power', 2, pow)
        create_function('rand', 0, random)
        create_function('py_upper', 1, py_upper)
        create_function('py_lower', 1, py_lower)
        create_function('py_json_unwrap', 1, py_json_unwrap)
        create_function('py_json_extract', -1, py_json_extract)
        create_function('py_json_contains', 3, py_json_contains)
        create_function('py_json_nonzero', 2, py_json_nonzero)
        create_function('py_json_array_length', -1, py_json_array_length)

        if sqlite.sqlite_version_info >= (3, 6, 19):
            con.execute('PRAGMA foreign_keys = true')
    def disconnect(pool):
        if pool.filename != ':memory:':
            Pool.disconnect(pool)
    def drop(pool, con):
        if pool.filename != ':memory:':
            Pool.drop(pool, con)
        else:
            con.rollback()
