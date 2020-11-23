from __future__ import absolute_import
from pony.py23compat import PY2, imap, basestring, buffer, int_types, unicode

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

from pony.orm import core, dbschema, dbapiprovider
from pony.orm.core import log_orm
from pony.orm.ormtypes import Json, TrackedArray
from pony.orm.sqltranslation import SQLTranslator, StringExprMonad
from pony.orm.sqlbuilding import SQLBuilder, Value, join, make_unary_func
from pony.orm.dbapiprovider import DBAPIProvider, Pool, wrap_dbapi_exceptions
from pony.utils import datetime2timestamp, timestamp2datetime, absolutize_path, localbase, throw, reraise, \
    cut_traceback_depth

class SqliteExtensionUnavailable(Exception):
    pass

NoneType = type(None)

class SQLiteForeignKey(dbschema.ForeignKey):
    def get_create_command(foreign_key):
        assert False  # pragma: no cover

class SQLiteSchema(dbschema.DBSchema):
    dialect = 'SQLite'
    named_foreign_keys = False
    fk_class = SQLiteForeignKey

def make_overriden_string_func(sqlop):
    def func(translator, monad):
        sql = monad.getsql()
        assert len(sql) == 1
        translator = monad.translator
        return StringExprMonad(monad.type, [ sqlop, sql[0] ])
    func.__name__ = sqlop
    return func


class SQLiteTranslator(SQLTranslator):
    dialect = 'SQLite'
    sqlite_version = sqlite.sqlite_version_info
    row_value_syntax = False
    rowid_support = True

    StringMixin_UPPER = make_overriden_string_func('PY_UPPER')
    StringMixin_LOWER = make_overriden_string_func('PY_LOWER')

class SQLiteValue(Value):
    __slots__ = []
    def __unicode__(self):
        value = self.value
        if isinstance(value, datetime):
            return self.quote_str(datetime2timestamp(value))
        if isinstance(value, date):
            return self.quote_str(str(value))
        if isinstance(value, timedelta):
            return repr(value.total_seconds() / (24 * 60 * 60))
        return Value.__unicode__(self)
    if not PY2: __str__ = __unicode__

class SQLiteBuilder(SQLBuilder):
    dialect = 'SQLite'
    least_func_name = 'min'
    greatest_func_name = 'max'
    value_class = SQLiteValue
    def __init__(builder, provider, ast):
        builder.json1_available = provider.json1_available
        SQLBuilder.__init__(builder, provider, ast)
    def SELECT_FOR_UPDATE(builder, nowait, skip_locked, *sections):
        assert not builder.indent
        return builder.SELECT(*sections)
    def INSERT(builder, table_name, columns, values, returning=None):
        if not values: return 'INSERT INTO %s DEFAULT VALUES' % builder.quote_name(table_name)
        return SQLBuilder.INSERT(builder, table_name, columns, values, returning)
    def STRING_SLICE(builder, expr, start, stop):
        if start is None:
            start = [ 'VALUE', None ]
        if stop is None:
            stop = [ 'VALUE', None ]
        return "py_string_slice(", builder(expr), ', ', builder(start), ', ', builder(stop), ")"
    def IN(builder, expr1, x):
        if not x:
            return '0 = 1'
        if len(x) >= 1 and x[0] == 'SELECT':
            return builder(expr1), ' IN ', builder(x)
        op = ' IN (VALUES ' if expr1[0] == 'ROW' else ' IN ('
        expr_list = [ builder(expr) for expr in x ]
        return builder(expr1), op, join(', ', expr_list), ')'
    def NOT_IN(builder, expr1, x):
        if not x:
            return '1 = 1'
        if len(x) >= 1 and x[0] == 'SELECT':
            return builder(expr1), ' NOT IN ', builder(x)
        op = ' NOT IN (VALUES ' if expr1[0] == 'ROW' else ' NOT IN ('
        expr_list = [ builder(expr) for expr in x ]
        return builder(expr1), op, join(', ', expr_list), ')'
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
        if delta[0] == 'VALUE' and isinstance(delta[1], timedelta):
            return builder.datetime_add('date', expr, delta[1])
        return 'datetime(julianday(', builder(expr), ') + ', builder(delta), ')'
    def DATE_SUB(builder, expr, delta):
        if delta[0] == 'VALUE' and isinstance(delta[1], timedelta):
            return builder.datetime_add('date', expr, -delta[1])
        return 'datetime(julianday(', builder(expr), ') - ', builder(delta), ')'
    def DATE_DIFF(builder, expr1, expr2):
        return 'julianday(', builder(expr1), ') - julianday(', builder(expr2), ')'
    def DATETIME_ADD(builder, expr, delta):
        if delta[0] == 'VALUE' and isinstance(delta[1], timedelta):
            return builder.datetime_add('datetime', expr, delta[1])
        return 'datetime(julianday(', builder(expr), ') + ', builder(delta), ')'
    def DATETIME_SUB(builder, expr, delta):
        if delta[0] == 'VALUE' and isinstance(delta[1], timedelta):
            return builder.datetime_add('datetime', expr, -delta[1])
        return 'datetime(julianday(', builder(expr), ') - ', builder(delta), ')'
    def DATETIME_DIFF(builder, expr1, expr2):
        return 'julianday(', builder(expr1), ') - julianday(', builder(expr2), ')'
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
    def ARRAY_INDEX(builder, col, index):
        return 'py_array_index(', builder(col), ', ', builder(index), ')'
    def ARRAY_CONTAINS(builder, key, not_in, col):
        return ('NOT ' if not_in else ''), 'py_array_contains(', builder(col), ', ', builder(key), ')'
    def ARRAY_SUBSET(builder, array1, not_in, array2):
        return ('NOT ' if not_in else ''), 'py_array_subset(', builder(array2), ', ', builder(array1), ')'
    def ARRAY_LENGTH(builder, array):
        return 'py_array_length(', builder(array), ')'
    def ARRAY_SLICE(builder, array, start, stop):
        return 'py_array_slice(', builder(array), ', ', \
               builder(start) if start else 'null', ',',\
               builder(stop) if stop else 'null', ')'
    def MAKE_ARRAY(builder, *items):
        return 'py_make_array(', join(', ', (builder(item) for item in items)), ')'

class SQLiteIntConverter(dbapiprovider.IntConverter):
    def sql_type(converter):
        attr = converter.attr
        if attr is not None and attr.auto: return 'INTEGER'  # Only this type can have AUTOINCREMENT option
        return dbapiprovider.IntConverter.sql_type(converter)

class SQLiteDecimalConverter(dbapiprovider.DecimalConverter):
    inf = Decimal('infinity')
    neg_inf = Decimal('-infinity')
    NaN = Decimal('NaN')
    def sql2py(converter, val):
        try: val = Decimal(str(val))
        except: return val
        exp = converter.exp
        if exp is not None: val = val.quantize(exp)
        return val
    def py2sql(converter, val):
        if type(val) is not Decimal: val = Decimal(val)
        exp = converter.exp
        if exp is not None:
            if val in (converter.inf, converter.neg_inf, converter.NaN):
                throw(ValueError, 'Cannot store %s Decimal value in database' % val)
            val = val.quantize(exp)
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

def dumps(items):
    return json.dumps(items, **SQLiteJsonConverter.json_kwargs)

class SQLiteArrayConverter(dbapiprovider.ArrayConverter):
    array_types = {
        int: ('int', SQLiteIntConverter),
        unicode: ('text', dbapiprovider.StrConverter),
        float: ('real', dbapiprovider.RealConverter)
    }

    def dbval2val(converter, dbval, obj=None):
        if not dbval: return None
        items = json.loads(dbval)
        if obj is None:
            return items
        return TrackedArray(obj, converter.attr, items)

    def val2dbval(converter, val, obj=None):
        return dumps(val)

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

    dbapi_module = sqlite
    dbschema_cls = SQLiteSchema
    translator_cls = SQLiteTranslator
    sqlbuilder_cls = SQLiteBuilder
    array_converter_cls = SQLiteArrayConverter

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
        provider.pre_transaction_lock = Lock()
        provider.transaction_lock = Lock()

    @wrap_dbapi_exceptions
    def inspect_connection(provider, conn):
        DBAPIProvider.inspect_connection(provider, conn)
        provider.json1_available = provider.check_json1(conn)

    def restore_exception(provider):
        if provider.local_exceptions.exc_info is not None:
            try: reraise(*provider.local_exceptions.exc_info)
            finally: provider.local_exceptions.exc_info = None

    def acquire_lock(provider):
        provider.pre_transaction_lock.acquire()
        try:
            provider.transaction_lock.acquire()
        finally:
            provider.pre_transaction_lock.release()

    def release_lock(provider):
        provider.transaction_lock.release()

    @wrap_dbapi_exceptions
    def set_transaction_mode(provider, connection, cache):
        assert not cache.in_transaction
        if cache.immediate:
            provider.acquire_lock()
        try:
            cursor = connection.cursor()

            db_session = cache.db_session
            if db_session is not None and db_session.ddl:
                cursor.execute('PRAGMA foreign_keys')
                fk = cursor.fetchone()
                if fk is not None: fk = fk[0]
                if fk:
                    sql = 'PRAGMA foreign_keys = false'
                    if core.local.debug: log_orm(sql)
                    cursor.execute(sql)
                cache.saved_fk_state = bool(fk)
                assert cache.immediate

            if cache.immediate:
                sql = 'BEGIN IMMEDIATE TRANSACTION'
                if core.local.debug: log_orm(sql)
                cursor.execute(sql)
                cache.in_transaction = True
            elif core.local.debug: log_orm('SWITCH TO AUTOCOMMIT MODE')
        finally:
            if cache.immediate and not cache.in_transaction:
                provider.release_lock()

    def commit(provider, connection, cache=None):
        in_transaction = cache is not None and cache.in_transaction
        try:
            DBAPIProvider.commit(provider, connection, cache)
        finally:
            if in_transaction:
                cache.in_transaction = False
                provider.release_lock()

    def rollback(provider, connection, cache=None):
        in_transaction = cache is not None and cache.in_transaction
        try:
            DBAPIProvider.rollback(provider, connection, cache)
        finally:
            if in_transaction:
                cache.in_transaction = False
                provider.release_lock()

    def drop(provider, connection, cache=None):
        in_transaction = cache is not None and cache.in_transaction
        try:
            DBAPIProvider.drop(provider, connection, cache)
        finally:
            if in_transaction:
                cache.in_transaction = False
                provider.release_lock()

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
        assert False  # pragma: no cover

    def check_json1(provider, connection):
        cursor = connection.cursor()
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
    if value is None:
        return None
    assert value.startswith('[null,'), value
    return value[6:-1]

path_cache = {}

json_path_re = re.compile(r'\[(-?\d+)\]|\.(?:(\w+)|"([^"]*)")', re.UNICODE)

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

def wrap_array_func(func):
    @wraps(func)
    def new_func(array, *args):
        if array is None:
            return None
        array = json.loads(array)
        return func(array, *args)
    return new_func

@wrap_array_func
def py_array_index(array, index):
    try:
        return array[index]
    except IndexError:
        return None

@wrap_array_func
def py_array_contains(array, item):
    return item in array

@wrap_array_func
def py_array_subset(array, items):
    if items is None: return None
    items = json.loads(items)
    return set(items).issubset(set(array))

@wrap_array_func
def py_array_length(array):
    return len(array)

@wrap_array_func
def py_array_slice(array, start, stop):
    return dumps(array[start:stop])

def py_make_array(*items):
    return dumps(items)

def py_string_slice(s, start, end):
    if s is None:
        return None
    if isinstance(start, basestring):
        start = int(start)
    if isinstance(end, basestring):
        end = int(end)
    return s[start:end]

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

        create_function('py_array_index', 2, py_array_index)
        create_function('py_array_contains', 2, py_array_contains)
        create_function('py_array_subset', 2, py_array_subset)
        create_function('py_array_length', 1, py_array_length)
        create_function('py_array_slice', 3, py_array_slice)
        create_function('py_make_array', -1, py_make_array)

        create_function('py_string_slice', 3, py_string_slice)

        if sqlite.sqlite_version_info >= (3, 6, 19):
            con.execute('PRAGMA foreign_keys = true')

        con.execute('PRAGMA case_sensitive_like = true')
    def disconnect(pool):
        if pool.filename != ':memory:':
            Pool.disconnect(pool)
    def drop(pool, con):
        if pool.filename != ':memory:':
            Pool.drop(pool, con)
        else:
            con.rollback()
