import os.path
import sqlite3 as sqlite
from threading import Lock, Thread
from Queue import Queue
from decimal import Decimal
from datetime import datetime, date
from time import strptime
from uuid import UUID

from pony.orm import dbschema, sqltranslation, dbapiprovider
from pony.orm.sqlbuilding import SQLBuilder, join
from pony.orm.dbapiprovider import DBAPIProvider, Pool
from pony.utils import localbase, datetime2timestamp, timestamp2datetime, decorator, absolutize_path, throw

class SQLiteForeignKey(dbschema.ForeignKey):
    def get_create_command(foreign_key):
        return None

class SQLiteSchema(dbschema.DBSchema):
    dialect = 'SQLite'
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
        if filename == ':memory:': return MemPool()
        else:
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

provider_cls = SQLiteProvider

class SQLitePool(Pool):
    def __init__(pool, filename, create_db): # called separately in each thread
        pool.filename = filename
        pool.create_db = create_db
        pool.con = None
    def connect(pool):
        con = pool.con
        if con is not None: return con
        filename = pool.filename
        if not pool.create_db and not os.path.exists(filename):
            throw(IOError, "Database file is not found: %r" % filename)
        pool.con = con = sqlite.connect(filename)
        _init_connection(con)
        return con

mem_connect_lock = Lock()

class MemPool(object):
    def __init__(mempool):
        mempool.con = MemoryConnectionWrapper()
        mem_connect_lock.acquire()
        try:
            if mempool.con is None:
                mempool.con = MemoryConnectionWrapper()
        finally: mem_connect_lock.release()
    def connect(mempool):
        return mempool.con
    def release(mempool, con):
        assert con is mempool.con
        con.rollback()
    def drop(mempool, con):
        assert con is mempool.con
        con.rollback()
    def __del__(mempool):
        con = mempool.con
        if con is None: con.close()

def _text_factory(s):
    return s.decode('utf8', 'replace')

def _init_connection(con):
    con.text_factory = _text_factory
    con.create_function('power', 2, pow)

mem_queue = Queue()

class Local(localbase):
    def __init__(local):
        local.lock = Lock()
        local.lock.acquire()

local = Local()

@decorator
def in_dedicated_thread(func, *args, **kwargs):
    result_holder = []
    mem_queue.put((local.lock, func, args, kwargs, result_holder))
    local.lock.acquire()
    result = result_holder[0]
    if isinstance(result, Exception):
        try: raise result
        finally: del result, result_holder
    if isinstance(result, sqlite.Cursor): result = MemoryCursorWrapper(result)
    return result

def make_wrapper_method(method_name):
    @in_dedicated_thread
    def wrapper_method(wrapper, *args, **kwargs):
        method = getattr(wrapper.obj, method_name)
        return method(*args, **kwargs)
    wrapper_method.__name__ = method_name
    return wrapper_method

def make_wrapper_property(attr):
    @in_dedicated_thread
    def getter(wrapper):
        return getattr(wrapper.obj, attr)
    @in_dedicated_thread
    def setter(wrapper, value):
        setattr(wrapper.obj, attr, value)
    return property(getter, setter)

class MemoryConnectionWrapper(object):
    @in_dedicated_thread
    def __init__(wrapper):
        con = sqlite.connect(':memory:')
        _init_connection(con)
        wrapper.obj = con
    def interrupt(wrapper):
        wrapper.obj.interrupt()
    @in_dedicated_thread
    def iterdump(wrapper, *args, **kwargs):
        return iter(list(wrapper.obj.iterdump()))

sqlite_con_methods = '''cursor commit rollback close execute executemany executescript
                        create_function create_aggregate create_collation
                        set_authorizer set_process_handler'''.split()

for m in sqlite_con_methods:
    setattr(MemoryConnectionWrapper, m, make_wrapper_method(m))

sqlite_con_properties = 'isolation_level row_factory text_factory total_changes'.split()

for p in sqlite_con_properties:
    setattr(MemoryConnectionWrapper, m, make_wrapper_property(p))

class MemoryCursorWrapper(object):
    def __init__(wrapper, cur):
        wrapper.obj = cur
    def __iter__(wrapper):
        return wrapper

sqlite_cur_methods = '''execute executemany executescript fetchone fetchmany fetchall
                        next close setinputsize setoutputsize'''.split()

for m in sqlite_cur_methods:
    setattr(MemoryCursorWrapper, m, make_wrapper_method(m))

sqlite_cur_properties = 'rowcount lastrowid description arraysize'.split()

for p in sqlite_cur_properties:
    setattr(MemoryCursorWrapper, p, make_wrapper_property(p))

class SqliteMemoryDbThread(Thread):
    def __init__(mem_thread):
        Thread.__init__(mem_thread, name="SqliteMemoryDbThread")
        mem_thread.setDaemon(True)
    def run(mem_thread):
        while True:
            x = mem_queue.get()
            if x is None: break
            lock, func, args, kwargs, result_holder = x
            try: result = func(*args, **kwargs)
            except Exception, e: result_holder.append(e)
            else: result_holder.append(result)
            if lock is not None: lock.release()

mem_thread = SqliteMemoryDbThread()
mem_thread.start()
