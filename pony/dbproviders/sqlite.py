from thread import get_ident
from threading import Lock, Thread
from Queue import Queue
import weakref
from decimal import Decimal, InvalidOperation
from datetime import datetime, date, time
from time import strptime
from os import path

from pony.thirdparty import sqlite
from pony.thirdparty.sqlite import (Warning, Error, InterfaceError, DatabaseError,
                                    DataError, OperationalError, IntegrityError, InternalError,
                                    ProgrammingError, NotSupportedError)

from pony import dbapiprovider
from pony.utils import localbase, datetime2timestamp, timestamp2datetime, simple_decorator

paramstyle = 'qmark'

def quote_name(connection, name):
    return dbapiprovider.quote_name(name)

def _text_factory(s):
    return s.decode('utf8', 'replace')

class Pool(localbase):
    def __init__(pool):
        pool.con = None

def get_pool(filename, create=False):
    if filename == ':memory:':
        return []
    else:
        return Pool()

def _init_connection(con):
    con.text_factory = _text_factory
    con.create_function("pow", 2, pow)

memdb_connect_lock = Lock()

def connect(pool, filename, create=False):
    if filename == ':memory:':
        assert type(pool) is list
        memdb_connect_lock.acquire()
        try:
            if pool:
                assert len(pool) == 1
                return pool[0]
            con = MemoryConnectionWrapper()
            pool.append(con)
        finally: memdb_connect_lock.release()
    else:
        assert isinstance(pool, Local)
        con = pool.con
        if con is not None: return con
        if not create and not path.exists(filename):
            raise IOError("Database file is not found: %r" % filename)
        pool.con = con = sqlite.connect(filename)
        _init_connection(con)
    return con

def release(connection):
    pass

class SQLiteBuilder(dbapiprovider.SQLBuilder):
    def POW(builder, expr1, expr2):
        return 'pow(', builder(expr1), ', ', builder(expr2), ')'

def ast2sql(con, ast):
    b = SQLiteBuilder(ast)
    return b.sql, b.adapter

def _get_converter_type_by_py_type(py_type):
    if issubclass(py_type, bool): return BoolConverter
    elif issubclass(py_type, unicode): return UnicodeConverter
    elif issubclass(py_type, str): return StrConverter
    elif issubclass(py_type, (int, long)): return IntConverter
    elif issubclass(py_type, float): return RealConverter
    elif issubclass(py_type, Decimal): return DecimalConverter
    elif issubclass(py_type, buffer): return BlobConverter
    elif issubclass(py_type, datetime): return DatetimeConverter
    elif issubclass(py_type, date): return DateConverter
    else: raise TypeError, py_type

def get_converter_by_py_type(py_type):
    return _get_converter_type_by_py_type(py_type)()

def get_converter_by_attr(attr):
    return _get_converter_type_by_py_type(attr.py_type)(attr)
    
def unexpected_args(attr, args):
    raise TypeError(
        'Unexpected positional argument%s for attribute %s: %r'
        % ((args > 1 and 's' or ''), attr, ', '.join(map(repr, args))))

class Converter(object):
    def __init__(converter, attr=None):
        converter.attr = attr
        if attr is None: return
        keyargs = attr.keyargs.copy()
        converter.init(keyargs)
        for option in keyargs: raise TypeError('Unknown option %r' % option)
    def init(converter, keyargs):
        pass
    def validate(converter, val):
        return val
    def py2sql(converter, val):
        return val
    def sql2py(converter, val):
        return val

class BoolConverter(Converter):
    def init(converter, keyargs):
        attr = converter.attr
        if attr and attr.args: unexpected_args(attr, attr.args)
    def validate(converter, val):
        return bool(val)
    def sql2py(converter, val):
        return bool(val)
    def sql_type(converter):
        return "BOOLEAN"

class BasestringConverter(Converter):
    def init(converter, keyargs):
        attr = converter.attr
        if attr and attr.args:
            if len(attr.args) > 1: unexpected_args(attr, attr.args[1:])
            max_len = attr.args[0]
            if not isinstance(max_len, (int, long)):
                raise TypeError('Max length argument must be int. Got: %r' % max_len)
            converter.max_len = max_len
        else: converter.max_len = None
    def validate(converter, val):
        max_len = converter.max_len
        val_len = len(val)
        if max_len and val_len > max_len:
            raise ValueError('Value for attribute %s is too long. Max length is %d, value length is %d'
                             % (converter.attr, max_len, val_len))
        if not val_len: raise ValueError('Empty strings are not allowed. Try using None instead')
        return val
    def sql_type(converter):
        if converter.max_len:
            return 'VARCHAR(%d)' % converter.max_len
        return 'TEXT'

class UnicodeConverter(BasestringConverter):
    def validate(converter, val):
        if val is None: pass
        elif isinstance(val, str): val = val.decode('ascii')
        elif not isinstance(val, unicode): raise TypeError(
            'Value type for attribute %s must be unicode. Got: %r' % (converter.attr, type(val)))
        return BasestringConverter.validate(converter, val)

class StrConverter(BasestringConverter):
    def __init__(converter, attr=None):
        converter.encoding = 'ascii'  # for the case when attr is None
        BasestringConverter.__init__(converter, attr)
    def init(converter, keyargs):
        BasestringConverter.init(converter, keyargs)
        converter.encoding = keyargs.pop('encoding', 'ascii')
    def validate(converter, val):
        if val is not None:
            if isinstance(val, str): pass
            elif isinstance(val, unicode): val = val.encode(converter.encoding)
            else: raise TypeError('Value type for attribute %s must be str in encoding %r. Got: %r'
                                  % (converter.attr, converter.encoding, type(val)))
        return BasestringConverter.validate(converter, val)
    def py2sql(converter, val):
        if converter.encoding == 'utf8': return val
        return val.decode(converter.encoding)
    def sql2py(converter, val):
        return val.encode(converter.encoding, 'replace')

class IntConverter(Converter):
    def init(converter, keyargs):
        attr = converter.attr
        if attr and attr.args: unexpected_args(attr, attr.args)
        min_val = keyargs.pop('min', None)
        if min_val is not None and not isinstance(min_val, (int, long)):
            raise TypeError("'min' argument for attribute %s must be int. Got: %r" % (attr, min_val))
        max_val = keyargs.pop('max', None)
        if max_val is not None and not isinstance(max_val, (int, long)):
            raise TypeError("'max' argument for attribute %s must be int. Got: %r" % (attr, max_val))
        converter.min_val = min_val
        converter.max_val = max_val
    def validate(converter, val):
        if not isinstance(val, (int, long)):
            raise TypeError('Value type for attribute %s must be int. Got: %r' % (converter.attr, type(val)))
        if converter.min_val and val < converter.min_val:
            raise ValueError('Value %r of attr %s is less than the minimum allowed value %r'
                             % (val, converter.attr, converter.min_val))
        if converter.max_val and val > converter.max_val:
            raise ValueError('Value %r of attr %s is greater than the maximum allowed value %r'
                             % (val, converter.attr, converter.max_val))
        return val
    def sql_type(converter):
        return 'INTEGER'

class RealConverter(Converter):
    def init(converter, keyargs):
        attr = converter.attr
        if attr and attr.args: unexpected_args(attr, attr.args)
        min_val = keyargs.pop('min', None)
        if min_val is not None:
            try: min_val = float(min_val)
            except ValueError:
                raise TypeError("Invalid value for 'min' argument for attribute %s: %r" % (attr, min_val))
        max_val = keyargs.pop('max', None)
        if max_val is not None:
            try: max_val = float(max_val)
            except ValueError:
                raise TypeError("Invalid value for 'max' argument for attribute %s: %r" % (attr, max_val))
        converter.min_val = min_val
        converter.max_val = max_val
    def validate(converter, val):
        try: val = float(val)
        except ValueError:
            raise TypeError('Invalid value for attribute %s: %r' % (converter.attr, val))
        if converter.min_val and val < converter.min_val:
            raise ValueError('Value %r of attr %s is less than the minimum allowed value %r'
                             % (val, converter.attr, converter.min_val))
        if converter.max_val and val > converter.max_val:
            raise ValueError('Value %r of attr %s is greater than the maximum allowed value %r'
                             % (val, converter.attr, converter.max_val))
        return val
    def sql_type(converter):
        return 'REAL'

class DecimalConverter(Converter):
    def __init__(converter, attr=None):
        converter.exp = None  # for the case when attr is None
        Converter.__init__(converter, attr)
    def init(converter, keyargs):
        attr = converter.attr
        if len(attr.args) != 2: raise TypeError(
            'Decimal attribute %s receives exactly two positional parameters: scale and precision. Got: %s'
            % (attr, attr.args or 'None'))

        scale, precision = attr.args
        if not isinstance(scale, (int, long)):
            raise TypeError("'scale' positional argument for attribute %s must be int. Got: %r" % (attr, scale))
        if scale <= 0: raise TypeError(
            "'scale' positional argument for attribute %s must be positive. Got: %r" % (attr, scale))
        if not isinstance(precision, (int, long)):
            raise TypeError("'precision' positional argument for attribute %s must be int. Got: %r" % (attr, precision))
        if precision <= 0: raise TypeError(
            "'precision' positional argument for attribute %s must be positive. Got: %r" % (attr, precision))
        if precision > scale: raise ValueError("'precision' must be less or equal 'scale'")
        converter.scale = scale
        converter.precision = precision
        converter.exp = Decimal(10) ** -precision

        min_val = keyargs.pop('min', None)
        if min_val is not None:
            try: min_val = Decimal(min_val)
            except TypeError: raise TypeError(
                "Invalid value for 'min' argument for attribute %s: %r" % (attr, min_val))
        max_val = keyargs.pop('max', None)
        if max_val is not None:
            try: max_val = Decimal(max_val)
            except TypeError: raise TypeError(
                "Invalid value for 'max' argument for attribute %s: %r" % (attr, max_val))
        converter.min_val = min_val
        converter.max_val = max_val
    def validate(converter, val):
        if type(val) is Decimal: return val
        try: return Decimal(val)
        except InvalidOperation, exc:
            raise TypeError('Invalid value for attribute %s: %r' % (converter.attr, val))
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
    def sql_type(converter):
        return 'DECIMAL(%d, %d)' % (converter.scale, converter.precision)

class BlobConverter(Converter):
    def init(converter, keyargs):
        attr = converter.attr
        if attr and attr.args: unexpected_args(attr, attr.args)
    def validate(converter, val):
        if isinstance(val, buffer): return val
        if isinstance(val, str): return buffer(val)
        raise TypeError("Attribute %r: expected type is 'buffer'. Got: %r" % (converter.attr, type(val)))
    def sql_type(converter):
        return 'BLOB'

class DatetimeConverter(Converter):
    def init(converter, keyargs):
        attr = converter.attr
        if attr and attr.args: unexpected_args(attr, attr.args)
    def validate(converter, val):
        if not isinstance(val, datetime):
            raise TypeError("Attribute %r: expected type is 'datetime'. Got: %r" % (converter.attr, val))
        return val
    def sql2py(converter, val):
        try: return timestamp2datetime(val)
        except: return val
    def py2sql(converter, val):
        return datetime2timestamp(val)
    def sql_type(converter):
        return 'DATETIME'

class DateConverter(Converter):
    def init(converter, keyargs):
        attr = converter.attr
        if attr and attr.args: unexpected_args(attr, attr.args)
    def validate(converter, val):
        if isinstance(val, datetime): return val.date()
        if not isinstance(val, date):
            raise TypeError("Attribute %r: expected type is 'date'. Got: %r" % (converter.attr, val))
        return val
    def sql2py(converter, val):
        try:       
            time_tuple = strptime(val[:10], '%Y-%m-%d')
            return date(*time_tuple[:3])
        except: return val
    def py2sql(converter, val):
        return val.strftime('%Y-%m-%d')
    def sql_type(converter):
        return 'DATE'

memdb_queue = Queue()

class Local(localbase):
    def __init__(local):
        local.lock = Lock()
        local.lock.acquire()

local = Local()

@simple_decorator
def in_dedicated_thread(func, *args, **keyargs):
    result_holder = []
    memdb_queue.put((local.lock, func, args, keyargs, result_holder))
    local.lock.acquire()
    result = result_holder[0]
    if isinstance(result, Exception):
        try: raise result
        finally: del result, result_holder
    if isinstance(result, sqlite.Cursor): result = MemoryCursorWrapper(result)
    return result

class Holder(object): # because sqlite3.Connection does not support weak refs
    def __init__(holder, obj):
        holder.obj = obj

def make_wrapper_method(method_name):
    @in_dedicated_thread
    def wrapper_method(wrapper, *args, **keyargs):
        method = getattr(wrapper._holder.obj, method_name)
        return method(*args, **keyargs)
    wrapper_method.__name__ = method_name
    return wrapper_method

def make_wrapper_property(attr):
    @in_dedicated_thread
    def getter(wrapper):
        return getattr(wrapper._holder.obj, attr)
    @in_dedicated_thread
    def setter(wrapper, value):
        setattr(wrapper._holder.obj, attr, value)
    return property(getter, setter)

class MemoryConnectionWrapper(object):
    @in_dedicated_thread
    def __init__(wrapper):
        con = sqlite.connect(':memory:')
        _init_connection(con)
        wrapper._holder = Holder(con)
        memdb_connections.add(weakref.ref(wrapper._holder))
    def interrupt(wrapper):
        wrapper._holder.obj.interrupt()
    @in_dedicated_thread
    def iterdump(wrapper, *args, **keyargs):
        return iter(list(wrapper._holder.obj.iterdump()))

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
        wrapper._holder = Holder(cur)
    def __iter__(wrapper):
        return wrapper
    
sqlite_cur_methods = '''execute executemany executescript fetchone fetchmany fetchall
                        next close setinputsize setoutputsize'''.split()

for m in sqlite_cur_methods:
    setattr(MemoryCursorWrapper, m, make_wrapper_method(m))

sqlite_cur_properties = 'rowcount lastrowid description arraysize'.split()

for p in sqlite_cur_properties:
    setattr(MemoryCursorWrapper, p, make_wrapper_property(p))
    
memdb_connections = set()

class SqliteMemoryDbThread(Thread):
    def __init__(self):
        Thread.__init__(self, name="SqliteMemoryDbThread")
        self.setDaemon(True)
    def run(self):
        try:
            while True:
                x = memdb_queue.get()
                if x is None: break
                lock, func, args, keyargs, result_holder = x
                try: result = func(*args, **keyargs)
                except Exception, e:
                    result_holder.append(e)
                    del e
                else: result_holder.append(result)
                if lock is not None: lock.release()
        finally:
            for ref in memdb_connections:
                holder = ref()
                if holder is not None: holder.obj.close()
            memdb_connections.clear()

memory_db_thread = SqliteMemoryDbThread()
memory_db_thread.start()
