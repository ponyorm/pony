from datetime import date, datetime
from decimal import Decimal

import cx_Oracle

from cx_Oracle import (Warning, Error, InterfaceError, DatabaseError,
                     DataError, OperationalError, IntegrityError, InternalError,
                     ProgrammingError, NotSupportedError)

from pony import orm, dbschema, sqlbuilding
from pony.sqltranslation import SQLTranslator
from pony.clobtypes import LongStr, LongUnicode
from pony.utils import is_utf8

paramstyle = 'named'

MAX_PARAMS_COUNT = 200
ROW_VALUE_SYNTAX = True

def quote_name(connection, name):
    return sqlbuilding.quote_name(name)

def get_pool(*args, **keyargs):
    user = password = dsn = None
    if len(args) == 1:
        conn_str = args[0]
        if '/' in conn_str:
            user, tail = conn_str.split('/', 1)
            if '@' in tail: password, dsn = tail.split('@', 1)
        if None in (user, password, dsn): raise ValueError(
            "Incorrect connection string (must be in form of 'user/password@dsn')")
    elif len(args) == 2: user, password = args
    elif len(args) == 3: user, password, dsn = args
    elif args: raise ValueError('Invalid number of positional arguments')
    if user != keyargs.setdefault('user', user):
        raise ValueError('Ambiguous value for user')
    if password != keyargs.setdefault('password', password):
        raise ValueError('Ambiguous value for password')
    if dsn != keyargs.setdefault('dsn', dsn):
        raise ValueError('Ambiguous value for dsn')
    keyargs.setdefault('threaded', True)
    keyargs.setdefault('min', 1)
    keyargs.setdefault('max', 10)
    keyargs.setdefault('increment', 1)
    return Pool(**keyargs)

def output_type_handler(cursor, name, defaultType, size, precision, scale):
    if defaultType == cx_Oracle.NUMBER:
        if scale == 0:
            return cursor.var(cx_Oracle.STRING, 40, cursor.arraysize, outconverter=int)
        if scale != -127:
            return cursor.var(cx_Oracle.STRING, 100, cursor.arraysize, outconverter=Decimal)
    elif defaultType in (cx_Oracle.STRING, cx_Oracle.FIXED_CHAR):
        return cursor.var(unicode, size, cursor.arraysize)  # from cx_Oracle example
    return None

class Pool(object):
    def __init__(pool, **keyargs):
        pool._pool = cx_Oracle.SessionPool(**keyargs)
    def connect(pool):
        con = pool._pool.acquire()
        con.outputtypehandler = output_type_handler
        return con
    def release(pool, con):
        pool._pool.release(con)
    def close(pool, con):
        pool._pool.drop(con)

translator_cls = SQLTranslator

def ast2sql(con, ast):
    b = sqlbuilding.SQLBuilder(ast, paramstyle)
    return str(b.sql), b.adapter

trigger_template = """
create trigger %s
  before insert on %s  
  for each row
begin
  if :new.%s is null then
    select %s.nextval into :new.%s from dual;
  end if;
end;"""

class OraTable(dbschema.Table):
    def create(table, provider, connection, created_tables=None):
        commands = table.get_create_commands(created_tables)
        for i, sql in enumerate(commands):
            if orm.debug:
                print sql
                print
            cursor = connection.cursor()
            try: orm.wrap_dbapi_exceptions(provider, cursor.execute, sql)
            except orm.DatabaseError, e:
                if e.exceptions[0].args[0].code == 955:
                    if orm.debug: print 'ALREADY EXISTS:', e.args[0].message
                    if not i:
                        if len(commands) > 1: print 'SKIP FURTHER DDL COMMANDS FOR TABLE %s\n' % table.name
                        return
                else: raise
    def get_create_commands(table, created_tables=None):
        result = dbschema.Table.get_create_commands(table, created_tables, False)
        for column in table.column_list:
            if column.is_pk == 'auto':
                quote_name = table.schema.quote_name
                case = table.schema.case
                seq_name = quote_name(table.name + '_SEQ')
                result.append(case('create sequence %s nocache') % seq_name)
                table_name = quote_name(table.name)
                trigger_name = quote_name(table.name + '_BI')  # Before Insert
                column_name = quote_name(column.name)
                result.append(case(trigger_template) % (trigger_name, table_name, column_name, seq_name, column_name))
                break
        return result

class OraColumn(dbschema.Column):
    auto_template = None
    
class OraSchema(dbschema.DBSchema):
    table_class = OraTable
    column_class = OraColumn

def create_schema(database):
    return OraSchema(database)

def get_inputsize(arg):
    if isinstance(arg, datetime):
        return cx_Oracle.TIMESTAMP
    return None

def set_input_sizes(cursor, arguments):
    if type(arguments) is dict:
        input_sizes = {}
        for name, arg in arguments.iteritems():
            size = get_inputsize(arg)
            if size is not None: input_sizes[name] = size
        cursor.setinputsizes(**input_sizes)
    elif type(arguments) is tuple:
        input_sizes = map(get_inputsize, arguments)
        cursor.setinputsizes(*input_sizes)
    else: assert False

def execute(cursor, sql, arguments):
    set_input_sizes(cursor, arguments)
    cursor.execute(sql, arguments)

def executemany(cursor, sql, arguments_list):
    set_input_sizes(cursor, arguments_list[0])
    cursor.executemany(sql, arguments_list)

def execute_sql_returning_id(cursor, sql, arguments, returning_py_type):
    if returning_py_type != int: raise NotImplementedError
    set_input_sizes(cursor, arguments)
    var = cursor.var(cx_Oracle.NUMBER)
    arguments['new_id'] = var
    cursor.execute(sql, arguments)
    val = var.getvalue()
    return returning_py_type(val)

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
        return bool(val)  # TODO: True/False, T/F, Y/N, Yes/No, etc.
    def sql_type(converter):
        return "NUMBER(1)"

class BasestringConverter(Converter):
    def init(converter, keyargs):
        attr = converter.attr
        if attr:
            if not attr.args: max_len = None
            elif len(attr.args) > 1: unexpected_args(attr, attr.args[1:])
            else: max_len = attr.args[0]
            if issubclass(attr.py_type, (LongStr, LongUnicode)):
                if max_len is not None: raise TypeError('Max length is not supported for CLOBs')
            elif max_len is None: max_len = 200
            elif not isinstance(max_len, (int, long)):
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
            return 'VARCHAR2(%d CHAR)' % converter.max_len
        return 'CLOB'

class UnicodeConverter(BasestringConverter):
    def validate(converter, val):
        if val is None: pass
        elif isinstance(val, str): val = val.decode('ascii')
        elif not isinstance(val, unicode): raise TypeError(
            'Value type for attribute %s must be unicode. Got: %r' % (converter.attr, type(val)))
        return BasestringConverter.validate(converter, val)
    def sql2py(converter, val):
        if isinstance(val, cx_Oracle.LOB):
            val = val.read()
            val = val.decode('utf8')
        return val

class StrConverter(BasestringConverter):
    def __init__(converter, attr=None):
        converter.encoding = 'ascii'  # for the case when attr is None
        BasestringConverter.__init__(converter, attr)
    def init(converter, keyargs):
        BasestringConverter.init(converter, keyargs)
        converter.encoding = keyargs.pop('encoding', 'latin1')
    def validate(converter, val):
        if val is not None:
            if isinstance(val, str): pass
            elif isinstance(val, unicode): val = val.encode(converter.encoding)
            else: raise TypeError('Value type for attribute %s must be str in encoding %r. Got: %r'
                                  % (converter.attr, converter.encoding, type(val)))
        return BasestringConverter.validate(converter, val)
    def py2sql(converter, val):
        return val.decode(converter.encoding)
    def sql2py(converter, val):
        if isinstance(val, cx_Oracle.LOB):
            val = val.read()
            if is_utf8(converter.encoding): return val
            val = val.decode('utf8')
        assert isinstance(val, unicode)
        val = val.encode(converter.encoding, 'replace')
        return val

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
        return 'NUMBER(38)'

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
        return 'NUMBER'

class DecimalConverter(Converter):
    def __init__(converter, attr=None):
        Converter.__init__(converter, attr)
    def init(converter, keyargs):
        attr = converter.attr
        args = attr.args
        if len(args) > 2: raise TypeError('Too many positional parameters for Decimal (expected: precision and scale)')

        if args: precision = args[0]
        else: precision = keyargs.pop('precision', 12)
        if not isinstance(precision, (int, long)):
            raise TypeError("'precision' positional argument for attribute %s must be int. Got: %r" % (attr, precision))
        if precision <= 0: raise TypeError(
            "'precision' positional argument for attribute %s must be positive. Got: %r" % (attr, precision))

        if len(args) == 2: scale = args[1]
        else: scale = keyargs.pop('scale', 2)
        if not isinstance(scale, (int, long)):
            raise TypeError("'scale' positional argument for attribute %s must be int. Got: %r" % (attr, scale))
        if scale <= 0: raise TypeError(
            "'scale' positional argument for attribute %s must be positive. Got: %r" % (attr, scale))

        if scale > precision: raise ValueError("'scale' must be less or equal 'precision'")
        converter.precision = precision
        converter.scale = scale
        converter.exp = Decimal(10) ** -scale

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
        if converter.min_val is not None and val < converter.min_val:
            raise ValueError('Value %r of attr %s is less than the minimum allowed value %r'
                             % (val, converter.attr, converter.min_val))
        if converter.max_val is not None and val > converter.max_val:
            raise ValueError('Value %r of attr %s is greater than the maximum allowed value %r'
                             % (val, converter.attr, converter.max_val))
    def sql_type(converter):
        return 'NUMBER(%d, %d)' % (converter.precision, converter.scale)

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
    def sql2py(converter, val):
        return buffer(val.read())

class DatetimeConverter(Converter):
    def init(converter, keyargs):
        attr = converter.attr
        if attr and attr.args: unexpected_args(attr, attr.args)
    def validate(converter, val):
        if not isinstance(val, datetime):
            raise TypeError("Attribute %r: expected type is 'datetime'. Got: %r" % (converter.attr, val))
        return val
    def sql_type(converter):
        return 'TIMESTAMP(6)'

class DateConverter(Converter):
    def init(converter, keyargs):
        attr = converter.attr
        if attr and attr.args: unexpected_args(attr, attr.args)
    def validate(converter, val):
        if isinstance(val, datetime): return val.date()
        if not isinstance(val, date):
            raise TypeError("Attribute %r: expected type is 'date'. Got: %r" % (converter.attr, val))
        return val
    def sql_type(converter):
        return 'DATE'
    def sql2py(converter, val):
        if isinstance(val, datetime): return val.date()
        return val

translator_cls = SQLTranslator

class OraBuilder(sqlbuilding.SQLBuilder):
    def INSERT(builder, table_name, columns, values, returning=None):
        result = sqlbuilding.SQLBuilder.INSERT(builder, table_name, columns, values)
        if returning is not None:
            result.extend([ ' RETURNING ', builder.quote_name(returning), ' INTO :new_id' ])
        return result

def ast2sql(con, ast):
    b = OraBuilder(ast, paramstyle)
    return str(b.sql), b.adapter
