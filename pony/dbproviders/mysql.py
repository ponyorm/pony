from decimal import Decimal, InvalidOperation
from datetime import datetime, date, time

import warnings
warnings.filterwarnings('ignore', '^Table.+already exists$', Warning, '^pony\\.orm$')

import MySQLdb
from MySQLdb import (Warning, Error, InterfaceError, DatabaseError,
                     DataError, OperationalError, IntegrityError, InternalError,
                     ProgrammingError, NotSupportedError)
from MySQLdb.constants import FIELD_TYPE, FLAG
import MySQLdb.converters

from pony import dbschema
from pony import sqlbuilding
from pony.sqltranslation import SQLTranslator as translator_cls
from pony.clobtypes import LongStr, LongUnicode
from pony.utils import localbase

paramstyle = 'format'

class MySQLColumn(dbschema.Column):
    autoincrement = 'AUTO_INCREMENT'

class MySQLSchema(dbschema.DBSchema):
    column_class = MySQLColumn

def create_schema(database):
    return MySQLSchema(database)

def quote_name(connection, name):
    return sqlbuilding.quote_name(name, "`")

def get_pool(*args, **keyargs):
    if 'conv' not in keyargs:
        conv = MySQLdb.converters.conversions.copy()
        conv[FIELD_TYPE.BLOB] = [(FLAG.BINARY, buffer)]
        keyargs['conv'] = conv
    if 'charset' not in keyargs:
        keyargs['charset'] = 'utf8'
    return Pool(*args, **keyargs)

class Pool(localbase):
    def __init__(pool, *args, **keyargs): # called separately in each thread
        pool.args = args
        pool.keyargs = keyargs
        pool.con = None
    def connect(pool):
        if pool.con is None:
            pool.con = MySQLdb.connect(*pool.args, **pool.keyargs)
        return pool.con
    def release(pool, con):
        assert con is pool.con
        try: con.rollback()
        except:
            pool.close(con)
            raise
    def close(pool, con):
        assert con is pool.con
        pool.con = None
        con.close()

class MyValue(sqlbuilding.Value):
    def quote_str(self, s):
        s = s.replace('%', '%%')
        return sqlbuilding.Value.quote_str(self, s)

class MySQLBuilder(sqlbuilding.SQLBuilder):
    make_value = MyValue

def ast2sql(con, ast):
    b = MySQLBuilder(ast, paramstyle, "`")
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
    def __init__(converter, attr=None):
        converter.db_encoding = None
        Converter.__init__(converter, attr)
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
        converter.db_encoding = keyargs.pop('db_encoding', 'utf8')
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
            return 'VARCHAR(%d) CHARACTER SET %s' % (converter.max_len, converter.db_encoding)
        return 'TEXT CHARACTER SET %s' % converter.db_encoding

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
        return 'DOUBLE'

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
        return 'DECIMAL(%d, %d)' % (converter.precision, converter.scale)

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
    def sql_type(converter):
        return 'DATE'