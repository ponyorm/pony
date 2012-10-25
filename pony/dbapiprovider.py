from decimal import Decimal, InvalidOperation
from datetime import datetime, date, time

from pony.utils import is_utf8, simple_decorator, throw
from pony.converting import str2date, str2datetime

class DBException(Exception):
    def __init__(exc, *args, **keyargs):
        exceptions = keyargs.pop('exceptions', [])
        assert not keyargs
        if not args and exceptions:
            if len(exceptions) == 1: args = getattr(exceptions[0], 'args', ())
            else: args = ('Multiple exceptions have occured',)
        Exception.__init__(exc, *args)
        exc.exceptions = exceptions

class RowNotFound(DBException): pass
class MultipleRowsFound(DBException): pass
class TooManyRowsFound(DBException): pass

##StandardError
##        |__Warning
##        |__Error
##           |__InterfaceError
##           |__DatabaseError
##              |__DataError
##              |__OperationalError
##              |__IntegrityError
##              |__InternalError
##              |__ProgrammingError
##              |__NotSupportedError

class Warning(DBException): pass
class Error(DBException): pass
class   InterfaceError(Error): pass
class   DatabaseError(Error): pass
class     DataError(DatabaseError): pass
class     OperationalError(DatabaseError): pass
class     IntegrityError(DatabaseError): pass
class     InternalError(DatabaseError): pass
class     ProgrammingError(DatabaseError): pass
class     NotSupportedError(DatabaseError): pass

@simple_decorator
def wrap_dbapi_exceptions(func, provider, *args, **keyargs):
    dbapi_module = provider.dbapi_module
    try: return func(provider, *args, **keyargs)
    except dbapi_module.NotSupportedError, e: raise NotSupportedError(exceptions=[e])
    except dbapi_module.ProgrammingError, e: raise ProgrammingError(exceptions=[e])
    except dbapi_module.InternalError, e: raise InternalError(exceptions=[e])
    except dbapi_module.IntegrityError, e: raise IntegrityError(exceptions=[e])
    except dbapi_module.OperationalError, e: raise OperationalError(exceptions=[e])
    except dbapi_module.DataError, e: raise DataError(exceptions=[e])
    except dbapi_module.DatabaseError, e: raise DatabaseError(exceptions=[e])
    except dbapi_module.InterfaceError, e:
        if e.args == (0, '') and getattr(dbapi_module, '__name__', None) == 'MySQLdb':
            throw(InterfaceError, 'MySQL server misconfiguration', exceptions=[e])
        raise InterfaceError(exceptions=[e])
    except dbapi_module.Error, e: raise Error(exceptions=[e])
    except dbapi_module.Warning, e: raise Warning(exceptions=[e])

class LongStr(str):
    lazy = True

class LongUnicode(unicode):
    lazy = True

class DBAPIProvider(object):
    paramstyle = 'qmark'
    quote_char = '"'
    max_params_count = 200
    
    dbschema_cls = None
    translator_cls = None
    sqlbuilder_cls = None

    def __init__(provider, dbapi_module):
        provider.dbapi_module = dbapi_module

    def get_default_entity_table_name(provider, entity):
        return entity.__name__

    def get_default_m2m_table_name(provider, attr, reverse):
        if attr.symmetric:
            assert reverse is attr
            return attr.entity.__name__ + '_' + attr.name
        return attr.entity.__name__ + '_' + reverse.entity.__name__

    def get_default_column_names(provider, attr, reverse_pk_columns=None):
        if reverse_pk_columns is None:
            return [ attr.name ]
        elif len(reverse_pk_columns) == 1:
            return [ attr.name ]
        else:
            prefix = attr.name + '_'
            return [ prefix + column for column in reverse_pk_columns ]

    def get_default_m2m_column_names(provider, entity):
        columns = entity._get_pk_columns_()
        if len(columns) == 1:
            return [ entity.__name__.lower() ]
        else:
            prefix = entity.__name__.lower() + '_'
            return [ prefix + column for column in columns ]

    def quote_name(provider, name):
        quote_char = provider.quote_char
        if isinstance(name, basestring):
            name = name.replace(quote_char, quote_char+quote_char)
            return quote_char + name + quote_char
        return '.'.join(provider.quote_name(item) for item in name)

    def ast2sql(provider, ast):
        builder = provider.sqlbuilder_cls(provider, ast)
        return builder.sql, builder.adapter

    @wrap_dbapi_exceptions
    def connect(provider):
        return provider.pool.connect()

    @wrap_dbapi_exceptions
    def release(provider, connection):
        return provider.pool.release(connection)

    @wrap_dbapi_exceptions
    def drop(provider, connection):
        return provider.pool.drop(connection)

    @wrap_dbapi_exceptions
    def execute(provider, cursor, sql, arguments=None):
        if arguments is None: cursor.execute(sql)
        else: cursor.execute(sql, arguments)

    @wrap_dbapi_exceptions
    def executemany(provider, cursor, sql, arguments_list):
        cursor.executemany(sql, arguments_list)

    @wrap_dbapi_exceptions
    def execute_returning_id(provider, cursor, sql, arguments):
        cursor.execute(sql, arguments)
        return cursor.lastrowid

    @wrap_dbapi_exceptions
    def commit(provider, connection):
        connection.commit()

    @wrap_dbapi_exceptions
    def rollback(provider, connection):
        connection.rollback()

    converter_classes = []

    def _get_converter_type_by_py_type(provider, py_type):
        if isinstance(py_type, type):
            for t, converter_cls in provider.converter_classes:
                if issubclass(py_type, t): return converter_cls
        throw(TypeError, 'No database converter found for type %s' % py_type)

    def get_converter_by_py_type(provider, py_type):
        return provider._get_converter_type_by_py_type(py_type)()

    def get_converter_by_attr(provider, attr):
        return provider._get_converter_type_by_py_type(attr.py_type)(attr)

class Converter(object):
    def __init__(converter, attr=None):
        converter.attr = attr
        if attr is None: return
        keyargs = attr.keyargs.copy()
        converter.init(keyargs)
        for option in keyargs: throw(TypeError, 'Unknown option %r' % option)
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
        converter.max_len = None
        converter.db_encoding = None
        Converter.__init__(converter, attr)
    def init(converter, keyargs):
        attr = converter.attr
        if not attr.args: max_len = None
        elif len(attr.args) > 1: unexpected_args(attr, attr.args[1:])
        else: max_len = attr.args[0]
        if issubclass(attr.py_type, (LongStr, LongUnicode)):
            if max_len is not None: throw(TypeError, 'Max length is not supported for CLOBs')
        elif max_len is None: max_len = 200
        elif not isinstance(max_len, (int, long)):
            throw(TypeError, 'Max length argument must be int. Got: %r' % max_len)
        converter.max_len = max_len
        converter.db_encoding = keyargs.pop('db_encoding', None)
    def validate(converter, val):
        max_len = converter.max_len
        val_len = len(val)
        if max_len and val_len > max_len:
            throw(ValueError, 'Value for attribute %s is too long. Max length is %d, value length is %d'
                             % (converter.attr, max_len, val_len))
        if not val_len: throw(ValueError, 'Empty strings are not allowed. Try using None instead')
        return val
    def sql_type(converter):
        if converter.max_len:
            return 'VARCHAR(%d)' % converter.max_len
        return 'TEXT'

class UnicodeConverter(BasestringConverter):
    def validate(converter, val):
        if val is None: pass
        elif isinstance(val, str): val = val.decode('ascii')
        elif not isinstance(val, unicode): throw(TypeError, 
            'Value type for attribute %s must be unicode. Got: %r' % (converter.attr, type(val)))
        return BasestringConverter.validate(converter, val)

class StrConverter(BasestringConverter):
    def __init__(converter, attr=None):
        converter.encoding = 'ascii'  # for the case when attr is None
        BasestringConverter.__init__(converter, attr)
        converter.utf8 = is_utf8(converter.encoding)
    def init(converter, keyargs):
        BasestringConverter.init(converter, keyargs)
        converter.encoding = keyargs.pop('encoding', 'latin1')
    def validate(converter, val):
        if val is not None:
            if isinstance(val, str): pass
            elif isinstance(val, unicode): val = val.encode(converter.encoding)
            else: throw(TypeError, 'Value type for attribute %s must be str in encoding %r. Got: %r'
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
            throw(TypeError, "'min' argument for attribute %s must be int. Got: %r" % (attr, min_val))
        max_val = keyargs.pop('max', None)
        if max_val is not None and not isinstance(max_val, (int, long)):
            throw(TypeError, "'max' argument for attribute %s must be int. Got: %r" % (attr, max_val))
        converter.min_val = min_val
        converter.max_val = max_val
    def validate(converter, val):
        if isinstance(val, (int, long)): pass
        elif isinstance(val, basestring):
            try: val = int(val)
            except ValueError: throw(ValueError, 
                'Value type for attribute %s must be int. Got string %r' % (converter.attr, val))
        else: throw(TypeError, 'Value type for attribute %s must be int. Got: %r' % (converter.attr, type(val)))

        if converter.min_val and val < converter.min_val:
            throw(ValueError, 'Value %r of attr %s is less than the minimum allowed value %r'
                             % (val, converter.attr, converter.min_val))
        if converter.max_val and val > converter.max_val:
            throw(ValueError, 'Value %r of attr %s is greater than the maximum allowed value %r'
                             % (val, converter.attr, converter.max_val))
        return val
    def sql2py(converter, val):
        return int(val)
    def sql_type(converter):
        return 'INTEGER'

class RealConverter(Converter):
    default_tolerance = None
    def init(converter, keyargs):
        attr = converter.attr
        if attr and attr.args: unexpected_args(attr, attr.args)
        min_val = keyargs.pop('min', None)
        if min_val is not None:
            try: min_val = float(min_val)
            except ValueError:
                throw(TypeError, "Invalid value for 'min' argument for attribute %s: %r" % (attr, min_val))
        max_val = keyargs.pop('max', None)
        if max_val is not None:
            try: max_val = float(max_val)
            except ValueError:
                throw(TypeError, "Invalid value for 'max' argument for attribute %s: %r" % (attr, max_val))
        converter.min_val = min_val
        converter.max_val = max_val
        converter.tolerance = keyargs.pop('tolerance', converter.default_tolerance)
    def validate(converter, val):
        try: val = float(val)
        except ValueError:
            throw(TypeError, 'Invalid value for attribute %s: %r' % (converter.attr, val))
        if converter.min_val and val < converter.min_val:
            throw(ValueError, 'Value %r of attr %s is less than the minimum allowed value %r'
                             % (val, converter.attr, converter.min_val))
        if converter.max_val and val > converter.max_val:
            throw(ValueError, 'Value %r of attr %s is greater than the maximum allowed value %r'
                             % (val, converter.attr, converter.max_val))
        return val
    def equals(converter, x, y):
        tolerance = converter.tolerance
        if tolerance is None: return x == y
        denominator = max(abs(x), abs(y))
        if not denominator: return True
        diff = abs(x-y) / denominator
        return diff <= tolerance
    def sql2py(converter, val):
        return float(val)
    def sql_type(converter):
        return 'REAL'

class DecimalConverter(Converter):
    def __init__(converter, attr=None):
        converter.exp = None  # for the case when attr is None
        Converter.__init__(converter, attr)
    def init(converter, keyargs):
        attr = converter.attr
        args = attr.args
        if len(args) > 2: throw(TypeError, 'Too many positional parameters for Decimal (expected: precision and scale)')

        if args: precision = args[0]
        else: precision = keyargs.pop('precision', 12)
        if not isinstance(precision, (int, long)):
            throw(TypeError, "'precision' positional argument for attribute %s must be int. Got: %r" % (attr, precision))
        if precision <= 0: throw(TypeError, 
            "'precision' positional argument for attribute %s must be positive. Got: %r" % (attr, precision))

        if len(args) == 2: scale = args[1]
        else: scale = keyargs.pop('scale', 2)
        if not isinstance(scale, (int, long)):
            throw(TypeError, "'scale' positional argument for attribute %s must be int. Got: %r" % (attr, scale))
        if scale <= 0: throw(TypeError, 
            "'scale' positional argument for attribute %s must be positive. Got: %r" % (attr, scale))

        if scale > precision: throw(ValueError, "'scale' must be less or equal 'precision'")
        converter.precision = precision
        converter.scale = scale
        converter.exp = Decimal(10) ** -scale

        min_val = keyargs.pop('min', None)
        if min_val is not None:
            try: min_val = Decimal(min_val)
            except TypeError: throw(TypeError, 
                "Invalid value for 'min' argument for attribute %s: %r" % (attr, min_val))

        max_val = keyargs.pop('max', None)
        if max_val is not None:
            try: max_val = Decimal(max_val)
            except TypeError: throw(TypeError, 
                "Invalid value for 'max' argument for attribute %s: %r" % (attr, max_val))
            
        converter.min_val = min_val
        converter.max_val = max_val
    def validate(converter, val):
        try: val = Decimal(val)
        except InvalidOperation, exc:
            throw(TypeError, 'Invalid value for attribute %s: %r' % (converter.attr, val))
        if converter.min_val is not None and val < converter.min_val:
            throw(ValueError, 'Value %r of attr %s is less than the minimum allowed value %r'
                             % (val, converter.attr, converter.min_val))
        if converter.max_val is not None and val > converter.max_val:
            throw(ValueError, 'Value %r of attr %s is greater than the maximum allowed value %r'
                             % (val, converter.attr, converter.max_val))
        return val
    def sql2py(converter, val):
        return Decimal(val)
    def sql_type(converter):
        return 'DECIMAL(%d, %d)' % (converter.precision, converter.scale)

class BlobConverter(Converter):
    def init(converter, keyargs):
        attr = converter.attr
        if attr and attr.args: unexpected_args(attr, attr.args)
    def validate(converter, val):
        if isinstance(val, buffer): return val
        if isinstance(val, str): return buffer(val)
        throw(TypeError, "Attribute %r: expected type is 'buffer'. Got: %r" % (converter.attr, type(val)))
    def sql2py(converter, val):
        if not isinstance(val, buffer): val = buffer(val)
        return val
    def sql_type(converter):
        return 'BLOB'

class DateConverter(Converter):
    def init(converter, keyargs):
        attr = converter.attr
        if attr and attr.args: unexpected_args(attr, attr.args)
    def validate(converter, val):
        if isinstance(val, datetime): return val.date()
        if isinstance(val, date): return val
        if isinstance(val, basestring): return str2date(val)
        throw(TypeError, "Attribute %r: expected type is 'date'. Got: %r" % (converter.attr, val))
    def sql2py(converter, val):
        if not isinstance(val, date): throw(ValueError, 
            'Value of unexpected type received from database: instead of date got %s', type(val))
        return val
    def sql_type(converter):
        return 'DATE'

class DatetimeConverter(Converter):
    def init(converter, keyargs):
        attr = converter.attr
        if attr and attr.args: unexpected_args(attr, attr.args)
    def validate(converter, val):
        if isinstance(val, datetime): return val
        if isinstance(val, basestring): return str2datetime(val)
        throw(TypeError, "Attribute %r: expected type is 'datetime'. Got: %r" % (converter.attr, val))
    def sql2py(converter, val):
        if not isinstance(val, datetime): raise ValueError
        return val
    def sql_type(converter):
        return 'DATETIME'
