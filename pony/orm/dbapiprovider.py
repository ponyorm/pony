from decimal import Decimal, InvalidOperation
from datetime import datetime, date, time
from uuid import uuid4, UUID
import re

import pony
from pony.utils import is_utf8, decorator, throw, localbase
from pony.converting import str2date, str2datetime
from pony.orm.ormtypes import LongStr, LongUnicode

class DBException(Exception):
    def __init__(exc, original_exc, *args):
        args = args or getattr(original_exc, 'args', ())
        Exception.__init__(exc, *args)
        exc.original_exc = original_exc

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

@decorator
def wrap_dbapi_exceptions(func, provider, *args, **kwargs):
    dbapi_module = provider.dbapi_module
    try: return func(provider, *args, **kwargs)
    except dbapi_module.NotSupportedError, e: raise NotSupportedError(e)
    except dbapi_module.ProgrammingError, e: raise ProgrammingError(e)
    except dbapi_module.InternalError, e: raise InternalError(e)
    except dbapi_module.IntegrityError, e: raise IntegrityError(e)
    except dbapi_module.OperationalError, e: raise OperationalError(e)
    except dbapi_module.DataError, e: raise DataError(e)
    except dbapi_module.DatabaseError, e: raise DatabaseError(e)
    except dbapi_module.InterfaceError, e:
        if e.args == (0, '') and getattr(dbapi_module, '__name__', None) == 'MySQLdb':
            throw(InterfaceError, e, 'MySQL server misconfiguration')
        raise InterfaceError(e)
    except dbapi_module.Error, e: raise Error(e)
    except dbapi_module.Warning, e: raise Warning(e)

def unexpected_args(attr, args):
    throw(TypeError,
        'Unexpected positional argument%s for attribute %s: %r'
        % ((args > 1 and 's' or ''), attr, ', '.join(map(repr, args))))

version_re = re.compile('[0-9\.]+')

def get_version_tuple(s):
    m = version_re.match(s)
    if m is not None:
        return tuple(map(int, m.group(0).split('.')))
    return None

class DBAPIProvider(object):
    paramstyle = 'qmark'
    quote_char = '"'
    max_params_count = 200
    max_name_len = 128
    table_if_not_exists_syntax = True
    index_if_not_exists_syntax = True
    max_time_precision = default_time_precision = 6
    select_for_update_nowait_syntax = True

    dialect = None
    dbapi_module = None
    dbschema_cls = None
    translator_cls = None
    sqlbuilder_cls = None

    name_before_table = 'schema_name'
    default_schema_name = None

    def __init__(provider, *args, **kwargs):
        pool_mockup = kwargs.pop('pony_pool_mockup', None)
        if pool_mockup: provider.pool = pool_mockup
        else: provider.pool = provider.get_pool(*args, **kwargs)
        connection = provider.connect()
        provider.inspect_connection(connection)
        provider.release(connection)

    @wrap_dbapi_exceptions
    def inspect_connection(provider, connection):
        pass

    def normalize_name(provider, name):
        return name[:provider.max_name_len]

    def get_default_entity_table_name(provider, entity):
        return provider.normalize_name(entity.__name__)

    def get_default_m2m_table_name(provider, attr, reverse):
        if attr.symmetric:
            assert reverse is attr
            return attr.entity.__name__ + '_' + attr.name
        name = attr.entity.__name__ + '_' + reverse.entity.__name__
        return provider.normalize_name(name)

    def get_default_column_names(provider, attr, reverse_pk_columns=None):
        normalize = provider.normalize_name
        if reverse_pk_columns is None:
            return [ normalize(attr.name) ]
        elif len(reverse_pk_columns) == 1:
            return [ normalize(attr.name) ]
        else:
            prefix = attr.name + '_'
            return [ normalize(prefix + column) for column in reverse_pk_columns ]

    def get_default_m2m_column_names(provider, entity):
        normalize = provider.normalize_name
        columns = entity._get_pk_columns_()
        if len(columns) == 1:
            return [ normalize(entity.__name__.lower()) ]
        else:
            prefix = entity.__name__.lower() + '_'
            return [ normalize(prefix + column) for column in columns ]

    def get_default_index_name(provider, table_name, column_names, is_pk=False, is_unique=False, m2m=False):
        if is_pk: index_name = 'pk_%s' % table_name
        else:
            if is_unique: template = 'unq_%(tname)s__%(cnames)s'
            elif m2m: template = 'idx_%(tname)s'
            else: template = 'idx_%(tname)s__%(cnames)s'
            index_name = template % dict(tname=table_name,
                                         cnames='_'.join(name for name in column_names))
        return provider.normalize_name(index_name.lower())

    def get_default_fk_name(provider, child_table_name, parent_table_name, child_column_names):
        fk_name = 'fk_%s__%s' % (child_table_name, '__'.join(child_column_names))
        return provider.normalize_name(fk_name.lower())

    def split_table_name(provider, table_name):
        if isinstance(table_name, basestring): return provider.default_schema_name, table_name
        if not table_name: throw(TypeError, 'Invalid table name: %r' % table_name)
        if len(table_name) != 2:
            size = len(table_name)
            throw(TypeError, '%s qualified table name must have two components: '
                             '%s and table_name. Got %d component%s: %s'
                             % (provider.dialect, provider.name_before_table,
                                size, 's' if size != 1 else '', table_name))
        return table_name[0], table_name[1]

    def quote_name(provider, name):
        quote_char = provider.quote_char
        if isinstance(name, basestring):
            name = name.replace(quote_char, quote_char+quote_char)
            return quote_char + name + quote_char
        return '.'.join(provider.quote_name(item) for item in name)

    def ast2sql(provider, ast):
        builder = provider.sqlbuilder_cls(provider, ast)
        return builder.sql, builder.adapter

    def should_reconnect(provider, exc):
        return False

    @wrap_dbapi_exceptions
    def connect(provider):
        return provider.pool.connect()

    @wrap_dbapi_exceptions
    def set_transaction_mode(provider, connection, cache):
        pass

    @wrap_dbapi_exceptions
    def commit(provider, connection):
        core = pony.orm.core
        if core.debug: core.log_orm('COMMIT')
        connection.commit()

    @wrap_dbapi_exceptions
    def rollback(provider, connection):
        core = pony.orm.core
        if core.debug: core.log_orm('ROLLBACK')
        connection.rollback()

    @wrap_dbapi_exceptions
    def release(provider, connection, cache=None):
        core = pony.orm.core
        if cache is not None and cache.db_session is not None and cache.db_session.ddl:
            provider.drop(connection)
        else:
            if core.debug: core.log_orm('RELEASE CONNECTION')
            provider.pool.release(connection)

    @wrap_dbapi_exceptions
    def drop(provider, connection):
        core = pony.orm.core
        if core.debug: core.log_orm('CLOSE CONNECTION')
        provider.pool.drop(connection)

    @wrap_dbapi_exceptions
    def disconnect(provider):
        core = pony.orm.core
        if core.debug: core.log_orm('DISCONNECT')
        provider.pool.disconnect()

    @wrap_dbapi_exceptions
    def execute(provider, cursor, sql, arguments=None, returning_id=False):
        if type(arguments) is list:
            assert arguments and not returning_id
            cursor.executemany(sql, arguments)
        else:
            if arguments is None: cursor.execute(sql)
            else: cursor.execute(sql, arguments)
            if returning_id: return cursor.lastrowid

    converter_classes = []

    def _get_converter_type_by_py_type(provider, py_type):
        if isinstance(py_type, type):
            for t, converter_cls in provider.converter_classes:
                if issubclass(py_type, t): return converter_cls
        throw(TypeError, 'No database converter found for type %s' % py_type)

    def get_converter_by_py_type(provider, py_type):
        converter_cls = provider._get_converter_type_by_py_type(py_type)
        return converter_cls(py_type)

    def get_converter_by_attr(provider, attr):
        py_type = attr.py_type
        converter_cls = provider._get_converter_type_by_py_type(py_type)
        return converter_cls(py_type, attr)

    def get_pool(provider, *args, **kwargs):
        return Pool(provider.dbapi_module, *args, **kwargs)

    def table_exists(provider, connection, table_name, case_sensitive=True):
        throw(NotImplementedError)

    def index_exists(provider, connection, table_name, index_name, case_sensitive=True):
        throw(NotImplementedError)

    def fk_exists(provider, connection, table_name, fk_name, case_sensitive=True):
        throw(NotImplementedError)

    def table_has_data(provider, connection, table_name):
        table_name = provider.quote_name(table_name)
        cursor = connection.cursor()
        cursor.execute('SELECT 1 FROM %s LIMIT 1' % table_name)
        return cursor.fetchone() is not None

    def disable_fk_checks(provider, connection):
        pass

    def enable_fk_checks(provider, connection, prev_state):
        pass

    def drop_table(provider, connection, table_name):
        table_name = provider.quote_name(table_name)
        cursor = connection.cursor()
        sql = 'DROP TABLE %s' % table_name
        cursor.execute(sql)

class Pool(localbase):
    def __init__(pool, dbapi_module, *args, **kwargs): # called separately in each thread
        pool.dbapi_module = dbapi_module
        pool.args = args
        pool.kwargs = kwargs
        pool.con = None
    def connect(pool):
        core = pony.orm.core
        if pool.con is None:
            if core.debug: core.log_orm('GET NEW CONNECTION')
            pool.con = pool.dbapi_module.connect(*pool.args, **pool.kwargs)
        elif core.debug: core.log_orm('GET CONNECTION FROM THE LOCAL POOL')
        return pool.con
    def release(pool, con):
        assert con is pool.con
        try: con.rollback()
        except:
            pool.drop(con)
            raise
    def drop(pool, con):
        assert con is pool.con, (con, pool.con)
        pool.con = None
        con.close()
    def disconnect(pool):
        con = pool.con
        pool.con = None
        if con is not None: con.close()

class Converter(object):
    def __deepcopy__(converter, memo):
        return converter  # Converter instances are "immutable"
    def __init__(converter, py_type, attr=None):
        converter.py_type = py_type
        converter.attr = attr
        if attr is None: return
        kwargs = attr.kwargs.copy()
        converter.init(kwargs)
        for option in kwargs: throw(TypeError, 'Attribute %s has unknown option %r' % (attr, option))
    def init(converter, kwargs):
        attr = converter.attr
        if attr and attr.args: unexpected_args(attr, attr.args)
    def validate(converter, val):
        return val
    def py2sql(converter, val):
        return val
    def sql2py(converter, val):
        return val

class BoolConverter(Converter):
    def validate(converter, val):
        return bool(val)
    def sql2py(converter, val):
        return bool(val)
    def sql_type(converter):
        return "BOOLEAN"

class BasestringConverter(Converter):
    def __init__(converter, py_type, attr=None):
        converter.max_len = None
        converter.db_encoding = None
        Converter.__init__(converter, py_type, attr)
    def init(converter, kwargs):
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
        converter.db_encoding = kwargs.pop('db_encoding', None)
    def validate(converter, val):
        max_len = converter.max_len
        val_len = len(val)
        if max_len and val_len > max_len:
            throw(ValueError, 'Value for attribute %s is too long. Max length is %d, value length is %d'
                             % (converter.attr, max_len, val_len))
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
    def __init__(converter, py_type, attr=None):
        converter.encoding = 'ascii'  # for the case when attr is None
        BasestringConverter.__init__(converter, py_type, attr)
        converter.utf8 = is_utf8(converter.encoding)
    def init(converter, kwargs):
        BasestringConverter.init(converter, kwargs)
        converter.encoding = kwargs.pop('encoding', 'latin1')
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
    def init(converter, kwargs):
        Converter.init(converter, kwargs)
        min_val = kwargs.pop('min', None)
        if min_val is not None and not isinstance(min_val, (int, long)):
            throw(TypeError, "'min' argument for attribute %s must be int. Got: %r" % (converter.attr, min_val))
        max_val = kwargs.pop('max', None)
        if max_val is not None and not isinstance(max_val, (int, long)):
            throw(TypeError, "'max' argument for attribute %s must be int. Got: %r" % (converter.attr, max_val))
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
    def init(converter, kwargs):
        Converter.init(converter, kwargs)
        min_val = kwargs.pop('min', None)
        if min_val is not None:
            try: min_val = float(min_val)
            except ValueError:
                throw(TypeError, "Invalid value for 'min' argument for attribute %s: %r" % (converter.attr, min_val))
        max_val = kwargs.pop('max', None)
        if max_val is not None:
            try: max_val = float(max_val)
            except ValueError:
                throw(TypeError, "Invalid value for 'max' argument for attribute %s: %r" % (converter.attr, max_val))
        converter.min_val = min_val
        converter.max_val = max_val
        converter.tolerance = kwargs.pop('tolerance', converter.default_tolerance)
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
    def __init__(converter, py_type, attr=None):
        converter.exp = None  # for the case when attr is None
        Converter.__init__(converter, py_type, attr)
    def init(converter, kwargs):
        attr = converter.attr
        args = attr.args
        if len(args) > 2: throw(TypeError, 'Too many positional parameters for Decimal '
                                           '(expected: precision and scale), got: %s' % args)
        if args: precision = args[0]
        else: precision = kwargs.pop('precision', 12)
        if not isinstance(precision, (int, long)):
            throw(TypeError, "'precision' positional argument for attribute %s must be int. Got: %r" % (attr, precision))
        if precision <= 0: throw(TypeError,
            "'precision' positional argument for attribute %s must be positive. Got: %r" % (attr, precision))

        if len(args) == 2: scale = args[1]
        else: scale = kwargs.pop('scale', 2)
        if not isinstance(scale, (int, long)):
            throw(TypeError, "'scale' positional argument for attribute %s must be int. Got: %r" % (attr, scale))
        if scale <= 0: throw(TypeError,
            "'scale' positional argument for attribute %s must be positive. Got: %r" % (attr, scale))

        if scale > precision: throw(ValueError, "'scale' must be less or equal 'precision'")
        converter.precision = precision
        converter.scale = scale
        converter.exp = Decimal(10) ** -scale

        min_val = kwargs.pop('min', None)
        if min_val is not None:
            try: min_val = Decimal(min_val)
            except TypeError: throw(TypeError,
                "Invalid value for 'min' argument for attribute %s: %r" % (attr, min_val))

        max_val = kwargs.pop('max', None)
        if max_val is not None:
            try: max_val = Decimal(max_val)
            except TypeError: throw(TypeError,
                "Invalid value for 'max' argument for attribute %s: %r" % (attr, max_val))

        converter.min_val = min_val
        converter.max_val = max_val
    def validate(converter, val):
        if isinstance(val, float):
            s = str(val)
            if float(s) != val: s = repr(val)
            val = Decimal(s)
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
    def validate(converter, val):
        if isinstance(val, datetime): return val.date()
        if isinstance(val, date): return val
        if isinstance(val, basestring): return str2date(val)
        throw(TypeError, "Attribute %r: expected type is 'date'. Got: %r" % (converter.attr, val))
    def sql2py(converter, val):
        if not isinstance(val, date): throw(ValueError,
            'Value of unexpected type received from database: instead of date got %s' % type(val))
        return val
    def sql_type(converter):
        return 'DATE'

class DatetimeConverter(Converter):
    sql_type_name = 'DATETIME'
    def __init__(converter, py_type, attr=None):
        converter.precision = None  # for the case when attr is None
        Converter.__init__(converter, py_type, attr)
    def init(converter, kwargs):
        attr = converter.attr
        args = attr.args        
        if len(args) > 1: throw(TypeError, 'Too many positional parameters for datetime attribute %s. '
                                           'Expected: precision, got: %r' % (attr, args))
        provider = attr.entity._database_.provider
        if args:
            precision = args[0]
            if 'precision' in kwargs: throw(TypeError,
                'Precision for datetime attribute %s has both positional and keyword value' % attr)
        else: precision = kwargs.pop('precision', provider.default_time_precision)
        if not isinstance(precision, int) or not 0 <= precision <= 6: throw(ValueError,
            'Precision value of datetime attribute %s must be between 0 and 6. Got: %r' % (attr, precision))
        if precision > provider.max_time_precision: throw(ValueError,
            'Precision value (%d) of attribute %s exceeds max datetime precision (%d) of %s %s'
            % (precision, attr, provider.max_time_precision, provider.dialect, provider.server_version))
        converter.precision = precision
    def validate(converter, val):
        if isinstance(val, datetime): pass
        elif isinstance(val, basestring): val = str2datetime(val)
        else: throw(TypeError, "Attribute %r: expected type is 'datetime'. Got: %r" % (converter.attr, val))
        p = converter.precision
        if not p: val = val.replace(microsecond=0)
        elif p == 6: pass
        else:
            rounding = 10 ** (6-p)
            microsecond = (val.microsecond // rounding) * rounding
            val = val.replace(microsecond=microsecond)
        return val
    def sql2py(converter, val):
        if not isinstance(val, datetime): throw(ValueError,
            'Value of unexpected type received from database: instead of datetime got %s' % type(val))
        return val
    def sql_type(converter):
        attr = converter.attr
        precision = converter.precision
        if not attr or precision == attr.entity._database_.provider.default_time_precision:
            return converter.sql_type_name
        return converter.sql_type_name + '(%d)' % precision

class UuidConverter(Converter):
    def __init__(converter, py_type, attr=None):
        if attr is not None and attr.auto:
            attr.auto = False
            if not attr.default: attr.default = uuid4
        Converter.__init__(converter, py_type, attr)
    def validate(converter, val):
        if isinstance(val, UUID): return val
        if isinstance(val, buffer): return UUID(bytes=val)
        if isinstance(val, basestring):
            if len(val) == 16: return UUID(bytes=val)
            return UUID(hex=val)
        if isinstance(val, int): return UUID(int=val)
        if converter.attr is not None:
            throw(ValueError, 'Value type of attribute %s must be UUID. Got: %r'
                               % (converter.attr, type(val)))
        else: throw(ValueError, 'Expected UUID value, got: %r' % type(val))
    def py2sql(converter, val):
        return buffer(val.bytes)
    sql2py = validate
    def sql_type(converter):
        return "UUID"
