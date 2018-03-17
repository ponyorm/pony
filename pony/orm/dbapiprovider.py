from __future__ import absolute_import, print_function, division
from pony.py23compat import PY2, basestring, unicode, buffer, int_types

import os, re, json
from decimal import Decimal, InvalidOperation
from datetime import datetime, date, time, timedelta
from uuid import uuid4, UUID
from time import time as get_time
from hashlib import md5
from functools import partial

import pony
from pony.utils import is_utf8, decorator, throw, localbase, deprecated
from pony.converting import str2date, str2time, str2datetime, str2timedelta
from pony.orm.ormtypes import LongStr, LongUnicode, RawSQLType, TrackedValue, Json

class DBException(Exception):
    def __init__(exc, original_exc, *args):
        args = args or getattr(original_exc, 'args', ())
        Exception.__init__(exc, *args)
        exc.original_exc = original_exc

# Exception inheritance layout of DBAPI 2.0-compatible provider:
#
# Exception
#   Warning
#   Error
#     InterfaceError
#     DatabaseError
#       DataError
#       OperationalError
#       IntegrityError
#       InternalError
#       ProgrammingError
#       NotSupportedError

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
    try:
        if provider.dialect != 'SQLite':
            return func(provider, *args, **kwargs)
        else:
            provider.local_exceptions.keep_traceback = True
            try: return func(provider, *args, **kwargs)
            finally: provider.local_exceptions.keep_traceback = False
    except dbapi_module.NotSupportedError as e: raise NotSupportedError(e)
    except dbapi_module.ProgrammingError as e: raise ProgrammingError(e)
    except dbapi_module.InternalError as e: raise InternalError(e)
    except dbapi_module.IntegrityError as e: raise IntegrityError(e)
    except dbapi_module.OperationalError as e:
        if provider.dialect == 'SQLite': provider.restore_exception()
        raise OperationalError(e)
    except dbapi_module.DataError as e: raise DataError(e)
    except dbapi_module.DatabaseError as e: raise DatabaseError(e)
    except dbapi_module.InterfaceError as e:
        if e.args == (0, '') and getattr(dbapi_module, '__name__', None) == 'MySQLdb':
            throw(InterfaceError, e, 'MySQL server misconfiguration')
        raise InterfaceError(e)
    except dbapi_module.Error as e: raise Error(e)
    except dbapi_module.Warning as e: raise Warning(e)

def unexpected_args(attr, args):
    throw(TypeError, 'Unexpected positional argument{} for attribute {}: {}'.format(
        len(args) > 1 and 's' or '', attr, ', '.join(repr(arg) for arg in args))
    )

class Name(unicode):
    __slots__ = ['obsolete_name']
    def __new__(cls, name, obsolete_name=None):
        result = unicode.__new__(cls, name)
        result.obsolete_name = obsolete_name
        return result

def obsolete(name):
    return getattr(name, 'obsolete_name', name)

class DBAPIProvider(object):
    paramstyle = 'qmark'
    quote_char = '"'
    max_params_count = 200
    max_name_len = 128
    table_if_not_exists_syntax = True
    index_if_not_exists_syntax = True
    max_time_precision = default_time_precision = 6
    uint64_support = False
    select_for_update_nowait_syntax = True

    # SQLite and PostgreSQL does not limit varchar max length.
    varchar_default_max_len = None

    dialect = None
    dbapi_module = None
    dbschema_cls = None
    translator_cls = None
    sqlbuilder_cls = None

    name_before_table = 'schema_name'
    default_schema_name = None

    fk_types = { 'SERIAL' : 'INTEGER', 'BIGSERIAL' : 'BIGINT' }

    pony_version_table_create_sql = 'CREATE TABLE pony_version (id INT PRIMARY KEY, pony_version VARCHAR(20) NOT NULL)'

    table_has_data_sql_template = "SELECT 1 FROM %(table_name)s LIMIT 1"
    drop_table_sql_template = "DROP TABLE %(table_name)s"
    rename_table_sql_template = "ALTER TABLE %(prev_name)s RENAME TO %(new_name)s"

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

    def get_pony_version(provider, connection):
        cursor = connection.cursor()
        if provider.table_exists(cursor, 'pony_version', case_sensitive=False):
            cursor.execute('select pony_version from pony_version where id = 1')
            row = cursor.fetchone()
            if row is not None: return row[0]
        return None

    def make_pony_version_table(provider, connection, version=None):
        cursor = connection.cursor()
        provider.execute(cursor, provider.pony_version_table_create_sql)
        insert_sql = "insert into pony_version values (1, '%s')" % (version or pony.__version__)
        provider.execute(cursor, insert_sql)

    def update_pony_version(provider, connection, to_version=None):
        cursor = connection.cursor()
        update_sql = "update pony_version set pony_version = '%s' where id = 1" % (to_version or pony.__version__)
        provider.execute(cursor, update_sql)

    def normalize_name_case(provider, name):
        return name.lower()

    def normalize_name(provider, name, obsolete_name=None):
        name = provider.normalize_name_case(name)
        if obsolete_name is not None:
            obsolete_name = provider.normalize_name_case(obsolete_name)
        max_len = provider.max_name_len
        if len(name) > max_len:
            hash = md5(name.encode('utf-8')).hexdigest()[:8]
            normalized_name = '%s_%s' % (name[:max_len-9], hash)
        else: normalized_name = name
        normalized_obsolete_name = (obsolete_name or name)[:max_len]
        return Name(normalized_name, obsolete_name=normalized_obsolete_name)

    def get_default_entity_table_name(provider, entity):
        return provider.normalize_name(entity.__name__)

    def get_default_m2m_table_name(provider, attr, reverse):
        if attr.symmetric:
            assert reverse is attr
            entity = attr.entity
            name = obsolete_name = '%s_%s' % (entity.__name__, attr.name)
        else:
            first_attr = sorted((attr, reverse), key=lambda attr: (attr.entity.__name__, attr.name))[0]
            entity = first_attr.entity
            name = '%s_%s' % (first_attr.entity.__name__, first_attr.name)
            obsolete_name = '%s_%s' % tuple(sorted((attr.entity.__name__, reverse.entity.__name__)))
        name = provider.normalize_name(name, obsolete_name=obsolete_name)
        if isinstance(entity._table_, tuple):
            schema_name, base_name = provider.split_table_name(entity._table_)
            name = schema_name, name
        return name

    def get_default_column_names(provider, attr, reverse_pk_columns=None):
        normalize = provider.normalize_name
        if reverse_pk_columns is None:
            return (normalize(attr.name),)
        elif len(reverse_pk_columns) == 1:
            return (normalize(attr.name),)
        else:
            prefix = attr.name + '_'
            return tuple(normalize(prefix + column) for column in reverse_pk_columns)

    def get_default_m2m_column_names(provider, entity):
        normalize = provider.normalize_name
        columns = entity._get_pk_columns_()
        if len(columns) == 1:
            return (normalize(entity.__name__),)
        else:
            prefix = entity.__name__ + '_'
            return tuple(normalize(prefix + column) for column in columns)

    def get_default_index_name(provider, table_name, column_names, is_pk=False, is_unique=False, m2m=False):
        table_name = provider.base_name(table_name)
        name = provider._get_default_index_name(table_name, column_names, is_pk, is_unique, m2m)
        obsolete_name = provider._get_default_index_name(obsolete(table_name), column_names, is_pk, is_unique, m2m)
        return provider.normalize_name(name, obsolete_name)

    def _get_default_index_name(provider, table_name, column_names, is_pk=False, is_unique=False, m2m=False):
        if is_pk: index_name = 'pk_%s' % table_name
        else:
            if is_unique: template = 'unq_%(tname)s__%(cnames)s'
            elif m2m: template = 'idx_%(tname)s'
            else: template = 'idx_%(tname)s__%(cnames)s'
            index_name = template % dict(tname=provider.base_name(table_name),
                                         cnames='_'.join(name for name in column_names))
        return index_name

    def get_default_fk_name(provider, table_name, col_names):
        fk_name = provider._get_default_fk_name(table_name, col_names)
        obsolete_fk_name = provider._get_default_fk_name(
            obsolete(table_name), [obsolete(col_name) for col_name in col_names])
        return provider.normalize_name(fk_name, obsolete_fk_name)

    def _get_default_fk_name(provider, table_name, col_names):
        return 'fk_%s__%s' % (provider.base_name(table_name), '__'.join(col_names))

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

    def base_name(provider, name):
        if not isinstance(name, basestring):
            assert type(name) is tuple
            name = name[-1]
            assert isinstance(name, basestring)
        return name

    def quote_name(provider, name):
        quote_char = provider.quote_char
        if isinstance(name, basestring):
            name = name.replace(quote_char, quote_char+quote_char)
            return quote_char + name + quote_char
        return '.'.join(provider.quote_name(item) for item in name)

    def format_table_name(provider, name):
        return provider.quote_name(name)

    def normalize_vars(provider, vars, vartypes):
        pass

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
    def commit(provider, connection, cache=None):
        core = pony.orm.core
        if core.local.debug: core.log_orm('COMMIT')
        connection.commit()
        if cache is not None: cache.in_transaction = False

    @wrap_dbapi_exceptions
    def rollback(provider, connection, cache=None):
        core = pony.orm.core
        if core.local.debug: core.log_orm('ROLLBACK')
        connection.rollback()
        if cache is not None: cache.in_transaction = False

    @wrap_dbapi_exceptions
    def release(provider, connection, cache=None):
        core = pony.orm.core
        if cache is not None and cache.db_session is not None and cache.db_session.ddl:
            provider.drop(connection, cache)
        else:
            if core.local.debug: core.log_orm('RELEASE CONNECTION')
            provider.pool.release(connection)

    @wrap_dbapi_exceptions
    def drop(provider, connection, cache=None):
        core = pony.orm.core
        if core.local.debug: core.log_orm('CLOSE CONNECTION')
        provider.pool.drop(connection)
        if cache is not None: cache.in_transaction = False

    @wrap_dbapi_exceptions
    def disconnect(provider):
        core = pony.orm.core
        if core.local.debug: core.log_orm('DISCONNECT')
        provider.pool.disconnect()

    @wrap_dbapi_exceptions
    def execute(provider, cursor, sql, arguments=None, returning_id=False):
        core = pony.orm.core
        if core.local.debug: core.log_sql(sql, arguments)
        start_time = get_time()
        new_id = provider._execute(cursor, sql, arguments, returning_id)
        return new_id, get_time() - start_time

    def _execute(provider, cursor, sql, arguments=None, returning_id=False):
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
        if isinstance(py_type, RawSQLType):
            return Converter  # for cases like select(raw_sql(...) for x in X)
        throw(TypeError, 'No database converter found for type %s' % py_type)

    def get_converter_by_py_type(provider, py_type):
        converter_cls = provider._get_converter_type_by_py_type(py_type)
        return converter_cls(provider, py_type)

    def get_converter_by_attr(provider, attr):
        py_type = attr.py_type
        converter_cls = provider._get_converter_type_by_py_type(py_type)
        return converter_cls(provider, py_type, attr)

    def get_pool(provider, *args, **kwargs):
        return Pool(provider.dbapi_module, *args, **kwargs)

    def table_exists(provider, cursor, table_name, case_sensitive=True):
        throw(NotImplementedError)

    def index_exists(provider, cursor, table_name, index_name, case_sensitive=True):
        throw(NotImplementedError)

    def fk_exists(provider, cursor, table_name, fk_name, case_sensitive=True):
        throw(NotImplementedError)

    def table_has_data(provider, cursor, table_name):
        table_name = provider.quote_name(table_name)
        cursor.execute(provider.table_has_data_sql_template % dict(table_name=table_name))
        return cursor.fetchone() is not None

    def disable_fk_checks(provider, cursor):
        pass

    def enable_fk_checks(provider, cursor, prev_state):
        pass

    def drop_table(provider, cursor, table_name):
        table_name = provider.quote_name(table_name)
        sql = provider.drop_table_sql_template % dict(table_name=table_name)
        provider.execute(cursor, sql)

    def rename_table(provider, cursor, prev_name, new_name):
        quote_name = provider.quote_name
        prev_name = quote_name(prev_name)
        new_name = quote_name(new_name)
        sql = provider.rename_table_sql_template % dict(prev_name=prev_name, new_name=new_name)
        provider.execute(cursor, sql)

class Pool(localbase):
    forked_connections = []
    def __init__(pool, dbapi_module, *args, **kwargs): # called separately in each thread
        pool.dbapi_module = dbapi_module
        pool.args = args
        pool.kwargs = kwargs
        pool.con = pool.pid = None
    def connect(pool):
        pid = os.getpid()
        if pool.con is not None and pool.pid != pid:
            pool.forked_connections.append((pool.con, pool.pid))
            pool.con = pool.pid = None
        core = pony.orm.core
        if pool.con is None:
            if core.local.debug: core.log_orm('GET NEW CONNECTION')
            pool._connect()
            pool.pid = pid
        elif core.local.debug: core.log_orm('GET CONNECTION FROM THE LOCAL POOL')
        return pool.con
    def _connect(pool):
        pool.con = pool.dbapi_module.connect(*pool.args, **pool.kwargs)
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
    EQ = 'EQ'
    NE = 'NE'
    optimistic = True
    def __deepcopy__(converter, memo):
        return converter  # Converter instances are "immutable"
    def __init__(converter, provider, py_type, attr=None):
        converter.provider = provider
        converter.py_type = py_type
        converter.attr = attr
        if attr is None: return
        kwargs = attr.kwargs.copy()
        converter.init(kwargs)
        for option in kwargs: throw(TypeError, 'Attribute %s has unknown option %r' % (attr, option))
    def init(converter, kwargs):
        attr = converter.attr
        if attr and attr.args: unexpected_args(attr, attr.args)
    def validate(converter, val, obj=None):
        return val
    def py2sql(converter, val):
        return val
    def sql2py(converter, val):
        return val
    def val2dbval(self, val, obj=None):
        return val
    def dbval2val(self, dbval, obj=None):
        return dbval
    def dbvals_equal(self, x, y):
        return x == y
    def get_sql_type(converter, attr=None):
        if attr is not None and attr.sql_type is not None:
            return attr.sql_type
        attr = converter.attr
        if attr.sql_type is not None:
            assert len(attr.columns) == 1
            return converter.get_fk_type(attr.sql_type)
        if attr is not None and attr.reverse and not attr.is_collection:
            i = attr.converters.index(converter)
            rentity = attr.reverse.entity
            rpk_converters = rentity._pk_converters_
            assert rpk_converters is not None and len(attr.converters) == len(rpk_converters)
            rconverter = rpk_converters[i]
            return rconverter.sql_type()
        return converter.sql_type()
    def get_fk_type(converter, sql_type):
        fk_types = converter.provider.fk_types
        if sql_type.isupper(): return fk_types.get(sql_type, sql_type)
        sql_type = sql_type.upper()
        return fk_types.get(sql_type, sql_type).lower()

class NoneConverter(Converter):  # used for raw_sql() parameters only
    def __init__(converter, provider, py_type, attr=None):
        if attr is not None: throw(TypeError, 'Attribute %s has invalid type NoneType' % attr)
        Converter.__init__(converter, provider, py_type)
    def get_sql_type(converter, attr=None):
        assert False
    def get_fk_type(converter, sql_type):
        assert False

class BoolConverter(Converter):
    def validate(converter, val, obj=None):
        return bool(val)
    def sql2py(converter, val):
        return bool(val)
    def sql_type(converter):
        return "BOOLEAN"

class StrConverter(Converter):
    def __init__(converter, provider, py_type, attr=None):
        converter.max_len = None
        converter.db_encoding = None
        Converter.__init__(converter, provider, py_type, attr)
    def init(converter, kwargs):
        attr = converter.attr
        max_len = kwargs.pop('max_len', None)
        if len(attr.args) > 1: unexpected_args(attr, attr.args[1:])
        elif attr.args:
            if max_len is not None: throw(TypeError,
                'Max length option specified twice: as a positional argument and as a `max_len` named argument')
            max_len = attr.args[0]
        if issubclass(attr.py_type, (LongStr, LongUnicode)):
            if max_len is not None: throw(TypeError, 'Max length is not supported for CLOBs')
        elif max_len is None: max_len = converter.provider.varchar_default_max_len
        elif not isinstance(max_len, int_types):
            throw(TypeError, 'Max length argument must be int. Got: %r' % max_len)
        converter.max_len = max_len
        converter.db_encoding = kwargs.pop('db_encoding', None)
        converter.autostrip = kwargs.pop('autostrip', True)
    def validate(converter, val, obj=None):
        if PY2 and isinstance(val, str): val = val.decode('ascii')
        elif not isinstance(val, unicode): throw(TypeError,
            'Value type for attribute %s must be %s. Got: %r' % (converter.attr, unicode.__name__, type(val)))
        if converter.autostrip: val = val.strip()
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

class IntConverter(Converter):
    signed_types = {None: 'INTEGER', 8: 'TINYINT', 16: 'SMALLINT', 24: 'MEDIUMINT', 32: 'INTEGER', 64: 'BIGINT'}
    unsigned_types = None
    def init(converter, kwargs):
        Converter.init(converter, kwargs)
        attr = converter.attr

        min_val = kwargs.pop('min', None)
        if min_val is not None and not isinstance(min_val, int_types):
            throw(TypeError, "'min' argument for attribute %s must be int. Got: %r" % (attr, min_val))

        max_val = kwargs.pop('max', None)
        if max_val is not None and not isinstance(max_val, int_types):
            throw(TypeError, "'max' argument for attribute %s must be int. Got: %r" % (attr, max_val))

        size = kwargs.pop('size', None)
        if size is None:
            if attr.py_type.__name__ == 'long':
                deprecated(9, "Attribute %s: 'long' attribute type is deprecated. "
                              "Please use 'int' type with size=64 option instead" % attr)
                attr.py_type = int
                size = 64
        elif attr.py_type.__name__ == 'long': throw(TypeError,
            "Attribute %s: 'size' option cannot be used with long type. Please use int type instead" % attr)
        elif not isinstance(size, int_types):
            throw(TypeError, "'size' option for attribute %s must be of int type. Got: %r" % (attr, size))
        elif size not in (8, 16, 24, 32, 64):
            throw(TypeError, "incorrect value of 'size' option for attribute %s. "
                             "Should be 8, 16, 24, 32 or 64. Got: %d" % (attr, size))

        unsigned = kwargs.pop('unsigned', False)
        if unsigned is not None and not isinstance(unsigned, bool):
            throw(TypeError, "'unsigned' option for attribute %s must be of bool type. Got: %r" % (attr, unsigned))

        if size == 64 and unsigned and not converter.provider.uint64_support: throw(TypeError,
            'Attribute %s: %s provider does not support unsigned bigint type' % (attr, converter.provider.dialect))

        if unsigned is not None and size is None: size = 32
        lowest = highest = None
        if size:
            highest = highest = 2 ** size - 1 if unsigned else 2 ** (size - 1) - 1
            lowest = 0 if unsigned else -(2 ** (size - 1))

        if highest is not None and max_val is not None and max_val > highest:
            throw(ValueError, "'max' argument should be less or equal to %d because of size=%d and unsigned=%s. "
                              "Got: %d" % (highest, size, max_val, unsigned))

        if lowest is not None and min_val is not None and min_val < lowest:
            throw(ValueError, "'min' argument should be greater or equal to %d because of size=%d and unsigned=%s. "
                              "Got: %d" % (lowest, size, min_val, unsigned))

        converter.min_val = min_val or lowest
        converter.max_val = max_val or highest
        converter.size = size
        converter.unsigned = unsigned
    def validate(converter, val, obj=None):
        if isinstance(val, int_types): pass
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
        if not converter.unsigned:
            return converter.signed_types.get(converter.size)
        if converter.unsigned_types is None:
            return converter.signed_types.get(converter.size) + ' UNSIGNED'
        return converter.unsigned_types.get(converter.size)

class RealConverter(Converter):
    EQ = 'FLOAT_EQ'
    NE = 'FLOAT_NE'
    # The tolerance is necessary for Oracle, because it has different representation of float numbers.
    # For other databases the default tolerance is set because the precision can be lost during
    # Python -> JavaScript -> Python conversion
    default_tolerance = 1e-14
    optimistic = False
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
    def validate(converter, val, obj=None):
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
    def dbvals_equal(converter, x, y):
        tolerance = converter.tolerance
        if tolerance is None or x is None or y is None: return x == y
        denominator = max(abs(x), abs(y))
        if not denominator: return True
        diff = abs(x-y) / denominator
        return diff <= tolerance
    def sql2py(converter, val):
        return float(val)
    def sql_type(converter):
        return 'REAL'

class DecimalConverter(Converter):
    def __init__(converter, provider, py_type, attr=None):
        converter.exp = None  # for the case when attr is None
        Converter.__init__(converter, provider, py_type, attr)
    def init(converter, kwargs):
        attr = converter.attr
        args = attr.args
        if len(args) > 2: throw(TypeError, 'Too many positional parameters for Decimal '
                                           '(expected: precision and scale), got: %s' % args)
        if args: precision = args[0]
        else: precision = kwargs.pop('precision', 12)
        if not isinstance(precision, int_types):
            throw(TypeError, "'precision' positional argument for attribute %s must be int. Got: %r" % (attr, precision))
        if precision <= 0: throw(TypeError,
            "'precision' positional argument for attribute %s must be positive. Got: %r" % (attr, precision))

        if len(args) == 2: scale = args[1]
        else: scale = kwargs.pop('scale', 2)
        if not isinstance(scale, int_types):
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
    def validate(converter, val, obj=None):
        if isinstance(val, float):
            s = str(val)
            if float(s) != val: s = repr(val)
            val = Decimal(s)
        try: val = Decimal(val)
        except InvalidOperation as exc:
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
    def validate(converter, val, obj=None):
        if isinstance(val, buffer): return val
        if isinstance(val, str): return buffer(val)
        throw(TypeError, "Attribute %r: expected type is 'buffer'. Got: %r" % (converter.attr, type(val)))
    def sql2py(converter, val):
        if not isinstance(val, buffer):
            try: val = buffer(val)
            except: pass
        return val
    def sql_type(converter):
        return 'BLOB'

class DateConverter(Converter):
    def validate(converter, val, obj=None):
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

class ConverterWithMicroseconds(Converter):
    def __init__(converter, provider, py_type, attr=None):
        converter.precision = None  # for the case when attr is None
        Converter.__init__(converter, provider, py_type, attr)
    def init(converter, kwargs):
        attr = converter.attr
        args = attr.args
        if len(args) > 1: throw(TypeError, 'Too many positional parameters for attribute %s. '
                                           'Expected: precision, got: %r' % (attr, args))
        provider = attr.entity._database_.provider
        if args:
            precision = args[0]
            if 'precision' in kwargs: throw(TypeError,
                'Precision for attribute %s has both positional and keyword value' % attr)
        else: precision = kwargs.pop('precision', provider.default_time_precision)
        if not isinstance(precision, int) or not 0 <= precision <= 6: throw(ValueError,
            'Precision value of attribute %s must be between 0 and 6. Got: %r' % (attr, precision))
        if precision > provider.max_time_precision: throw(ValueError,
            'Precision value (%d) of attribute %s exceeds max datetime precision (%d) of %s %s'
            % (precision, attr, provider.max_time_precision, provider.dialect, provider.server_version))
        converter.precision = precision
    def round_microseconds_to_precision(converter, microseconds, precision):
        # returns None if no change is required
        if not precision: result = 0
        elif precision < 6:
            rounding = 10 ** (6-precision)
            result = (microseconds // rounding) * rounding
        else: return None
        return result if result != microseconds else None
    def sql_type(converter):
        attr = converter.attr
        precision = converter.precision
        if not attr or precision == attr.entity._database_.provider.default_time_precision:
            return converter.sql_type_name
        return converter.sql_type_name + '(%d)' % precision

class TimeConverter(ConverterWithMicroseconds):
    sql_type_name = 'TIME'
    def validate(converter, val, obj=None):
        if isinstance(val, time): pass
        elif isinstance(val, basestring): val = str2time(val)
        else: throw(TypeError, "Attribute %r: expected type is 'time'. Got: %r" % (converter.attr, val))
        mcs = converter.round_microseconds_to_precision(val.microsecond, converter.precision)
        if mcs is not None: val = val.replace(microsecond=mcs)
        return val
    def sql2py(converter, val):
        if not isinstance(val, time): throw(ValueError,
            'Value of unexpected type received from database: instead of time got %s' % type(val))
        return val

class TimedeltaConverter(ConverterWithMicroseconds):
    sql_type_name = 'INTERVAL'
    def validate(converter, val, obj=None):
        if isinstance(val, timedelta): pass
        elif isinstance(val, basestring): val = str2timedelta(val)
        else: throw(TypeError, "Attribute %r: expected type is 'timedelta'. Got: %r" % (converter.attr, val))
        mcs = converter.round_microseconds_to_precision(val.microseconds, converter.precision)
        if mcs is not None: val = timedelta(val.days, val.seconds, mcs)
        return val
    def sql2py(converter, val):
        if not isinstance(val, timedelta): throw(ValueError,
            'Value of unexpected type received from database: instead of time got %s' % type(val))
        return val

class DatetimeConverter(ConverterWithMicroseconds):
    sql_type_name = 'DATETIME'
    def validate(converter, val, obj=None):
        if isinstance(val, datetime): pass
        elif isinstance(val, basestring): val = str2datetime(val)
        else: throw(TypeError, "Attribute %r: expected type is 'datetime'. Got: %r" % (converter.attr, val))
        mcs = converter.round_microseconds_to_precision(val.microsecond, converter.precision)
        if mcs is not None: val = val.replace(microsecond=mcs)
        return val
    def sql2py(converter, val):
        if not isinstance(val, datetime): throw(ValueError,
            'Value of unexpected type received from database: instead of datetime got %s' % type(val))
        return val

class UuidConverter(Converter):
    def __init__(converter, provider, py_type, attr=None):
        if attr is not None and attr.auto:
            attr.auto = False
            if not attr.default: attr.default = uuid4
        Converter.__init__(converter, provider, py_type, attr)
    def validate(converter, val, obj=None):
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

class JsonConverter(Converter):
    json_kwargs = {}
    class JsonEncoder(json.JSONEncoder):
        def default(converter, obj):
            if isinstance(obj, Json):
                return obj.wrapped
            return json.JSONEncoder.default(converter, obj)
    def validate(converter, val, obj=None):
        if obj is None or converter.attr is None:
            return val
        if isinstance(val, TrackedValue) and val.obj_ref() is obj and val.attr is converter.attr:
            return val
        return TrackedValue.make(obj, converter.attr, val)
    def val2dbval(converter, val, obj=None):
        return json.dumps(val, cls=converter.JsonEncoder, **converter.json_kwargs)
    def dbval2val(converter, dbval, obj=None):
        if isinstance(dbval, (int, bool, float, type(None))):
            return dbval
        val = json.loads(dbval)
        if obj is None:
            return val
        return TrackedValue.make(obj, converter.attr, val)
    def dbvals_equal(converter, x, y):
        if x == y: return True  # optimization
        if isinstance(x, basestring): x = json.loads(x)
        if isinstance(y, basestring): y = json.loads(y)
        return x == y
    def sql_type(converter):
        return "JSON"
