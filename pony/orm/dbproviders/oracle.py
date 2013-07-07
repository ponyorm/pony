import os
os.environ["NLS_LANG"] = "AMERICAN_AMERICA.UTF8"

from types import NoneType
from datetime import date, datetime
from decimal import Decimal
from uuid import UUID

import cx_Oracle

from pony.orm import core, dbschema, sqlbuilding, dbapiprovider, sqltranslation
from pony.orm.core import log_orm, log_sql, DatabaseError
from pony.orm.dbapiprovider import DBAPIProvider, wrap_dbapi_exceptions, get_version_tuple
from pony.utils import throw

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
            if core.debug: log_sql(sql)
            cursor = connection.cursor()
            try: provider.execute(cursor, sql)
            except DatabaseError, e:
                if e.original_exc.args[0].code != 955: raise
                if core.debug: log_orm('ALREADY EXISTS: %s' % e.args[0].message)
                if i: continue
                if len(commands) > 1:
                    log_orm('SKIP FURTHER DDL COMMANDS FOR TABLE %s\n' % table.name)
                return
    def get_create_commands(table, created_tables=None):
        result = dbschema.Table.get_create_commands(table, created_tables)
        for column in table.column_list:
            if column.is_pk == 'auto':
                quote_name = table.schema.provider.quote_name
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
    dialect = 'Oracle'
    table_class = OraTable
    column_class = OraColumn

class OraNoneMonad(sqltranslation.NoneMonad):
    def __init__(monad, translator, value=None):
        assert value in (None, '')
        sqltranslation.ConstMonad.__init__(monad, translator, None)

class OraConstMonad(sqltranslation.ConstMonad):
    @staticmethod
    def new(translator, value):
        if value == '': value = None
        return sqltranslation.ConstMonad.new(translator, value)    

class OraTranslator(sqltranslation.SQLTranslator):
    dialect = 'Oracle'
    NoneMonad = OraNoneMonad
    ConstMonad = OraConstMonad

    @classmethod
    def get_normalized_type_of(translator, value):
        if value == '': return NoneType
        return sqltranslation.SQLTranslator.get_normalized_type_of(value)

class OraBuilder(sqlbuilding.SQLBuilder):
    dialect = 'Oracle'
    def INSERT(builder, table_name, columns, values, returning=None):
        result = sqlbuilding.SQLBuilder.INSERT(builder, table_name, columns, values)
        if returning is not None:
            result.extend((' RETURNING ', builder.quote_name(returning), ' INTO :new_id'))
        return result
    def SELECT(builder, *sections):
        last_section = sections[-1]
        limit = offset = None
        if last_section[0] == 'LIMIT':
            limit = last_section[1]
            if len(last_section) > 2: offset = last_section[2]
            sections = sections[:-1]
        result = builder.subquery(*sections)
        indent = builder.indent_spaces * builder.indent
        if not limit: pass
        elif not offset:
            result = [ 'SELECT * FROM (\n' ]
            builder.indent += 1
            result.extend(builder.subquery(*sections))
            builder.indent -= 1
            result.extend((indent, ') WHERE ROWNUM <= ', builder(limit)))
        else:
            indent2 = indent + builder.indent_spaces
            result = [ 'SELECT * FROM (\n', indent2, 'SELECT t.*, ROWNUM "row-num" FROM (\n' ]
            builder.indent += 2
            result.extend(builder.subquery(*sections))
            builder.indent -= 2
            result.extend((indent2, ') t '))
            if limit[0] == 'VALUE' and offset[0] == 'VALUE' \
                    and isinstance(limit[1], int) and isinstance(offset[1], int):
                total_limit = [ 'VALUE', limit[1] + offset[1] ]
                result.extend(('WHERE ROWNUM <= ', builder(total_limit), '\n'))
            else: result.extend(('WHERE ROWNUM <= ', builder(limit), ' + ', builder(offset), '\n'))
            result.extend((indent, ') WHERE "row-num" > ', builder(offset)))
        if builder.indent:
            indent = builder.indent_spaces * builder.indent
            return '(\n', result, indent + ')'
        return result
    def LIMIT(builder, limit, offset=None):
        assert False
        if not offset: return 'LIMIT ', builder(limit), '\n'
        else: return 'LIMIT ', builder(limit), ' OFFSET ', builder(offset), '\n'
    def DATE(builder, expr):
        return 'TRUNC(', builder(expr), ')'

class OraBoolConverter(dbapiprovider.BoolConverter):
    def sql2py(converter, val):
        return bool(val)  # TODO: True/False, T/F, Y/N, Yes/No, etc.
    def sql_type(converter):
        return "NUMBER(1)"

def _string_sql_type(converter):
    if converter.max_len:
        return 'VARCHAR2(%d CHAR)' % converter.max_len
    return 'CLOB'

class OraUnicodeConverter(dbapiprovider.UnicodeConverter):
    def validate(converter, val):
        if val == '': return None
        return dbapiprovider.UnicodeConverter.validate(converter, val)
    def sql2py(converter, val):
        if isinstance(val, cx_Oracle.LOB):
            val = val.read()
            val = val.decode('utf8')
        return val
    sql_type = _string_sql_type  # TODO: Add support for NVARCHAR2 and NCLOB datatypes

class OraStrConverter(dbapiprovider.StrConverter):
    def validate(converter, val):
        if val == '': return None
        return dbapiprovider.StrConverter.validate(converter, val)
    def sql2py(converter, val):
        if isinstance(val, cx_Oracle.LOB):
            val = val.read()
            if converter.utf8: return val
            val = val.decode('utf8')
        assert isinstance(val, unicode)
        val = val.encode(converter.encoding, 'replace')
        return val
    sql_type = _string_sql_type

class OraIntConverter(dbapiprovider.IntConverter):
    def sql_type(converter):
        return 'NUMBER(38)'

class OraRealConverter(dbapiprovider.RealConverter):
    default_tolerance = 1e-14
    def sql_type(converter):
        return 'NUMBER'

class OraDecimalConverter(dbapiprovider.DecimalConverter):
    def sql_type(converter):
        return 'NUMBER(%d, %d)' % (converter.precision, converter.scale)

class OraBlobConverter(dbapiprovider.BlobConverter):
    def sql2py(converter, val):
        return buffer(val.read())

class OraDateConverter(dbapiprovider.DateConverter):
    def sql2py(converter, val):
        if isinstance(val, datetime): return val.date()
        if not isinstance(val, date): throw(ValueError,
            'Value of unexpected type received from database: instead of date got %s', type(val))
        return val

class OraDatetimeConverter(dbapiprovider.DatetimeConverter):
    sql_type_name = 'TIMESTAMP'

class OraUuidConverter(dbapiprovider.UuidConverter):
    def sql_type(converter):
        return 'RAW(16)'

class OraProvider(DBAPIProvider):
    dialect = 'Oracle'
    paramstyle = 'named'
    max_name_len = 30

    table_if_not_exists_syntax = False
    index_if_not_exists_syntax = False

    dbapi_module = cx_Oracle
    dbschema_cls = OraSchema
    translator_cls = OraTranslator
    sqlbuilder_cls = OraBuilder

    def inspect_connection(provider, connection):
        sql = "select version from product_component_version where product like 'Oracle Database %'"
        cursor = connection.cursor()
        cursor.execute(sql)
        row = cursor.fetchone()
        assert row is not None
        provider.server_version = get_version_tuple(row[0])

    def should_reconnect(provider, exc):
        reconnect_error_codes = (
            3113,  # ORA-03113: end-of-file on communication channel
            3114,  # ORA-03114: not connected to ORACLE
            )
        return isinstance(exc, cx_Oracle.OperationalError) \
               and exc.args[0].code in reconnect_error_codes

    def get_default_entity_table_name(provider, entity):
        return DBAPIProvider.get_default_entity_table_name(provider, entity).upper()

    def get_default_m2m_table_name(provider, attr, reverse):
        return DBAPIProvider.get_default_m2m_table_name(provider, attr, reverse).upper()

    def get_default_column_names(provider, attr, reverse_pk_columns=None):
        return [ column.upper() for column in DBAPIProvider.get_default_column_names(provider, attr, reverse_pk_columns) ]

    def get_default_m2m_column_names(provider, entity):
        return [ column.upper() for column in DBAPIProvider.get_default_m2m_column_names(provider, entity) ]

    def get_default_index_name(*args, **kwargs):
        return DBAPIProvider.get_default_index_name(*args, **kwargs).upper()

    @wrap_dbapi_exceptions
    def execute(provider, cursor, sql, arguments=None, returning_id=False):
        if type(arguments) is list:
            assert arguments and not returning_id
            set_input_sizes(cursor, arguments[0])
            cursor.executemany(sql, arguments)
        else:
            if arguments is not None: set_input_sizes(cursor, arguments)
            if returning_id:
                var = cursor.var(cx_Oracle.STRING, 40, cursor.arraysize, outconverter=int)
                arguments['new_id'] = var
                if arguments is None: cursor.execute(sql)
                else: cursor.execute(sql, arguments)
                return var.getvalue()
            if arguments is None: cursor.execute(sql)
            else: cursor.execute(sql, arguments)

    converter_classes = [
        (bool, OraBoolConverter),
        (unicode, OraUnicodeConverter),
        (str, OraStrConverter),
        ((int, long), OraIntConverter),
        (float, OraRealConverter),
        (Decimal, OraDecimalConverter),
        (buffer, OraBlobConverter),
        (datetime, OraDatetimeConverter),
        (date, OraDateConverter),
        (UUID, OraUuidConverter),
    ]

    def get_pool(provider, *args, **kwargs):
        user = password = dsn = None
        if len(args) == 1:
            conn_str = args[0]
            if '/' in conn_str:
                user, tail = conn_str.split('/', 1)
                if '@' in tail: password, dsn = tail.split('@', 1)
            if None in (user, password, dsn): throw(ValueError,
                "Incorrect connection string (must be in form of 'user/password@dsn')")
        elif len(args) == 2: user, password = args
        elif len(args) == 3: user, password, dsn = args
        elif args: throw(ValueError, 'Invalid number of positional arguments')
        if user != kwargs.setdefault('user', user):
            throw(ValueError, 'Ambiguous value for user')
        if password != kwargs.setdefault('password', password):
            throw(ValueError, 'Ambiguous value for password')
        if dsn != kwargs.setdefault('dsn', dsn):
            throw(ValueError, 'Ambiguous value for dsn')
        kwargs.setdefault('threaded', True)
        kwargs.setdefault('min', 1)
        kwargs.setdefault('max', 10)
        kwargs.setdefault('increment', 1)
        return OraPool(**kwargs)

provider_cls = OraProvider

def to_int_or_decimal(val):
    val = val.replace(',', '.')
    if '.' in val: return Decimal(val)
    return int(val)

def to_decimal(val):
    return Decimal(val.replace(',', '.'))

def output_type_handler(cursor, name, defaultType, size, precision, scale):
    if defaultType == cx_Oracle.NUMBER:
        if scale == 0:
            if precision: return cursor.var(cx_Oracle.STRING, 40, cursor.arraysize, outconverter=int)
            return cursor.var(cx_Oracle.STRING, 40, cursor.arraysize, outconverter=to_int_or_decimal)
        if scale != -127:
            return cursor.var(cx_Oracle.STRING, 100, cursor.arraysize, outconverter=to_decimal)
    elif defaultType in (cx_Oracle.STRING, cx_Oracle.FIXED_CHAR):
        return cursor.var(unicode, size, cursor.arraysize)  # from cx_Oracle example
    return None

class OraPool(object):
    def __init__(pool, **kwargs):
        pool._pool = cx_Oracle.SessionPool(**kwargs)
    def connect(pool):
        con = pool._pool.acquire()
        con.outputtypehandler = output_type_handler
        return con
    def release(pool, con):
        pool._pool.release(con)
    def drop(pool, con):
        pool._pool.drop(con)

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
    else: assert False, type(arguments)
