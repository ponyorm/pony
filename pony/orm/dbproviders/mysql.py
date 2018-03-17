from __future__ import absolute_import
from pony.py23compat import PY2, imap, basestring, buffer, int_types

import json
from decimal import Decimal
from datetime import datetime, date, time, timedelta
from uuid import UUID

NoneType = type(None)

import warnings
warnings.filterwarnings('ignore', '^Table.+already exists$', Warning, '^pony\\.orm\\.dbapiprovider$')

try:
    import MySQLdb as mysql_module
    from MySQLdb import string_literal
    import MySQLdb.converters as mysql_converters
    from MySQLdb.constants import FIELD_TYPE, FLAG, CLIENT
    mysql_module_name = 'MySQLdb'
except ImportError:
    try:
        import pymysql as mysql_module
    except ImportError:
        raise ImportError('In order to use PonyORM with MySQL please install MySQLdb or pymysql')
    from pymysql.converters import escape_str as string_literal
    import pymysql.converters as mysql_converters
    from pymysql.constants import FIELD_TYPE, FLAG, CLIENT
    mysql_module_name = 'pymysql'

from pony.orm import core, dbschema, dbapiprovider, ormtypes, sqltranslation
from pony.orm.core import log_orm
from pony.orm.dbapiprovider import DBAPIProvider, Pool, wrap_dbapi_exceptions, obsolete
from pony.orm.sqltranslation import SQLTranslator, TranslationError
from pony.orm.sqlbuilding import Value, Param, SQLBuilder, join
from pony.utils import throw, get_version_tuple
from pony.converting import str2timedelta, timedelta2str

from pony.migrate.operations import Op, alter_table

class MySQLIndex(dbschema.DBIndex):
    rename_sql_template = 'ALTER TABLE %(table_name)s RENAME INDEX %(prev_name)s TO %(new_name)s'
    drop_sql_template = 'DROP INDEX %(name)s ON %(table_name)s'

    def get_drop_ops(index):
        table = index.table
        schema = table.schema
        quote_name = schema.provider.quote_name
        sql = 'DROP INDEX %s ON %s' % (quote_name(index.name), quote_name(table.name))
        return [ Op(sql, obj=index, type='drop') ]

    def can_be_renamed(index):
        return index.table.schema.provider.server_version >= (5, 7)

class MySQLForeignKey(dbschema.ForeignKey):
    drop_sql_template = 'ALTER TABLE %(table_name)s DROP FOREIGN KEY %(name)s'

    def can_be_renamed(fk):
        return False

class MySQLColumn(dbschema.Column):
    pk_constraint_template = ''
    auto_template = '%(type)s PRIMARY KEY AUTO_INCREMENT'
    rename_sql_template = 'ALTER TABLE %(table_name)s CHANGE %(prev_name)s %(new_col_def)s'

    def get_rename_ops(column):
        prev_table = column.table
        schema = prev_table.schema
        quote_name = schema.provider.quote_name
        prev_name = quote_name(column.name)
        sql = '%s %s %s' % ('CHANGE', prev_name, column.new.get_sql())
        return [ Op(sql, obj=column, type='rename', prefix=alter_table(prev_table)) ]

    def db_rename(column, cursor):
        schema = column.table.schema
        provider = schema.provider
        quote_name = provider.quote_name
        sql = column.rename_sql_template % dict(
            table_name=quote_name(column.table.name),
            prev_name=quote_name(obsolete(column.name)),
            new_col_def=column.get_sql())
        provider.execute(cursor, sql)

class MySQLSchema(dbschema.DBSchema):
    inline_fk_syntax = False
    column_class = MySQLColumn
    index_class = MySQLIndex
    fk_class = MySQLForeignKey
    ALTER_COLUMN = 'MODIFY COLUMN'

class MySQLTranslator(SQLTranslator):
    json_path_wildcard_syntax = True

class MySQLValue(Value):
    __slots__ = []
    def __unicode__(self):
        value = self.value
        if isinstance(value, datetime): return 'datetime ' + Value.__unicode__(self)
        if isinstance(value, date): return 'date ' + Value.__unicode__(self)
        return Value.__unicode__(self)
    if not PY2: __str__ = __unicode__

class MySQLBuilder(SQLBuilder):
    value_class = MySQLValue
    def ALTER_COLUMN_DEFAULT(builder, column):
        return 'ALTER COLUMN ', builder.quote_name(column), ' DROP DEFAULT'
    def CONCAT(builder, *args):
        return 'concat(',  join(', ', imap(builder, args)), ')'
    def TRIM(builder, expr, chars=None):
        if chars is None: return 'trim(', builder(expr), ')'
        return 'trim(both ', builder(chars), ' from ' ,builder(expr), ')'
    def LTRIM(builder, expr, chars=None):
        if chars is None: return 'ltrim(', builder(expr), ')'
        return 'trim(leading ', builder(chars), ' from ' ,builder(expr), ')'
    def RTRIM(builder, expr, chars=None):
        if chars is None: return 'rtrim(', builder(expr), ')'
        return 'trim(trailing ', builder(chars), ' from ' ,builder(expr), ')'
    def TO_INT(builder, expr):
        return 'CAST(', builder(expr), ' AS SIGNED)'
    def TO_REAL(builder, expr):
        return 'CAST(', builder(expr), ' AS DOUBLE)'
    def YEAR(builder, expr):
        return 'year(', builder(expr), ')'
    def MONTH(builder, expr):
        return 'month(', builder(expr), ')'
    def DAY(builder, expr):
        return 'day(', builder(expr), ')'
    def HOUR(builder, expr):
        return 'hour(', builder(expr), ')'
    def MINUTE(builder, expr):
        return 'minute(', builder(expr), ')'
    def SECOND(builder, expr):
        return 'second(', builder(expr), ')'
    def DATE_ADD(builder, expr, delta):
        if isinstance(delta, timedelta):
            return 'DATE_ADD(', builder(expr), ", INTERVAL '", timedelta2str(delta), "' HOUR_SECOND)"
        return 'ADDTIME(', builder(expr), ', ', builder(delta), ')'
    def DATE_SUB(builder, expr, delta):
        if isinstance(delta, timedelta):
            return 'DATE_SUB(', builder(expr), ", INTERVAL '", timedelta2str(delta), "' HOUR_SECOND)"
        return 'SUBTIME(', builder(expr), ', ', builder(delta), ')'
    def DATETIME_ADD(builder, expr, delta):
        if isinstance(delta, timedelta):
            return 'DATE_ADD(', builder(expr), ", INTERVAL '", timedelta2str(delta), "' HOUR_SECOND)"
        return 'ADDTIME(', builder(expr), ', ', builder(delta), ')'
    def DATETIME_SUB(builder, expr, delta):
        if isinstance(delta, timedelta):
            return 'DATE_SUB(', builder(expr), ", INTERVAL '", timedelta2str(delta), "' HOUR_SECOND)"
        return 'SUBTIME(', builder(expr), ', ', builder(delta), ')'
    def JSON_QUERY(builder, expr, path):
        path_sql, has_params, has_wildcards = builder.build_json_path(path)
        return 'json_extract(', builder(expr), ', ', path_sql, ')'
    def JSON_VALUE(builder, expr, path, type):
        path_sql, has_params, has_wildcards = builder.build_json_path(path)
        result = 'json_extract(', builder(expr), ', ', path_sql, ')'
        if type is NoneType:
            return 'NULLIF(', result, ", CAST('null' as JSON))"
        if type in (bool, int):
            return 'CAST(', result, ' AS SIGNED)'
        return 'json_unquote(', result, ')'
    def JSON_NONZERO(builder, expr):
        return 'COALESCE(CAST(', builder(expr), ''' as CHAR), 'null') NOT IN ('null', 'false', '0', '""', '[]', '{}')'''
    def JSON_ARRAY_LENGTH(builder, value):
        return 'json_length(', builder(value), ')'
    def JSON_EQ(builder, left, right):
        return '(', builder(left), ' = CAST(', builder(right), ' AS JSON))'
    def JSON_NE(builder, left, right):
        return '(', builder(left), ' != CAST(', builder(right), ' AS JSON))'
    def JSON_CONTAINS(builder, expr, path, key):
        key_sql = builder(key)
        if isinstance(key_sql, Value):
            wrapped_key = builder.value_class(builder.paramstyle, json.dumps([ key_sql.value ]))
        elif isinstance(key_sql, Param):
            wrapped_key = builder.make_composite_param(
                (key_sql.paramkey,), [key_sql], builder.wrap_param_to_json_array)
        else: assert False
        expr_sql = builder(expr)
        result = [ '(json_contains(', expr_sql, ', ', wrapped_key ]
        path_sql, has_params, has_wildcards = builder.build_json_path(path)
        if has_wildcards: throw(TranslationError, 'Wildcards are not allowed in json_contains()')
        path_with_key_sql, _, _ = builder.build_json_path(path + [key])
        result += [ ', ', path_sql, ') or json_contains_path(', expr_sql, ", 'one', ", path_with_key_sql, '))' ]
        return result
    @classmethod
    def wrap_param_to_json_array(cls, values):
        return json.dumps(values)
    def JSON_PARAM(builder, expr):
        return 'CAST(', builder(expr), ' AS JSON)'

class MySQLStrConverter(dbapiprovider.StrConverter):
    def sql_type(converter):
        result = 'VARCHAR(%d)' % converter.max_len if converter.max_len else 'LONGTEXT'
        if converter.db_encoding: result += ' CHARACTER SET %s' % converter.db_encoding
        return result

class MySQLRealConverter(dbapiprovider.RealConverter):
    def sql_type(converter):
        return 'DOUBLE'

class MySQLBlobConverter(dbapiprovider.BlobConverter):
    def sql_type(converter):
        return 'LONGBLOB'

class MySQLTimeConverter(dbapiprovider.TimeConverter):
    def sql2py(converter, val):
        if isinstance(val, timedelta):  # MySQLdb returns timedeltas instead of times
            total_seconds = val.days * (24 * 60 * 60) + val.seconds
            if 0 <= total_seconds <= 24 * 60 * 60:
                minutes, seconds = divmod(total_seconds, 60)
                hours, minutes = divmod(minutes, 60)
                return time(hours, minutes, seconds, val.microseconds)
        elif not isinstance(val, time): throw(ValueError,
            'Value of unexpected type received from database%s: instead of time or timedelta got %s'
            % ('for attribute %s' % converter.attr if converter.attr else '', type(val)))
        return val

class MySQLTimedeltaConverter(dbapiprovider.TimedeltaConverter):
    sql_type_name = 'TIME'

class MySQLUuidConverter(dbapiprovider.UuidConverter):
    def sql_type(converter):
        return 'BINARY(16)'

class MySQLJsonConverter(dbapiprovider.JsonConverter):
    EQ = 'JSON_EQ'
    NE = 'JSON_NE'
    def init(self, kwargs):
        if self.provider.server_version < (5, 7, 8):
            version = '.'.join(imap(str, self.provider.server_version))
            raise NotImplementedError("MySQL %s has no JSON support" % version)

class MySQLProvider(DBAPIProvider):
    dialect = 'MySQL'
    paramstyle = 'format'
    quote_char = "`"
    max_name_len = 64
    table_if_not_exists_syntax = True
    index_if_not_exists_syntax = False
    select_for_update_nowait_syntax = False
    max_time_precision = default_time_precision = 0
    varchar_default_max_len = 255
    uint64_support = True

    dbapi_module = mysql_module
    dbschema_cls = MySQLSchema
    translator_cls = MySQLTranslator
    sqlbuilder_cls = MySQLBuilder

    fk_types = { 'SERIAL' : 'BIGINT UNSIGNED' }

    converter_classes = [
        (NoneType, dbapiprovider.NoneConverter),
        (bool, dbapiprovider.BoolConverter),
        (basestring, MySQLStrConverter),
        (int_types, dbapiprovider.IntConverter),
        (float, MySQLRealConverter),
        (Decimal, dbapiprovider.DecimalConverter),
        (datetime, dbapiprovider.DatetimeConverter),
        (date, dbapiprovider.DateConverter),
        (time, MySQLTimeConverter),
        (timedelta, MySQLTimedeltaConverter),
        (UUID, MySQLUuidConverter),
        (buffer, MySQLBlobConverter),
        (ormtypes.Json, MySQLJsonConverter),
    ]

    @wrap_dbapi_exceptions
    def inspect_connection(provider, connection):
        cursor = connection.cursor()
        cursor.execute('select version()')
        row = cursor.fetchone()
        assert row is not None
        provider.server_version = get_version_tuple(row[0])
        if provider.server_version >= (5, 6, 4):
            provider.max_time_precision = 6
        cursor.execute('select database()')
        provider.default_schema_name = cursor.fetchone()[0]

    def should_reconnect(provider, exc):
        return isinstance(exc, mysql_module.OperationalError) and exc.args[0] == 2006

    def get_pool(provider, *args, **kwargs):
        if 'conv' not in kwargs:
            conv = mysql_converters.conversions.copy()
            if mysql_module_name == 'MySQLdb':
                conv[FIELD_TYPE.BLOB] = [(FLAG.BINARY, buffer)]
            else:
                if PY2:
                    def encode_buffer(val, encoders=None):
                        return string_literal(str(val), encoders)

                    conv[buffer] = encode_buffer

            def encode_timedelta(val, encoders=None):
                return string_literal(timedelta2str(val), encoders)

            conv[timedelta] = encode_timedelta
            conv[FIELD_TYPE.TIMESTAMP] = str2datetime
            conv[FIELD_TYPE.DATETIME] = str2datetime
            conv[FIELD_TYPE.TIME] = str2timedelta
            kwargs['conv'] = conv
        if 'charset' not in kwargs:
            kwargs['charset'] = 'utf8'
        kwargs['client_flag'] = kwargs.get('client_flag', 0) | CLIENT.FOUND_ROWS
        return Pool(mysql_module, *args, **kwargs)

    @wrap_dbapi_exceptions
    def set_transaction_mode(provider, connection, cache):
        assert not cache.in_transaction
        db_session = cache.db_session
        if db_session is not None and db_session.ddl:
            cursor = connection.cursor()
            cursor.execute("SHOW VARIABLES LIKE 'foreign_key_checks'")
            row = cursor.fetchone()
            val = row is not None and row[1] == 'ON'
            if val:
                sql = 'SET foreign_key_checks = 0'
                if core.local.debug: log_orm(sql)
                cursor.execute(sql)
            cache.saved_fk_state = bool(val)
            cache.in_transaction = True
        cache.immediate = True
        if db_session is not None and db_session.serializable:
            cursor = connection.cursor()
            sql = 'SET TRANSACTION ISOLATION LEVEL SERIALIZABLE'
            if core.local.debug: log_orm(sql)
            cursor.execute(sql)
            cache.in_transaction = True

    @wrap_dbapi_exceptions
    def release(provider, connection, cache=None):
        if cache is not None:
            db_session = cache.db_session
            if db_session is not None and db_session.ddl and cache.saved_fk_state:
                try:
                    cursor = connection.cursor()
                    sql = 'SET foreign_key_checks = 1'
                    if core.local.debug: log_orm(sql)
                    cursor.execute(sql)
                except:
                    provider.pool.drop(connection)
                    raise
        DBAPIProvider.release(provider, connection, cache)

    def table_exists(provider, cursor, table_name, case_sensitive=True):
        db_name, table_name = provider.split_table_name(table_name)
        if case_sensitive: sql = 'SELECT table_name FROM information_schema.tables ' \
                                 'WHERE table_schema=%s and table_name=%s'
        else: sql = 'SELECT table_name FROM information_schema.tables ' \
                    'WHERE table_schema=%s and UPPER(table_name)=UPPER(%s)'
        cursor.execute(sql, [ db_name, table_name ])
        row = cursor.fetchone()
        return row[0] if row is not None else None

    def index_exists(provider, cursor, table_name, index_name, case_sensitive=True):
        db_name, table_name = provider.split_table_name(table_name)
        if case_sensitive: sql = 'SELECT index_name FROM information_schema.statistics ' \
                                 'WHERE table_schema=%s and table_name=%s and index_name=%s'
        else: sql = 'SELECT index_name FROM information_schema.statistics ' \
                    'WHERE table_schema=%s and table_name=%s and UPPER(index_name)=UPPER(%s)'
        cursor.execute(sql, [ db_name, table_name, index_name ])
        row = cursor.fetchone()
        return row[0] if row is not None else None

    def fk_exists(provider, cursor, table_name, fk_name, case_sensitive=True):
        db_name, table_name = provider.split_table_name(table_name)
        if case_sensitive: sql = 'SELECT constraint_name FROM information_schema.table_constraints ' \
                                 'WHERE table_schema=%s and table_name=%s ' \
                                 "and constraint_type='FOREIGN KEY' and constraint_name=%s"
        else: sql = 'SELECT constraint_name FROM information_schema.table_constraints ' \
                    'WHERE table_schema=%s and table_name=%s ' \
                    "and constraint_type='FOREIGN KEY' and UPPER(constraint_name)=UPPER(%s)"
        cursor.execute(sql, [ db_name, table_name, fk_name ])
        row = cursor.fetchone()
        return row[0] if row is not None else None

provider_cls = MySQLProvider

def str2datetime(s):
    if 19 < len(s) < 26: s += '000000'[:26-len(s)]
    s = s.replace('-', ' ').replace(':', ' ').replace('.', ' ').replace('T', ' ')
    try:
        return datetime(*imap(int, s.split()))
    except ValueError:
        return None  # for incorrect values like 0000-00-00 00:00:00
