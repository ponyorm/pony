from __future__ import absolute_import
from pony.py23compat import PY2, imap, basestring, buffer, int_types

from decimal import Decimal
from datetime import datetime, date, time, timedelta
from uuid import UUID

import warnings
warnings.filterwarnings('ignore', '^Table.+already exists$', Warning, '^pony\\.orm\\.dbapiprovider$')

try: import MySQLdb as mysql_module
except ImportError:
    try: import pymysql as mysql_module
    except ImportError: raise ImportError('No module named MySQLdb or pymysql found')
    else:
        import pymysql.converters as mysql_converters
        from pymysql.constants import FIELD_TYPE, FLAG, CLIENT
        if PY2:
            mysql_converters.encoders[buffer] = lambda val: mysql_converters.escape_str(str(val))
        mysql_converters.encoders[timedelta] = lambda val: mysql_converters.escape_str(timedelta2str(val))
        mysql_module_name = 'pymysql'
else:
    import MySQLdb.converters as mysql_converters
    from MySQLdb.constants import FIELD_TYPE, FLAG, CLIENT
    mysql_module_name = 'MySQLdb'
    from MySQLdb import string_literal

from pony.orm import core, dbschema, dbapiprovider
from pony.orm.core import log_orm, OperationalError
from pony.orm.dbapiprovider import DBAPIProvider, Pool, get_version_tuple, wrap_dbapi_exceptions
from pony.orm.sqltranslation import SQLTranslator
from pony.orm.sqlbuilding import SQLBuilder, join
from pony.utils import throw
from pony.converting import str2timedelta, timedelta2str

class MySQLColumn(dbschema.Column):
    auto_template = '%(type)s PRIMARY KEY AUTO_INCREMENT'

class MySQLSchema(dbschema.DBSchema):
    dialect = 'MySQL'
    inline_fk_syntax = False
    column_class = MySQLColumn

class MySQLTranslator(SQLTranslator):
    dialect = 'MySQL'

class MySQLBuilder(SQLBuilder):
    dialect = 'MySQL'
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
    ]

    def normalize_name(provider, name):
        return name[:provider.max_name_len].lower()

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
                conv[timedelta] = lambda td, c: string_literal(timedelta2str(td), c)
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
            fk = cursor.fetchone()
            if fk is not None: fk = (fk[1] == 'ON')
            if fk:
                sql = 'SET foreign_key_checks = 0'
                if core.debug: log_orm(sql)
                cursor.execute(sql)
            cache.saved_fk_state = bool(fk)
            cache.in_transaction = True
        cache.immediate = True
        if db_session is not None and db_session.serializable:
            cursor = connection.cursor()
            sql = 'SET TRANSACTION ISOLATION LEVEL SERIALIZABLE'
            if core.debug: log_orm(sql)
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
                    if core.debug: log_orm(sql)
                    cursor.execute(sql)
                except:
                    provider.pool.drop(connection)
                    raise
        DBAPIProvider.release(provider, connection, cache)


    def table_exists(provider, connection, table_name, case_sensitive=True):
        db_name, table_name = provider.split_table_name(table_name)
        cursor = connection.cursor()
        if case_sensitive: sql = 'SELECT table_name FROM information_schema.tables ' \
                                 'WHERE table_schema=%s and table_name=%s'
        else: sql = 'SELECT table_name FROM information_schema.tables ' \
                    'WHERE table_schema=%s and UPPER(table_name)=UPPER(%s)'
        cursor.execute(sql, [ db_name, table_name ])
        row = cursor.fetchone()
        return row[0] if row is not None else None

    def index_exists(provider, connection, table_name, index_name, case_sensitive=True):
        db_name, table_name = provider.split_table_name(table_name)
        if case_sensitive: sql = 'SELECT index_name FROM information_schema.statistics ' \
                                 'WHERE table_schema=%s and table_name=%s and index_name=%s'
        else: sql = 'SELECT index_name FROM information_schema.statistics ' \
                    'WHERE table_schema=%s and table_name=%s and UPPER(index_name)=UPPER(%s)'
        cursor = connection.cursor()
        cursor.execute(sql, [ db_name, table_name, index_name ])
        row = cursor.fetchone()
        return row[0] if row is not None else None

    def fk_exists(provider, connection, table_name, fk_name, case_sensitive=True):
        db_name, table_name = provider.split_table_name(table_name)
        if case_sensitive: sql = 'SELECT constraint_name FROM information_schema.table_constraints ' \
                                 'WHERE table_schema=%s and table_name=%s ' \
                                 "and constraint_type='FOREIGN KEY' and constraint_name=%s"
        else: sql = 'SELECT constraint_name FROM information_schema.table_constraints ' \
                    'WHERE table_schema=%s and table_name=%s ' \
                    "and constraint_type='FOREIGN KEY' and UPPER(constraint_name)=UPPER(%s)"
        cursor = connection.cursor()
        cursor.execute(sql, [ db_name, table_name, fk_name ])
        row = cursor.fetchone()
        return row[0] if row is not None else None

provider_cls = MySQLProvider

def str2datetime(s):
    if 19 < len(s) < 26: s += '000000'[:26-len(s)]
    s = s.replace('-', ' ').replace(':', ' ').replace('.', ' ').replace('T', ' ')
    return datetime(*imap(int, s.split()))
