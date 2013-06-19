from decimal import Decimal, InvalidOperation
from datetime import datetime, date, time, timedelta

import warnings
warnings.filterwarnings('ignore', '^Table.+already exists$', Warning, '^pony\\.orm\\.dbapiprovider$')

import MySQLdb
import MySQLdb.converters
from MySQLdb.constants import FIELD_TYPE, FLAG

from pony.orm import dbschema
from pony.orm import dbapiprovider
from pony.orm.dbapiprovider import DBAPIProvider, Pool, get_version_tuple
from pony.orm.sqltranslation import SQLTranslator
from pony.orm.sqlbuilding import Value, SQLBuilder, join

class MySQLColumn(dbschema.Column):
    auto_template = '%(type)s PRIMARY KEY AUTO_INCREMENT'

class MySQLSchema(dbschema.DBSchema):
    dialect = 'MySQL'
    column_class = MySQLColumn

class MySQLTranslator(SQLTranslator):
    dialect = 'MySQL'

class MySQLBuilder(SQLBuilder):
    dialect = 'MySQL'
    def CONCAT(builder, *args):
        return 'concat(',  join(', ', map(builder, args)), ')'
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

def _string_sql_type(converter):
    db_encoding = converter.db_encoding or 'utf8'
    if converter.max_len:
        return 'VARCHAR(%d) CHARACTER SET %s' % (converter.max_len, db_encoding)
    return 'LONGTEXT CHARACTER SET %s' % db_encoding

class MySQLUnicodeConverter(dbapiprovider.UnicodeConverter):
    sql_type = _string_sql_type

class MySQLStrConverter(dbapiprovider.StrConverter):
    sql_type = _string_sql_type

class MySQLLongConverter(dbapiprovider.IntConverter):
    def sql_type(converter):
        return 'BIGINT'

class MySQLRealConverter(dbapiprovider.RealConverter):
    def sql_type(converter):
        return 'DOUBLE'

class MySQLBlobConverter(dbapiprovider.BlobConverter):
    def sql_type(converter):
        return 'LONGBLOB'

class MySQLProvider(DBAPIProvider):
    dialect = 'MySQL'
    paramstyle = 'format'
    quote_char = "`"

    max_time_precision = default_time_precision = 0

    dbapi_module = MySQLdb
    dbschema_cls = MySQLSchema
    translator_cls = MySQLTranslator
    sqlbuilder_cls = MySQLBuilder

    converter_classes = [
        (bool, dbapiprovider.BoolConverter),
        (unicode, MySQLUnicodeConverter),
        (str, MySQLStrConverter),
        (int, dbapiprovider.IntConverter),
        (long, MySQLLongConverter),
        (float, MySQLRealConverter),
        (Decimal, dbapiprovider.DecimalConverter),
        (buffer, MySQLBlobConverter),
        (datetime, dbapiprovider.DatetimeConverter),
        (date, dbapiprovider.DateConverter)
    ]

    def inspect_connection(provider, connection):
        cursor = connection.cursor()
        cursor.execute('select version()')
        row = cursor.fetchone()
        assert row is not None
        provider.server_version = get_version_tuple(row[0])
        if provider.server_version >= (5, 6, 4):
            provider.max_time_precision = 6

    def get_pool(provider, *args, **kwargs):
        if 'conv' not in kwargs:
            conv = MySQLdb.converters.conversions.copy()
            conv[FIELD_TYPE.BLOB] = [(FLAG.BINARY, buffer)]
            conv[FIELD_TYPE.TIMESTAMP] = str2datetime
            conv[FIELD_TYPE.DATETIME] = str2datetime
            conv[FIELD_TYPE.TIME] = str2timedelta
            kwargs['conv'] = conv
        if 'charset' not in kwargs:
            kwargs['charset'] = 'utf8'
        return Pool(MySQLdb, *args, **kwargs)

provider_cls = MySQLProvider

def str2datetime(s):
    if 19 < len(s) < 26: s += '000000'[:26-len(s)]
    s = s.replace('-', ' ').replace(':', ' ').replace('.', ' ').replace('T', ' ')
    return datetime(*map(int, s.split()))

def str2timedelta(s):
    if '.' in s:
        s, fractional = s.split('.')
        microseconds = int((fractional + '000000')[:6])
    else: microseconds = 0
    h, m, s = map(int, s.split(':'))
    td = timedelta(hours=abs(h), minutes=m, seconds=s, microseconds=microseconds)
    return -td if h < 0 else td
