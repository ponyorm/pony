from decimal import Decimal, InvalidOperation
from datetime import datetime, date, time

import warnings
warnings.filterwarnings('ignore', '^Table.+already exists$', Warning, '^pony\\.orm$')

import MySQLdb
import MySQLdb.converters
from MySQLdb.constants import FIELD_TYPE, FLAG

from pony import dbschema
from pony import dbapiprovider
from pony.dbapiprovider import DBAPIProvider, LongStr, LongUnicode
from pony.sqltranslation import SQLTranslator
from pony.sqlbuilding import Value, SQLBuilder
from pony.utils import localbase

def get_provider(*args, **keyargs):
    return MySQLProvider(*args, **keyargs)

class MySQLColumn(dbschema.Column):
    auto_template = '%(type)s PRIMARY KEY AUTO_INCREMENT'

class MySQLSchema(dbschema.DBSchema):
    column_class = MySQLColumn

class MyValue(Value):
    def quote_str(self, s):
        s = s.replace('%', '%%')
        return Value.quote_str(self, s)

class MySQLTranslator(SQLTranslator):
    dialect = 'MySQL'

class MySQLBuilder(SQLBuilder):
    dialect = 'MySQL'
    make_value = MyValue
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
    paramstyle = 'format'
    quote_char = "`"

    dbschema_cls = MySQLSchema
    translator_cls = MySQLTranslator
    sqlbuilder_cls = MySQLBuilder

    def __init__(provider, *args, **keyargs):
        DBAPIProvider.__init__(provider, MySQLdb)
        provider.pool = _get_pool(*args, **keyargs)

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

def _get_pool(*args, **keyargs):
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
    def drop(pool, con):
        assert con is pool.con
        pool.con = None
        con.close()
