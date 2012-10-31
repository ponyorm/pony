import re
from itertools import imap
from decimal import Decimal, InvalidOperation
from datetime import datetime, date, time
from binascii import unhexlify

import pgdb

from pony import orm, dbschema, sqlbuilding, dbapiprovider
from pony.dbapiprovider import DBAPIProvider, wrap_dbapi_exceptions, LongStr, LongUnicode
from pony.sqltranslation import SQLTranslator
from pony.utils import localbase, timestamp2datetime

def get_provider(*args, **keyargs):
    return PGProvider(*args, **keyargs)

class PGColumn(dbschema.Column):
    auto_template = 'SERIAL PRIMARY KEY'

class PGTable(dbschema.Table):
    def create(table, provider, connection, created_tables=None):
        try: dbschema.Table.create(table, provider, connection, created_tables)
        except orm.DatabaseError, e:
            if 'already exists' not in e.args[0]: raise
            if orm.debug:
                print 'ALREADY EXISTS:', e.args[0]
                print 'ROLLBACK\n'
            provider.rollback(connection)
        else: provider.commit(connection)
    def get_create_commands(table, created_tables=None):
        return dbschema.Table.get_create_commands(table, created_tables, False)

class PGSchema(dbschema.DBSchema):
    table_class = PGTable
    column_class = PGColumn

class PGValue(sqlbuilding.Value):
    def __unicode__(self):
        value = self.value
        if isinstance(value, buffer):
            return "'%s'::bytea" % "".join(imap(char2oct.__getitem__, val))
        return sqlbuilding.Value.__unicode__(self)

class PGTranslator(SQLTranslator):
    dialect = 'PostgreSQL'

class PGSQLBuilder(sqlbuilding.SQLBuilder):
    dialect = 'PostgreSQL'
    make_value = PGValue
    def INSERT(builder, table_name, columns, values, returning=None):
        result = sqlbuilding.SQLBuilder.INSERT(builder, table_name, columns, values)
        if returning is not None:
            result.extend([' RETURNING ', builder.quote_name(returning) ])
        return result

class PGUnicodeConverter(dbapiprovider.UnicodeConverter):
    def py2sql(converter, val):
        return val.encode('utf-8')
    def sql2py(converter, val):
        return val.decode('utf-8')

class PGStrConverter(dbapiprovider.StrConverter):
    def py2sql(converter, val):
        return val.decode(converter.encoding).encode('utf-8')
    def sql2py(converter, val):
        return val.decode('utf-8').encode(converter.encoding, 'replace')

class PGLongConverter(dbapiprovider.IntConverter):
    def sql_type(converter):
        return 'BIGINT'

class PGRealConverter(dbapiprovider.RealConverter):
    def sql_type(converter):
        return 'DOUBLE PRECISION'

char2oct = {}
for i in range(256):
    ch = chr(i)    
    if 31 < i < 127:
        char2oct[ch] = ch
    else: char2oct[ch] = '\\' + ('00'+oct(i))[-3:]
char2oct['\\'] = '\\\\'

oct_re = re.compile(r'\\[0-7]{3}')

class PGBlobConverter(dbapiprovider.BlobConverter):
    def py2sql(converter, val):
        db_val = "".join(imap(char2oct.__getitem__, val))
        return db_val
    def sql2py(converter, val):
        if val.startswith('\\x'): val = unhexlify(val[2:])
        else: val = oct_re.sub(lambda match: chr(int(match.group(0)[-3:], 8)), val.replace('\\\\', '\\'))
        return buffer(val)
    def sql_type(converter):
        return 'BYTEA'

class PGDateConverter(dbapiprovider.DateConverter):
    def py2sql(converter, val):
        return datetime(val.year, val.month, val.day)
    def sql2py(converter, val):
        return datetime.strptime(val, '%Y-%m-%d').date()

class PGDatetimeConverter(dbapiprovider.DatetimeConverter):
    def sql_type(converter):
        return 'TIMESTAMP'
    def sql2py(converter, val):
        return timestamp2datetime(val)

class PGProvider(DBAPIProvider):
    paramstyle = 'pyformat'

    dbschema_cls = PGSchema
    translator_cls = PGTranslator
    sqlbuilder_cls = PGSQLBuilder

    def __init__(provider, *args, **keyargs):
        DBAPIProvider.__init__(provider, pgdb)
        provider.pool = _get_pool(*args, **keyargs)

    def get_default_entity_table_name(provider, entity):
        return DBAPIProvider.get_default_entity_table_name(provider, entity).lower()

    def get_default_m2m_table_name(provider, attr, reverse):
        return DBAPIProvider.get_default_m2m_table_name(provider, attr, reverse).lower()

    def get_default_column_names(provider, attr, reverse_pk_columns=None):
        return [ column.lower() for column in DBAPIProvider.get_default_column_names(provider, attr, reverse_pk_columns) ]

    def get_default_m2m_column_names(provider, entity):
        return [ column.lower() for column in DBAPIProvider.get_default_m2m_column_names(provider, entity) ]

    @wrap_dbapi_exceptions
    def execute(provider, cursor, sql, arguments=None):
        if isinstance(sql, unicode): sql = sql.encode('utf8')
        if arguments is None: cursor.execute(sql)
        else: cursor.execute(sql, arguments)

    @wrap_dbapi_exceptions
    def executemany(provider, cursor, sql, arguments_list):
        if isinstance(sql, unicode): sql = sql.encode('utf8')
        cursor.executemany(sql, arguments_list)

    @wrap_dbapi_exceptions    
    def execute_returning_id(provider, cursor, sql, arguments):
        if isinstance(sql, unicode): sql = sql.encode('utf8')
        cursor.execute(sql, arguments)
        return cursor.fetchone()[0]

    converter_classes = [
        (bool, dbapiprovider.BoolConverter),
        (unicode, PGUnicodeConverter),
        (str, PGStrConverter),
        (long, PGLongConverter),
        (int, dbapiprovider.IntConverter),
        (float, PGRealConverter),
        (Decimal, dbapiprovider.DecimalConverter),
        (buffer, PGBlobConverter),
        (datetime, PGDatetimeConverter),
        (date, PGDateConverter)
    ]

def _get_pool(*args, **keyargs):
    return Pool(*args, **keyargs)

class Pool(localbase):
    def __init__(pool, *args, **keyargs):
        pool.args = args
        pool.keyargs = keyargs
        pool.con = None
    def connect(pool):
        if pool.con is None:
            pool.con = pgdb.connect(*pool.args, **pool.keyargs)
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
