from decimal import Decimal
from datetime import datetime, date
from uuid import UUID

from pony.orm import dbschema, sqlbuilding, dbapiprovider
from pony.orm.dbapiprovider import DBAPIProvider, wrap_dbapi_exceptions
from pony.orm.sqltranslation import SQLTranslator
from pony.orm.sqlbuilding import Value
from pony.utils import throw

class PGColumn(dbschema.Column):
    auto_template = 'SERIAL PRIMARY KEY'

class PGSchema(dbschema.DBSchema):
    dialect = 'PostgreSQL'
    column_class = PGColumn

class PGTranslator(SQLTranslator):
    dialect = 'PostgreSQL'

class PGValue(Value):
    __slots__ = []
    def __unicode__(self):
        value = self.value
        if isinstance(value, bool): return value and 'true' or 'false'
        return Value.__unicode__(self)

class PGSQLBuilder(sqlbuilding.SQLBuilder):
    dialect = 'PostgreSQL'
    make_value = PGValue
    def INSERT(builder, table_name, columns, values, returning=None):
        result = sqlbuilding.SQLBuilder.INSERT(builder, table_name, columns, values)
        if returning is not None:
            result.extend([' RETURNING ', builder.quote_name(returning) ])
        return result
    def TO_INT(builder, expr):
        return '(', builder(expr), ')::int'
    def DATE(builder, expr):
        return '(', builder(expr), ')::date'

class PGUnicodeConverter(dbapiprovider.UnicodeConverter):
    def py2sql(converter, val):
        return val.encode('utf-8')
    def sql2py(converter, val):
        if isinstance(val, unicode): return val
        return val.decode('utf-8')

class PGStrConverter(dbapiprovider.StrConverter):
    def py2sql(converter, val):
        return val.decode(converter.encoding).encode('utf-8')
    def sql2py(converter, val):
        if not isinstance(val, unicode):
            if converter.utf8: return val
            val = val.decode('utf-8')
        return val.encode(converter.encoding, 'replace')

class PGLongConverter(dbapiprovider.IntConverter):
    def sql_type(converter):
        return 'BIGINT'

class PGRealConverter(dbapiprovider.RealConverter):
    def sql_type(converter):
        return 'DOUBLE PRECISION'

class PGBlobConverter(dbapiprovider.BlobConverter):
    def sql_type(converter):
        return 'BYTEA'

class PGDatetimeConverter(dbapiprovider.DatetimeConverter):
    sql_type_name = 'TIMESTAMP'

class PGUuidConverter(dbapiprovider.UuidConverter):
    def py2sql(converter, val):
        return val
    
class PGProvider(DBAPIProvider):
    dialect = 'PostgreSQL'
    paramstyle = 'pyformat'
    max_name_len = 63
    index_if_not_exists_syntax = False

    dbapi_module = None  # pgdb or psycopg2
    dbschema_cls = PGSchema
    translator_cls = PGTranslator
    sqlbuilder_cls = PGSQLBuilder  # pygresql redefines this to PyGreSQLBuilder

    default_schema_name = 'public'

    def normalize_name(provider, name):
        return name[:provider.max_name_len].lower()
    
    @wrap_dbapi_exceptions
    def execute(provider, cursor, sql, arguments=None, returning_id=False):
        if isinstance(sql, unicode): sql = sql.encode('utf8')
        if type(arguments) is list:
            assert arguments and not returning_id
            cursor.executemany(sql, arguments)
        else:
            if arguments is None: cursor.execute(sql)
            else: cursor.execute(sql, arguments)
            if returning_id: return cursor.fetchone()[0]

    def table_exists(provider, connection, table_name):
        schema_name, table_name = provider.split_table_name(table_name)
        cursor = connection.cursor()
        cursor.execute('SELECT 1 FROM pg_catalog.pg_tables WHERE schemaname = %s '
                       'AND tablename = %s', (schema_name, table_name))
        return cursor.fetchone() is not None
    
    def index_exists(provider, connection, table_name, index_name):
        schema_name, table_name = provider.split_table_name(table_name)
        cursor = connection.cursor()
        cursor.execute('SELECT 1 FROM pg_catalog.pg_indexes WHERE schemaname = %s '
                       'AND tablename = %s AND indexname = %s',
                       [ schema_name, table_name, index_name ])
        return cursor.fetchone() is not None

    def fk_exists(provider, connection, table_name, fk_name):
        schema_name, table_name = provider.split_table_name(table_name)
        cursor = connection.cursor()
        cursor.execute('SELECT 1 FROM pg_class cls '
                       'JOIN pg_namespace ns ON cls.relnamespace = ns.oid '
                       'JOIN pg_constraint con ON con.conrelid = cls.oid '
                       'WHERE ns.nspname = %s AND cls.relname = %s '
                       "AND con.contype = 'f' AND con.conname = %s",
                       [ schema_name, table_name, fk_name ])
        return cursor.fetchone() is not None

    def table_has_data(provider, connection, table_name):
        table_name = provider.quote_name(table_name)
        cursor = connection.cursor()
        cursor.execute('SELECT 1 FROM %s LIMIT 1' % table_name)
        return cursor.fetchone() is not None

    def drop_table(provider, connection, table_name):
        table_name = provider.quote_name(table_name)
        cursor = connection.cursor()
        sql = 'DROP TABLE %s CASCADE' % table_name
        cursor.execute(sql)

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
        (date, dbapiprovider.DateConverter),
        (UUID, PGUuidConverter),
    ]
