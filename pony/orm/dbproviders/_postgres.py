from decimal import Decimal
from datetime import datetime, date
from uuid import UUID

from pony.orm import dbschema, sqlbuilding, dbapiprovider
from pony.orm.dbapiprovider import DBAPIProvider, wrap_dbapi_exceptions
from pony.orm.sqltranslation import SQLTranslator
from pony.orm.sqlbuilding import Value

class PGColumn(dbschema.Column):
    auto_template = 'SERIAL PRIMARY KEY'

class PGTable(dbschema.Table):
    pass

class PGSchema(dbschema.DBSchema):
    dialect = 'PostgreSQL'
    table_class = PGTable
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

    def get_default_entity_table_name(provider, entity):
        return DBAPIProvider.get_default_entity_table_name(provider, entity).lower()

    def get_default_m2m_table_name(provider, attr, reverse):
        return DBAPIProvider.get_default_m2m_table_name(provider, attr, reverse).lower()

    def get_default_column_names(provider, attr, reverse_pk_columns=None):
        return [ column.lower() for column in DBAPIProvider.get_default_column_names(provider, attr, reverse_pk_columns) ]

    def get_default_m2m_column_names(provider, entity):
        return [ column.lower() for column in DBAPIProvider.get_default_m2m_column_names(provider, entity) ]

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
