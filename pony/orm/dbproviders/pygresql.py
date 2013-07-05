import re
from itertools import imap
from binascii import unhexlify
from uuid import UUID

from pony.orm import core
from pony.orm.dbapiprovider import ProgrammingError
from pony.orm.dbproviders._postgres import *

import pgdb

class PyGreSQLTable(PGTable):
    def create(table, provider, connection, created_tables=None):
        try: dbschema.Table.create(table, provider, connection, created_tables)
        except ProgrammingError, e:
            if getattr(e.original_exc, 'sqlstate', '42P07') != '42P07':
                provider.rollback(connection)
                raise
            if core.debug:
                core.log_orm('ALREADY EXISTS: %s' % e.args[0])
                core.log_orm('ROLLBACK')
            provider.rollback(connection)
        else: provider.commit(connection)

class PyGreSQLSchema(PGSchema):
    table_class = PyGreSQLTable

char2oct = {}
for i in range(256):
    ch = chr(i)
    if 31 < i < 127:
        char2oct[ch] = ch
    else: char2oct[ch] = '\\' + ('00'+oct(i))[-3:]
char2oct['\\'] = '\\\\'

oct_re = re.compile(r'\\[0-7]{3}')

class PyGreSQLValue(PGValue):
    __slots__ = []
    def __unicode__(self):
        value = self.value
        if isinstance(value, buffer):
            # currently this is not used, because buffer always translated to parameter
            return "'%s'::bytea" % "".join(imap(char2oct.__getitem__, value))
        return sqlbuilding.Value.__unicode__(self)

class PyGreSQLBuilder(PGSQLBuilder):
    make_value = PyGreSQLValue

class PyGreSQLBlobConverter(PGBlobConverter):
    def py2sql(converter, val):
        db_val = "".join(imap(char2oct.__getitem__, val))
        return db_val
    def sql2py(converter, val):
        if val.startswith('\\x'): val = unhexlify(val[2:])
        else: val = oct_re.sub(lambda match: chr(int(match.group(0)[-3:], 8)), val.replace('\\\\', '\\'))
        return buffer(val)

class PyGreSQLDateConverter(dbapiprovider.DateConverter):
    def py2sql(converter, val):
        return datetime(val.year, val.month, val.day)
    def sql2py(converter, val):
        return datetime.strptime(val, '%Y-%m-%d').date()
    
class PyGreSQLDatetimeConverter(PGDatetimeConverter):
    def sql2py(converter, val):
        return timestamp2datetime(val)

class PyGreSQLProvider(PGProvider):
    dbapi_module = pgdb
    dbschema_cls = PyGreSQLSchema
    sqlbuilder_cls = PyGreSQLBuilder

    def inspect_connection(provider, connection):
        provider.server_version = connection._cnx.server_version
        provider.table_if_not_exists_syntax = provider.server_version >= 90100

    def should_reconnect(provider, exc):
        return isinstance(exc, pgdb.OperationalError) and exc.sqlstate is None

    converter_classes = [
        (bool, dbapiprovider.BoolConverter),
        (unicode, PGUnicodeConverter),
        (str, PGStrConverter),
        (long, PGLongConverter),
        (int, dbapiprovider.IntConverter),
        (float, PGRealConverter),
        (Decimal, dbapiprovider.DecimalConverter),
        (buffer, PyGreSQLBlobConverter),
        (datetime, PyGreSQLDatetimeConverter),
        (date, PyGreSQLDateConverter),
        (UUID, PGUuidConverter),
    ]

provider_cls = PyGreSQLProvider
