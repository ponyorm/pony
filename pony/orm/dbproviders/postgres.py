from pony.utils import throw, is_utf8
from pony.orm.dbapiprovider import Pool, ProgrammingError
from pony.orm.dbproviders._postgres import *

import psycopg2

class PsycopgTable(PGTable):
    def create(table, provider, connection, created_tables=None):
        try: dbschema.Table.create(table, provider, connection, created_tables)
        except ProgrammingError, e:
            if e.original_exc.pgcode !='42P07':
                provider.rollback(connection)
                raise
            if core.debug:
                log_orm('ALREADY EXISTS: %s' % e.args[0])
                log_orm('ROLLBACK')
            provider.rollback(connection)
        else: provider.commit(connection)

class PsycopgSchema(PGSchema):
    table_class = PsycopgTable

class PsycopgProvider(PGProvider):
    dbapi_module = psycopg2
    dbschema_cls = PsycopgSchema

    def inspect_connection(provider, connection):
        provider.server_version = connection.server_version
        provider.table_if_not_exists_syntax = provider.server_version >= 90100

    def get_pool(provider, *args, **kwargs):
        encoding = kwargs.setdefault('client_encoding', 'UTF8')
        if not is_utf8(encoding): throw(ValueError,
            "Only client_encoding='UTF8' Psycopg database option is supported")
        return Pool(provider.dbapi_module, *args, **kwargs)
    
provider_cls = PsycopgProvider
