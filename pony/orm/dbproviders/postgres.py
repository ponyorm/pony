from pony.utils import throw, is_utf8
from pony.orm import core
from pony.orm.dbapiprovider import Pool, ProgrammingError
from pony.orm.dbproviders._postgres import *

import psycopg2
from psycopg2 import extensions

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

class PsycopgPool(Pool):
    def connect(pool):
        if pool.con is None:
            pool.con = pool.dbapi_module.connect(*pool.args, **pool.kwargs)
            if 'client_encoding' not in pool.kwargs:
                pool.con.set_client_encoding('UTF8')
        return pool.con

class PsycopgProvider(PGProvider):
    dbapi_module = psycopg2
    dbschema_cls = PsycopgSchema

    def inspect_connection(provider, connection):
        provider.server_version = connection.server_version
        provider.table_if_not_exists_syntax = provider.server_version >= 90100

    def get_pool(provider, *args, **kwargs):
        return PsycopgPool(provider.dbapi_module, *args, **kwargs)

    def set_optimistic_mode(provider, connection):
        if core.debug: core.log_orm('SET AUTOCOMMIT = ON')
        connection.autocommit = True

    def set_pessimistic_mode(provider, connection):
        if core.debug: core.log_orm('SET TRANSACTION ISOLATION LEVEL READ COMMITTED')
        connection.set_isolation_level(extensions.ISOLATION_LEVEL_READ_COMMITTED)

    def start_optimistic_save(provider, connection):
        if core.debug: core.log_orm('SET TRANSACTION ISOLATION LEVEL READ COMMITTED')
        connection.set_isolation_level(extensions.ISOLATION_LEVEL_READ_COMMITTED)

provider_cls = PsycopgProvider
