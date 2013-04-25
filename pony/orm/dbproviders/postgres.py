from pony.utils import throw, is_utf8
from pony.orm.dbapiprovider import Pool
from pony.orm.dbproviders._postgres import *

import psycopg2

class PsycopgProvider(PGProvider):
    dbapi_module = psycopg2

    def inspect_connection(provider, connection):
        provider.server_version = connection.server_version
        provider.table_if_not_exists_syntax = provider.server_version >= 90100

    def get_pool(provider, *args, **kwargs):
        encoding = kwargs.setdefault('client_encoding', 'UTF8')
        if not is_utf8(encoding): throw(ValueError,
            "Only client_encoding='UTF8' Psycopg database option is supported")
        return Pool(provider.dbapi_module, *args, **kwargs)
    
provider_cls = PsycopgProvider
