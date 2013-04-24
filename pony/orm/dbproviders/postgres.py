from pony.utils import throw, is_utf8
from pony.orm.dbapiprovider import Pool
from pony.orm.dbproviders._postgres import *

import psycopg2

class PsycopgProvider(PGProvider):
    dbapi_module = psycopg2
    def get_pool(provider, *args, **kwargs):
        encoding = kwargs.setdefault('client_encoding', 'UTF8')
        if not is_utf8(encoding): throw(ValueError,
            "Only client_encoding='UTF8' Psycopg database option is supported")
        return Pool(provider.dbapi_module, *args, **kwargs)
    
provider_cls = PsycopgProvider
