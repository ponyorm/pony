from pony.orm.dbproviders._postgres import *

import pgdb

class PyGreSQLProvider(PGProvider):
    dbapi_module = pgdb

provider_cls = PyGreSQLProvider
