# -*- coding: cp1251 -*-

from pysqlite2 import dbapi2

from pony.db.providers.scaffolding.dbapi import DBAPIConnection

def connect(*args, **kwargs):
    return SQLiteConnection(*args, **kwargs)

class SQLiteConnection(DBAPIConnection):
    dbapi_module = dbapi2

