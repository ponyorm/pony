# -*- coding: cp1251 -*-

from pysqlite2 import dbapi2 as _sqlite
from pony.db.dbapiprovider import DBAPIConnection

__all__ = 'connect'

def connect(*args, **keyargs):
    return SQLiteConnection(*args, **keyargs)

class SQLiteConnection(object):
    






    def simple_select(self, *args, **keyargs):
        return SQLiteSimpleSelect(self, *args, **keyargs)
    def insert(self, *args, **keyargs)

















