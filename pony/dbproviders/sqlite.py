from pony.thirdparty import sqlite

from pony.thirdparty.sqlite import (Warning, Error, InterfaceError, DatabaseError,
                                    DataError, OperationalError, IntegrityError, InternalError,
                                    ProgrammingError, NotSupportedError)

from pony import dbapiprovider
from pony.utils import localbase

paramstyle = 'qmark'

class Local(localbase):
    def __init__(self):
        self.db_to_conn = {}

local = Local()

def quote_name(connection, name):
    return dbapiprovider.quote_name(name)

def connect(filename):
    conn = local.db_to_conn.get(filename)
    if conn is None:
        local.db_to_conn[filename] = conn = sqlite.connect(filename)
    return conn

def release(connection):
    pass

def ast2sql(con, ast):
    b = dbapiprovider.SQLBuilder(ast)
    return b.sql, dbapiprovider.adapter_factory(b.params)
