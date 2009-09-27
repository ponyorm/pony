from pony.thirdparty import sqlite

from pony.thirdparty.sqlite import (Warning, Error, InterfaceError, DatabaseError,
                                    DataError, OperationalError, IntegrityError, InternalError,
                                    ProgrammingError, NotSupportedError)

from pony import dbapiprovider
from pony.utils import localbase

paramstyle = 'qmark'

class Local(localbase):
    def __init__(self):
        self.connection = None

local = Local()

def quote_name(connection, name):
    return dbapiprovider.quote_name(name)

def connect(filename):
    if local.connection is None:
        local.connection = sqlite.connect(filename)
    return local.connection

def release(connection):
    connection.close()

def ast2sql(con, ast):
    b = dbapiprovider.SQLBuilder(ast)
    return b.sql, b.params

