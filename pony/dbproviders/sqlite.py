from pony.thirdparty import sqlite

from pony.thirdparty.sqlite import (Warning, Error, InterfaceError, DatabaseError,
                                    DataError, OperationalError, IntegrityError, InternalError,
                                    ProgrammingError, NotSupportedError)

from pony import dbapiprovider
from pony.utils import localbase
from os import path

paramstyle = 'qmark'

class Local(localbase):
    def __init__(self):
        self.db_to_conn = {}

local = Local()

def quote_name(connection, name):
    return dbapiprovider.quote_name(name)

def connect(filename, create=False):
    conn = local.db_to_conn.get(filename)
    if conn is None:
        if not create and filename != ':memory:' and not path.exists(filename):
            raise IOError("Database file is not found: %r" % filename)
        local.db_to_conn[filename] = conn = sqlite.connect(filename)
    return conn

def release(connection):
    pass

def ast2sql(con, ast):
    b = dbapiprovider.SQLBuilder(ast)
    return b.sql, b.adapter
