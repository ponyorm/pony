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
        self.connections = {}

local = Local()

memory_db_conn = None

def quote_name(connection, name):
    return dbapiprovider.quote_name(name)

def connect(filename, create=False):
    if filename == ':memory:':
        global memory_db_conn
        if memory_db_conn is None:
            try: memory_db_conn = sqlite.connect(':memory:', check_same_thread=False)
            except TypeError, e:
                if 'check_same_thread' in e.args[0]:
                    raise TypeError("Please upgrade sqlite or use file database instead of :memory:")
        return memory_db_conn

    conn = local.connections.get(filename)
    if conn is None:
        if not create and not path.exists(filename):
            raise IOError("Database file is not found: %r" % filename)
        local.connections[filename] = conn = sqlite.connect(filename)
    return conn

def release(connection):
    pass

def ast2sql(con, ast):
    b = dbapiprovider.SQLBuilder(ast)
    return b.sql, b.adapter
