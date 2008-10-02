from pony.thirdparty import sqlite

from pony.thirdparty.sqlite import (Warning, Error, InterfaceError, DatabaseError,
                                    DataError, OperationalError, IntegrityError, InternalError,
                                    ProgrammingError, NotSupportedError)

from pony import dbapiprovider

paramstyle = 'qmark'

def quote_name(connection, name):
    return dbapiprovider.quote_name(name)

def connect(filename):
    return sqlite.connect(filename)

def release(connection):
    pass

def ast2sql(con, ast):
    b = dbapiprovider.SQLBuilder(ast)
    return b.sql, b.params

