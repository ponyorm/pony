import cx_Oracle

from cx_Oracle import (Warning, Error, InterfaceError, DatabaseError,
                     DataError, OperationalError, IntegrityError, InternalError,
                     ProgrammingError, NotSupportedError)

from pony import dbapiprovider

paramstyle = 'named'

def quote_name(connection, name):
    return dbapiprovider.quote_name(name, "`")

def connect(*args, **keyargs):
    return cx_Oracle.connect(*args, **keyargs)

def release(connection):
    connection.close()

def ast2sql(con, ast):
    b = dbapiprovider.SQLBuilder(ast)
    return str(b.sql), b.params

