import MySQLdb

from MySQLdb import (Warning, Error, InterfaceError, DatabaseError,
                     DataError, OperationalError, IntegrityError, InternalError,
                     ProgrammingError, NotSupportedError)

from pony import dbapiprovider

paramstyle = 'format'

def quote_name(connection, name):
    return dbapiprovider.quote_name(name, "`")

def connect(*args, **keyargs):
    return MySQLdb.connect(*args, **keyargs)

def release(connection):
    connection.close()

def ast2sql(con, ast):
    b = dbapiprovider.SQLBuilder(ast, paramstyle, "`")
    return b.sql, b.adapter

