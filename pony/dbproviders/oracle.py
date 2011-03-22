import cx_Oracle

from cx_Oracle import (Warning, Error, InterfaceError, DatabaseError,
                     DataError, OperationalError, IntegrityError, InternalError,
                     ProgrammingError, NotSupportedError)

from pony import sqlbuilding

paramstyle = 'named'

def quote_name(connection, name):
    return sqlbuilding.quote_name(name, "`")

def connect(*args, **keyargs):
    return cx_Oracle.connect(*args, **keyargs)

def release(connection):
    connection.close()

def ast2sql(con, ast):
    b = sqlbuilding.SQLBuilder(ast, paramstyle)
    return str(b.sql), b.adapter
