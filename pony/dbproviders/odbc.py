import pyodbc

from pyodbc import (Warning, Error, InterfaceError, DatabaseError,
                    DataError, OperationalError, IntegrityError, InternalError,
                    ProgrammingError, NotSupportedError)

from pony import dbapiprovider

paramstyle = 'qmark'

def quote_name(connection, name):
    quote_char = connection.getinfo(pyodbc.SQL_IDENTIFIER_QUOTE_CHAR)
    return dbapiprovider.quote_name(name, quote_char)

def connect(*args, **keyargs):
    return pyodbc.connect(*args, **keyargs)

def release(connection):
    connection.close()

def ast2sql(connection, ast):
    quote_char = connection.getinfo(pyodbc.SQL_IDENTIFIER_QUOTE_CHAR)
    b = dbapiprovider.SQLBuilder(ast, quote_char)
    return b.sql, dbapiprovider.adapter_factory(b.params)
