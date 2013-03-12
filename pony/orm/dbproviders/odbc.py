import pyodbc

from pyodbc import (Warning, Error, InterfaceError, DatabaseError,
                    DataError, OperationalError, IntegrityError, InternalError,
                    ProgrammingError, NotSupportedError)

from pony.orm import sqlbuilding

paramstyle = 'qmark'

MAX_PARAMS_COUNT = 200
ROW_VALUE_SYNTAX = False

def quote_name(connection, name):
    quote_char = connection.getinfo(pyodbc.SQL_IDENTIFIER_QUOTE_CHAR)
    return sqlbuilding.quote_name(name, quote_char)

def connect(*args, **kwargs):
    return pyodbc.connect(*args, **kwargs)

def release(connection):
    connection.close()

def ast2sql(connection, ast):
    quote_char = connection.getinfo(pyodbc.SQL_IDENTIFIER_QUOTE_CHAR)
    b = sqlbuilding.SQLBuilder(ast, paramstyle, quote_char)
    return b.sql, b.adapter

def get_last_rowid(cursor):
    return cursor.lastrowid
