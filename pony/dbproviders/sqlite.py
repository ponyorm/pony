from pony.thirdparty import sqlite as module

from pony.thirdparty.sqlite import (Warning, Error, InterfaceError, DatabaseError,
                                    DataError, OperationalError, IntegrityError, InternalError,
                                    ProgrammingError, NotSupportedError)

from pony.dbapiprovider import SQLBuilder

quote_name = SQLBuilder.quote_name
param = SQLBuilder.param

paramstyle = 'qmark'

def connect(filename):
    return module.connect(filename)

def release(con):
    pass

def ast2sql(ast):
    b = SQLBuilder(ast)
    return b.sql, b.params

