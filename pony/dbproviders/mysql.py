import MySQLdb

from MySQLdb import (Warning, Error, InterfaceError, DatabaseError,
                     DataError, OperationalError, IntegrityError, InternalError,
                     ProgrammingError, NotSupportedError)

from pony import dbapiprovider

paramstyle = 'format'

class Param(dbapiprovider.Param):
    def __unicode__(self):
        return u'%s'

class MySQLBuilder(dbapiprovider.SQLBuilder):
    param = Param

def quote_name(connection, name):
    return dbapiprovider.quote_name(name, "`")

def connect(*args, **keyargs):
    return MySQLdb.connect(*args, **keyargs)

def release(connection):
    pass

def ast2sql(con, ast):
    b = MySQLBuilder(ast, "`")
    return b.sql, b.params

