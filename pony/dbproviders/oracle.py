import cx_Oracle

from cx_Oracle import (Warning, Error, InterfaceError, DatabaseError,
                     DataError, OperationalError, IntegrityError, InternalError,
                     ProgrammingError, NotSupportedError)

from pony import dbapiprovider

paramstyle = 'named'

class Param(dbapiprovider.Param):
    def __unicode__(self):
        return ':%s' % self.key

class OracleBuilder(dbapiprovider.SQLBuilder):
    param = Param
    
def quote_name(connection, name):
    return dbapiprovider.quote_name(name, "`")

def connect(*args, **keyargs):
    return cx_Oracle.connect(*args, **keyargs)

def release(connection):
    connection.close()

def ast2sql(con, ast):
    b = OracleBuilder(ast)
    return str(b.sql), b.params

