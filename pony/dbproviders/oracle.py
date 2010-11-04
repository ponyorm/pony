import cx_Oracle

from cx_Oracle import (Warning, Error, InterfaceError, DatabaseError,
                     DataError, OperationalError, IntegrityError, InternalError,
                     ProgrammingError, NotSupportedError)

from pony import dbapiprovider

paramstyle = 'named'

class Param(dbapiprovider.Param):
    def __unicode__(self):
        return ':p%d' % self.index

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
    param_names = [ 'p%d' % i for i in b.layout ]
    return str(b.sql), dbapiprovider.dict_adapter_factory(param_names)
