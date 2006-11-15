# -*- coding: cp1251 -*-

class DBAPIConnection(object):
    def __init__(self, *args, **keyargs):
        self.dbapi_connection = self.dbapi_module.connect(*args, **keyargs)
        self.state = 'opened'
    def commit(self):
        if self.state != 'in_transaction':
            raise TypeError, 'Connection not in transaction'
        try: self.dbapi_connection.commit()
        except: self.state = 'unknown'; raise
        else: self.state = 'opened'
    def abort():
        if self.state != 'in_transaction':
            raise TypeError, 'Connection not in transaction'
        try: self.dbapi_connection.abort()
        except: self.state = 'unknown'; raise
        else: self.state = 'opened'
    def close():
        self.dbapi_connection.close()
        self.state = 'closed'
    def get(operation, parameters=None):
        cursor = self.dbapi_connection.cursor()
        cursor.execute(operation, parameters)
        result = cursor.fetchone()
        if cursor.fetchone() is not None:
            raise ValueError, 'Query returns more then one row'
        if len(result) == 1: return result[0]
        else: return result
    def fetchone(operation, parameters=None):
        self.execute(operation, parameters).fetchone()
    def execute(operation, parameters=None):
        cursor = self.dbapi_connection.cursor()
        cursor.execute(operation, parameters)
        return cursor
    def executemany(operation, seq_of_parameters):
        cursor = self.dbapi_connection.cursor()
        cursor.executemany(operation, parameters)
        return cursor
    









