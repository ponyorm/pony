from pony.thirdparty import sqlite

def connect(filename):
    return Connection(filename)

class Connection(object):
    def __init__(self, filename):
        self.filename = filename
        self.dbapi_connection = sqlite.connect(filename)
        self.paramstyle = 'qmark'
    