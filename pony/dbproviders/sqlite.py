from pony.thirdparty import sqlite

paramstyle = 'qmark'

def connect(filename):
    return sqlite.connect(filename)

def release(con):
    pass
