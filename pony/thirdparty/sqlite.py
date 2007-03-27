try: from sqlite3 import *
except ImportError:
    from pysqlite2.dbapi2 import *
