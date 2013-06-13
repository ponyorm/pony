from pony.orm.core import Database

def raises_exception(exc_class, msg=None):
    def decorator(func):
        def wrapper(self, *args, **kwargs):
            try:
                func(self, *args, **kwargs)
                self.assert_(False, "expected exception %s wasn't raised" % exc_class.__name__)
            except exc_class, e:
                if not e.args: self.assertEqual(msg, None)
                elif msg is not None:
                    self.assertEqual(e.args[0], msg, "incorrect exception message. expected '%s', got '%s'" % (msg, e.args[0]))
        wrapper.__name__ = func.__name__
        return wrapper
    return decorator

def flatten(x):
    result = []
    for el in x:
        if hasattr(el, "__iter__") and not isinstance(el, basestring):
            result.extend(flatten(el))
        else:
            result.append(el)
    return result

class TestConnection(object):
    def commit(con):
        pass
    def rollback(con):
        pass

class TestCursor(object):
    def __init__(cursor):
        cursor.description = []
    def fetchone(cursor):
        return None
    def fetchmany(cursor, size):
        return []
    def fetchall(cursor):
        return []

test_cursor = TestCursor()

class TestPool(object):
    def connect(pool):
        return TestConnection()
    def release(pool, con):
        pass
    def drop(pool, con):
        pass

class TestDatabase(Database):
    real_provider_name = None
    sql = None
    def __init__(self, provider_name, *args, **kwargs):
        kwargs['pony_check_connection'] = False
        kwargs['pony_pool_mockup'] = TestPool()
        Database.__init__(self, self.real_provider_name or provider_name, *args, **kwargs)
    def _execute(database, sql, globals, locals, frame_depth):
        assert False
    def _exec_sql(database, sql, arguments=None):
        database.sql = sql
        database.arguments = arguments
        return test_cursor
    def _exec_sql_returning_id(database, sql, arguments):
        assert False
    def _exec_sql_many(database, sql, arguments_list):
        assert False
    def generate_mapping(database, filename=None, check_tables=False, create_tables=False):
        return Database.generate_mapping(database, filename)
