from pony.orm import Database

def raises_exception(exc_class, msg=None):
    def decorator(func):
        def wrapper(self, *args, **keyargs):
            try:
                func(self, *args, **keyargs)
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

class TestDatabase(Database):
    def __init__(database, *args, **keyargs):
        Database.__init__(database, *args, **keyargs)
        database.last_sql = None
    def _exec_sql(database, sql, arguments=None):
        database.last_sql = sql
        return Database._exec_sql(database, sql, arguments)
    def _exec_sql_returning_id(database, sql, arguments, returning_py_type):
        database.last_sql = sql
        return Database._exec_sql_returning_id(database, sql, arguments, returning_py_type)
    def _exec_sql_many(database, sql, arguments_list):
        database.last_sql = sql
        return Database._exec_sql_many(database, sql, arguments_list)
    def _execute(database, sql, globals, locals, frame_depth):
        database.last_sql = sql
        return Database._execute(database, sql, globals, locals, frame_depth + 1)