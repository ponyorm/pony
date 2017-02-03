from __future__ import absolute_import, print_function, division
from pony.py23compat import basestring

from functools import wraps
from contextlib import contextmanager

from pony.orm.core import Database
from pony.utils import import_module

def raises_exception(exc_class, msg=None):
    def decorator(func):
        def wrapper(self, *args, **kwargs):
            try:
                func(self, *args, **kwargs)
                self.fail("expected exception %s wasn't raised" % exc_class.__name__)
            except exc_class as e:
                if not e.args: self.assertEqual(msg, None)
                elif msg is not None:
                    self.assertEqual(e.args[0], msg, "incorrect exception message. expected '%s', got '%s'" % (msg, e.args[0]))
        wrapper.__name__ = func.__name__
        return wrapper
    return decorator

@contextmanager
def raises_if(test, cond, exc_class, exc_msg=None):
    try:
        yield
    except exc_class as e:
        test.assertTrue(cond)
        if exc_msg is None: pass
        elif exc_msg.startswith('...') and exc_msg != '...':
            if exc_msg.endswith('...'):
                test.assertIn(exc_msg[3:-3], str(e))
            else:
                test.assertTrue(str(e).endswith(exc_msg[3:]))
        elif exc_msg.endswith('...'):
            test.assertTrue(str(e).startswith(exc_msg[:-3]))
        else:
            test.assertEqual(str(e), exc_msg)
    else:
        test.assertFalse(cond)

def flatten(x):
    result = []
    for el in x:
        if hasattr(el, "__iter__") and not isinstance(el, basestring):
            result.extend(flatten(el))
        else:
            result.append(el)
    return result

class TestConnection(object):
    def __init__(con, database):
        con.database = database
        if database and database.provider_name == 'postgres':
            con.autocommit = True
    def commit(con):
        pass
    def rollback(con):
        pass
    def cursor(con):
        return test_cursor

class TestCursor(object):
    def __init__(cursor):
        cursor.description = []
        cursor.rowcount = 0
    def execute(cursor, sql, args=None):
        pass
    def fetchone(cursor):
        return None
    def fetchmany(cursor, size):
        return []
    def fetchall(cursor):
        return []

test_cursor = TestCursor()

class TestPool(object):
    def __init__(pool, database):
        pool.database = database
    def connect(pool):
        return TestConnection(pool.database)
    def release(pool, con):
        pass
    def drop(pool, con):
        pass
    def disconnect(pool):
        pass

class TestDatabase(Database):
    real_provider_name = None
    raw_server_version = None
    sql = None
    def bind(self, provider_name, *args, **kwargs):
        if self.real_provider_name is not None:
            provider_name = self.real_provider_name
        self.provider_name = provider_name
        provider_module = import_module('pony.orm.dbproviders.' + provider_name)
        provider_cls = provider_module.provider_cls
        raw_server_version = self.raw_server_version

        if raw_server_version is None:
            if provider_name == 'sqlite': raw_server_version = '3.7.17'
            elif provider_name in ('postgres', 'pygresql'): raw_server_version = '9.2'
            elif provider_name == 'oracle': raw_server_version = '11.2.0.2.0'
            elif provider_name == 'mysql': raw_server_version = '5.6.11'
            else: assert False, provider_name  # pragma: no cover

        t = [ int(component) for component in raw_server_version.split('.') ]
        if len(t) == 2: t.append(0)
        server_version = tuple(t)
        if provider_name in ('postgres', 'pygresql'):
            server_version = int('%d%02d%02d' % server_version)

        class TestProvider(provider_cls):
            def inspect_connection(provider, connection):
                pass
        TestProvider.server_version = server_version

        kwargs['pony_check_connection'] = False
        kwargs['pony_pool_mockup'] = TestPool(self)
        Database.bind(self, TestProvider, *args, **kwargs)
    def _execute(database, sql, globals, locals, frame_depth):
        assert False  # pragma: no cover
    def _exec_sql(database, sql, arguments=None, returning_id=False):
        assert type(arguments) is not list and not returning_id
        database.sql = sql
        database.arguments = arguments
        return test_cursor
    def generate_mapping(database, filename=None, check_tables=True, create_tables=False):
        return Database.generate_mapping(database, filename, create_tables=False)
