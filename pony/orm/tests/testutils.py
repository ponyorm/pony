from __future__ import absolute_import, print_function, division
from pony.py23compat import basestring

import re
from contextlib import contextmanager

from pony.orm.core import Database
from pony.utils import import_module

def test_exception_msg(test_case, exc_msg, test_msg=None):
    if test_msg is None: return
    error_template = "incorrect exception message. expected '%s', got '%s'"
    error_msg = error_template % (test_msg, exc_msg)
    assert test_msg not in ('...', '....', '.....', '......')
    if '...' not in test_msg:
        test_case.assertEqual(test_msg, exc_msg, error_msg)
    else:
        pattern = ''.join(
            '[%s]' % char for char in test_msg.replace('\\', '\\\\')
                                              .replace('[', '\\[')
        ).replace('[.][.][.]', '.*')
        regex = re.compile(pattern)
        if not regex.match(exc_msg):
            test_case.fail(error_template % (test_msg, exc_msg))

def raises_exception(exc_class, test_msg=None):
    def decorator(func):
        def wrapper(test_case, *args, **kwargs):
            try:
                func(test_case, *args, **kwargs)
                test_case.fail("Expected exception %s wasn't raised" % exc_class.__name__)
            except exc_class as e:
                if not e.args: test_case.assertEqual(test_msg, None)
                else: test_exception_msg(test_case, str(e), test_msg)
        wrapper.__name__ = func.__name__
        return wrapper
    return decorator

@contextmanager
def raises_if(test_case, cond, exc_class, test_msg=None):
    try:
        yield
    except exc_class as e:
        test_case.assertTrue(cond)
        test_exception_msg(test_case, str(e), test_msg)
    else:
        test_case.assertFalse(cond, "Expected exception %s wasn't raised" % exc_class.__name__)

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
        return TestConnection(pool.database), True
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
    def bind(self, provider, *args, **kwargs):
        provider_name = provider
        assert isinstance(provider_name, basestring)
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
            json1_available = False  # for SQLite
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
