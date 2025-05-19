import re
from contextlib import contextmanager
from typing import Literal, overload

from pony.orm.core import Database
from pony.utils import import_module


def check_exception_msg(test_case, exc_msg, test_msg=None):
    if test_msg is None:
        return
    error_template = "incorrect exception message. expected '%s', got '%s'"
    error_msg = error_template % (test_msg, exc_msg)
    assert test_msg not in ("...", "....", ".....", "......")
    if "..." not in test_msg:
        test_case.assertEqual(test_msg, exc_msg, error_msg)
    else:
        pattern = "".join(
            "[%s]" % char for char in test_msg.replace("\\", "\\\\").replace("[", "\\[")
        ).replace("[.][.][.]", ".*")
        regex = re.compile(pattern)
        if not regex.match(exc_msg):
            test_case.fail(error_template % (test_msg, exc_msg))


def raises_exception(exc_class, test_msg=None):
    def decorator(func):
        def wrapper(test_case, *args, **kwargs):
            try:
                func(test_case, *args, **kwargs)
                test_case.fail(
                    "Expected exception %s wasn't raised" % exc_class.__name__
                )
            except exc_class as e:
                if not e.args:
                    test_case.assertEqual(test_msg, None)
                else:
                    check_exception_msg(test_case, str(e), test_msg)

        wrapper.__name__ = func.__name__
        return wrapper

    return decorator


@contextmanager
def raises_if(test_case, cond, exc_class, test_msg=None):
    try:
        yield
    except exc_class as e:
        test_case.assertTrue(cond)
        check_exception_msg(test_case, str(e), test_msg)
    else:
        test_case.assertFalse(
            cond, "Expected exception %s wasn't raised" % exc_class.__name__
        )


def flatten(x):
    result = []
    for el in x:
        if hasattr(el, "__iter__") and not isinstance(el, str):
            result.extend(flatten(el))
        else:
            result.append(el)
    return result


class TestConnection:
    def __init__(self, database):
        self.database = database
        if database and database.provider_name == "postgres":
            self.autocommit = True

    def commit(self):
        pass

    def rollback(self):
        pass

    def cursor(self):
        return test_cursor


class TestCursor:
    arraysize = 1

    def __init__(self):
        self.description = []
        self.rowcount = 0
        self.lastrowid = None

    def execute(self, sql, args=None):
        pass

    def executemany(self, sql, arguments):
        pass

    def fetchone(self):
        return None

    def fetchmany(self, size: int = 0, /) -> list:
        return []

    def fetchall(self) -> list:
        return []

    def var(self, typ, size, arrazsize, /, outconverter):
        return None


test_cursor = TestCursor()


class TestPool:
    def __init__(self, database):
        self.database = database

    def connect(self):
        return TestConnection(self.database), True

    def release(self, con):
        pass

    def drop(self, con):
        pass

    def disconnect(self):
        pass


class TestDatabase(Database):
    __test__ = False
    real_provider_name = None
    raw_server_version = None
    sql = None

    def bind(self, provider, *args, **kwargs):
        provider_name = provider
        assert isinstance(provider_name, str)
        if self.real_provider_name is not None:
            provider_name = self.real_provider_name
        self.provider_name = provider_name
        provider_module = import_module("pony.orm.dbproviders." + provider_name)
        provider_cls = provider_module.provider_cls
        raw_server_version = self.raw_server_version

        if raw_server_version is None:
            if provider_name == "sqlite":
                raw_server_version = "3.7.17"
            elif provider_name in ("postgres", "pygresql"):
                raw_server_version = "9.2"
            elif provider_name == "oracle":
                raw_server_version = "11.2.0.2.0"
            elif provider_name == "mysql":
                raw_server_version = "5.6.11"
            else:
                assert False, provider_name  # pragma: no cover

        t = [int(component) for component in raw_server_version.split(".")]
        if len(t) == 2:
            t.append(0)
        server_version = tuple(t)
        if provider_name in ("postgres", "pygresql"):
            server_version = int("%d%02d%02d" % server_version)

        class TestProvider(provider_cls):
            json1_available = False  # for SQLite

            def inspect_connection(self, connection):
                pass

        TestProvider.server_version = server_version

        kwargs["pony_check_connection"] = False
        kwargs["pony_pool_mockup"] = TestPool(self)
        Database.bind(self, TestProvider, *args, **kwargs)

    def _execute(self, sql, globals, locals, frame_depth):
        assert False  # pragma: no cover

    @overload
    def _exec_sql(
        self,
        sql,
        arguments=None,
        *,
        returning_id: Literal[False] = False,
        start_transaction: bool = False,
    ) -> TestCursor: ...

    @overload
    def _exec_sql(
        self,
        sql,
        arguments=None,
        *,
        returning_id: Literal[True],
        start_transaction: bool = False,
    ) -> int: ...

    def _exec_sql(
        self,
        sql,
        arguments=None,
        returning_id: bool = False,
        start_transaction: bool = False,
    ) -> TestCursor | int:
        assert type(arguments) is not list and not returning_id
        self.sql = sql
        self.arguments = arguments
        return test_cursor

    def generate_mapping(self, filename=None, check_tables=True, create_tables=False):
        return Database.generate_mapping(
            self, filename, create_tables=False, check_tables=False
        )
