'''
Command to launch tests:
python -m pony.testing <args> [--ipdb] [--log]
'''

from unittest import suite, loader, TestCase as _TestCase, TestProgram as _TestProgram

from functools import wraps
import logging
from contextlib import contextmanager
import sys

@contextmanager
def noop():
    yield

class TestCase(_TestCase):
    def __getattr__(self, key):
        if key.startswith('PONY_'):
            method = getattr(self, key[5:])
            if callable(method):
                return self.wrap_test(method)
        raise AttributeError

    @contextmanager
    def log_mgr(self):
        from pony.orm.core import debug as debug_mode, sql_debug
        logger = logging.getLogger()
        level = logger.level
        logger.setLevel(logging.INFO)
        sql_debug(True)
        yield
        logger.setLevel(level)
        sql_debug(debug_mode)

    def wrap_test(self, func):
        @wraps(func)
        def wrapper(*args, **kw):
            import ipdb
            debug_mgr = ipdb.launch_ipdb_on_exception if self.pony['debug'] else noop
            log_mgr = self.log_mgr if self.pony['log'] else noop
            with log_mgr(), debug_mgr():
                try:
                    return func(*args, **kw)
                except Exception as exc:
                    raise
            raise exc
        return wrapper


class PonySuite(suite.TestSuite):
    def __iter__(self):
        for test in super(PonySuite, self).__iter__():
            if not isinstance(test, TestCase):
                yield test
                continue
            test.pony = self.pony
            test._testMethodName = 'PONY_%s' % test._testMethodName
            yield test


class PonyLoader(loader.TestLoader):
    def suiteClass(self, tests):
        ret = PonySuite(tests)
        ret.pony = self.pony
        return ret


class TestProgram(_TestProgram):
    def __init__(self, *args, **kwargs):
        try:
            sys.argv.remove('--ipdb')
            debug = True
        except ValueError:
            debug = False
        try:
            sys.argv.remove('--log')
            log = True
        except ValueError:
            log = False
        if log or debug:
            loader = PonyLoader()
            loader.pony = {'log': log, 'debug': debug}
            kwargs['testLoader'] = loader
        super(TestProgram, self).__init__(*args, **kwargs)

