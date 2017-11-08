from pony.py23compat import StringIO, ExitStack

import os, os.path, sys, unittest
from contextlib import contextmanager

from pony.migrate import questioner

from pony.orm.tests import fixtures
from ponytest import provider, pony_fixtures

try:
    from unittest.mock import patch
except ImportError:
    from mock import patch

muted = True

@provider(fixture='stdout', enabled=False)
@contextmanager
def allow_stdout(test):
    global muted
    muted = False
    yield


@provider(fixture='migration_dir')
@contextmanager
def migration_dir(test):
    if os.path.exists('migrations'):
        import shutil
        shutil.rmtree('migrations')
    os.mkdir('migrations')
    yield

pony_fixtures['class'].extend(
    ['stdout']
)
try:
    pony_fixtures['class'].remove('db')
except ValueError:
    pass

pony_fixtures['test'].extend(
    ['migration_dir', 'db']
)
# pony_fixtures['test'].insert(0, 'separate_process')
# pony_fixtures['test'].append('debug')

def invoke(cmd, args=()):
    class StreamWrapper(object):

        def __init__(self, stream):
            self.stream = stream
            self._stringio_ = StringIO()

        def __getattr__(self, name):
            return getattr(self.stream, name)

        def write(self, s, *args, **kw):
            if not muted:
                self.stream.write(s)
            self._stringio_.write(s)

        @classmethod
        @contextmanager
        def stdout(cls):
            __stdout__ = sys.stdout
            sys.stdout = cls(sys.stdout)
            yield sys.stdout._stringio_
            sys.stdout = __stdout__

    try:
        sys_argv = list(sys.argv)
        sys.argv = ['migrate'] + list(args)
        with StreamWrapper.stdout() as c:
            cmd()
        return c.getvalue()
    finally:
        sys.argv = sys_argv


class TestQuestioner(questioner.Questioner):

    def ask_not_null_addition(self, field_name, model_name):
        return 5

    def ask_not_null_alteration(self, field_name, model_name):
        return 5

    def ask_rename(self, model_name, old_name, new_name):
        return True

    def ask_rename_model(self, old_name, new_name):
        return True

    def ask_merge(self, leaves):
        return True

    def ask_auto_now_add_addition(self, field_name, model_name):
        return True


@provider(fixture='patches')
@contextmanager
def patches(test):
    with ExitStack() as stack:
        stack.enter_context(patch(
            'pony.migrate.writer.InteractiveMigrationQuestioner',
            TestQuestioner
        ))
        stack.enter_context(patch(
            'pony.migrate.command.InteractiveMigrationQuestioner',
            TestQuestioner
        ))
        yield


@provider(fixture='names', enabled=False)
@contextmanager
def names(test):
    print('\n{sep} {klass}.{method_name} {sep}'.format(**{
        'sep': '-' * 50,
        'klass': test.__class__.__name__,
        'method_name': test._testMethodName,
    }))
    yield


pony_fixtures['test'].extend(
    ['names']
)
pony_fixtures['class'].extend(
    ['patches']
)

