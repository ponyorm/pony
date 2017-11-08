from pony.py23compat import ExitStack, StringIO

import os, os.path, sys, unittest
from contextlib import contextmanager

from pony.orm.tests import fixtures
from ponytest import provider, pony_fixtures

from pony import orm
from pony.migrate import command, migration
from pony.migrate.utils import run_path


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
    ['stdout', 'migration_dir']
)


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

    with StreamWrapper.stdout() as c:
        cmd(standalone_mode=False, args=args)
    return c.getvalue()




class Test(unittest.TestCase):
    '''
    BUG:
    Let an entity have 2 set attributes.
    So we have 2 tables to store relations.
    We delete the first attribute. The corresponding relation table is
    continued to be used for 2nd attribute relation.
    '''


    def test_1(self):
        p = os.path.join('migrations', '0001_initial.py')
        with open(p, 'w') as f:
            f.write('''\
from pony import orm

dependencies = []

def define_entities(db):

    class A(db.Entity):
        name = orm.Required(str)
        peers1_B = orm.Set('B', reverse='peered1_A')
        peered1_B = orm.Set('B', reverse='peers1_A')
        peers2_B = orm.Set('B', reverse='peered2_A')
        peered2_B = orm.Set('B', reverse='peers2_A')

    class B(db.Entity):
        name = orm.Required(str)
        peers1_A = orm.Set('A', reverse='peered1_B')
        peered1_A = orm.Set('A', reverse='peers1_B')
        peers2_A = orm.Set('A', reverse='peered2_B')
        peered2_A = orm.Set('A', reverse='peers2_B')

        ''')

        s = invoke(command.migrate, ['apply', '-v'])

        define_entities = run_path(p)['define_entities']
        p = 'entities.py'
        db = run_path(p)['db']
        with ExitStack() as stack:
            stack.callback(db.disconnect)
            db = migration.reconstruct_db(db)
            stack.callback(db.disconnect)
            define_entities(db)
            db.generate_mapping(create_tables=False)
            stack.enter_context(orm.db_session)
            a1 = db.A(name='a1')
            a2 = db.A(name='a2')
            b1 = db.B(name='b1', peers1_A=[a1])
            b2 = db.B(name='b2', peers2_A=[a2])

            assert not list(db.A.get(name='a1').peered2_B)


    def test_2(self):
        p = os.path.join('migrations', '0002.py')
        with open(p, 'w') as f:
            f.write('''\
from pony import orm

dependencies = ['0001_initial']

def define_entities(db):

    class A(db.Entity):
        name = orm.Required(str)
        peers2_B = orm.Set('B', reverse='peered2_A')
        peered2_B = orm.Set('B', reverse='peers2_A')

    class B(db.Entity):
        name = orm.Required(str)
        peers2_A = orm.Set('A', reverse='peered2_B')
        peered2_A = orm.Set('A', reverse='peers2_B')
        ''')

        s = invoke(command.migrate, ['apply', '-v'])

        define_entities = run_path(p)['define_entities']
        p = 'entities.py'
        db = run_path(p)['db']
        with ExitStack() as stack:
            stack.callback(db.disconnect)
            db = migration.reconstruct_db(db)
            stack.callback(db.disconnect)
            define_entities(db)
            db.generate_mapping(create_tables=False)

            with orm.db_session:
                self.assertEqual(list(db.A.get(name='a1').peered2_B), [])


