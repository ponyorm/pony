from pony.py23compat import ExitStack

import os, os.path, unittest
from glob import glob
from datetime import datetime
from contextlib import contextmanager

from ponytest import provider

try:
    from unittest.mock import patch
except ImportError:
    from mock import patch

from pony import orm
from pony.migrate import command, questioner


class TestInitial(unittest.TestCase):

    include_fixtures = {
        'class': ['db']
    }

    def test(self):
        db_args, db_kwargs = self.db_params
        db = orm.Database(*db_args, **db_kwargs)

        class MyEntity(db.Entity):
            int_f = orm.Required(int)
            str_f = orm.Required(str)
            other = orm.Set('OtherEntity')

        class OtherEntity(db.Entity):
            fk = orm.PrimaryKey(MyEntity)

        command.migrate(db, 'make -v'); command.migrate(db, 'apply -v')


class TestChange(unittest.TestCase):

    include_fixtures = {
        'test': ['db', 'clear_sequences']
    }
    exclude_fixtures = {
        'class': ['db', 'clear_sequences']
    }

    def setUp(self):
        db_args, db_kwargs = self.db_params

        self.__class__.DB = db = orm.Database(*db_args, **db_kwargs)

        class MyEntity(db.Entity):
            int_f = orm.Required(int)
            str_f = orm.Required(str)

        command.migrate(db, 'make -v'); command.migrate(db, 'apply -v')

    def test_type(self):
        db_args, db_kwargs = self.db_params
        db = orm.Database(*db_args, **db_kwargs)

        class MyEntity(db.Entity):
            int_f = orm.Required(str)
            str_f = orm.Required(str)

        command.migrate(db, 'make -v'); command.migrate(db, 'apply -v')

    def test_nullable(self):
        db_args, db_kwargs = self.db_params
        db = orm.Database(*db_args, **db_kwargs)

        class MyEntity(db.Entity):
            int_f = orm.Required(int, nullable=True)
            str_f = orm.Required(str)

        command.migrate(db, 'make -v'); command.migrate(db, 'apply -v')


class TestDropPk(unittest.TestCase):

    exclude_fixtures = {
        'test': ['db', 'clear_tables']
    }
    include_fixtures = {
        'class': ['db']
    }

    def test0(self):
        db_args, db_kwargs = self.db_params

        db = orm.Database(*db_args, **db_kwargs)

        class MyEntity(db.Entity):
            int_f = orm.PrimaryKey(int)
            str_f = orm.Required(str)

        command.migrate(db, 'make -v'); command.migrate(db, 'apply -v')

    def test1(self):
        db_args, db_kwargs = self.db_params

        db = orm.Database(*db_args, **db_kwargs)

        class MyEntity(db.Entity):
            int_f = orm.Required(int)
            str_f = orm.Required(str)

        command.migrate(db, 'make -v')
        command.migrate(db, 'apply -v')

    # FIXME incorrect pk migration
    # def test_data(self):
    #     with ExitStack() as stack:
    #         # FIXME
    #         from pony.orm.core import OrmError
    #         stack.enter_context(self.assertRaises(OrmError))
    #         db = run_path('entities.py')['db']
    #         stack.callback(db.disconnect)
    #         stack.enter_context(db_session)
    #         db.generate_mapping(create_tables=False)
    #         db.MyEntity(int_f=1, str_f='s')
    #         db.MyEntity(int_f=1, str_f='s')


class TestDropFk(unittest.TestCase):

    exclude_fixtures = {
        'test': ['db', 'migration_dir']
    }
    include_fixtures = {
        'class': ['db', 'migration_dir']
    }

    def test0(self):
        db_args, db_kwargs = self.db_params

        db = orm.Database(*db_args, **db_kwargs)

        class Activity(db.Entity):
            descr = orm.Required(str)
            periods = orm.Set('Time')

        class Time(db.Entity):
            amount = orm.Required(int)
            spent_on = orm.Required('Activity')

        db.generate_mapping(create_tables=False, check_tables=False)

        command.migrate(db, 'make -v'); command.migrate(db, 'apply -v')

    def test2(self):
        db_args, db_kwargs = self.db_params

        db = orm.Database(*db_args, **db_kwargs)

        class Activity(db.Entity):
            descr = orm.Required(str)

        class Time(db.Entity):
            amount = orm.Required(int)
            spent_on = orm.Required(str)

        db.generate_mapping(create_tables=False, check_tables=False)

        command.migrate(db, 'make -v'); command.migrate(db, 'apply -v')


class TestAddFk(unittest.TestCase):

    exclude_fixtures = {
        'test': ['db', 'migration_dir']
    }
    include_fixtures = {
        'class': ['db', 'migration_dir']
    }

    def test0(self):
        db_args, db_kwargs = self.db_params

        db = orm.Database(*db_args, **db_kwargs)

        class Activity(db.Entity):
            descr = orm.Required(str)

        command.migrate(db, 'make -v'); command.migrate(db, 'apply -v')

    def test1(self):
        db_args, db_kwargs = self.db_params

        db = orm.Database(*db_args, **db_kwargs)

        class Activity(db.Entity):
            descr = orm.Required(str)
            periods = orm.Set('Time')

        class Time(db.Entity):
            amount = orm.Required(int)
            spent_on = orm.Required('Activity')

        command.migrate(db, 'make -v'); command.migrate(db, 'apply -v')



class TestDropTable(unittest.TestCase):

    include_fixtures = {
        'class': ['migration_dir', 'db']
    }
    exclude_fixtures = {
        'test': ['migration_dir', 'db']
    }

    def test0(self):
        db_args, db_kwargs = self.db_params

        db = orm.Database(*db_args, **db_kwargs)

        class Activity(db.Entity):
            descr = orm.Required(str)

        command.migrate(db, 'make -v'); command.migrate(db, 'apply -v')


    def test1(self):
        db_args, db_kwargs = self.db_params

        db = orm.Database(*db_args, **db_kwargs)

        class Activity(db.Entity):
            descr = orm.Optional('Description')

        class Description(db.Entity):
            text = orm.Required(str)
            activity = orm.Required('Activity')

        command.migrate(db, 'make -v'); command.migrate(db, 'apply -v')

    def test2(self):
        db_args, db_kwargs = self.db_params

        db = orm.Database(*db_args, **db_kwargs)

        class Activity(db.Entity):
            descr = orm.Required(str)

        command.migrate(db, 'make -v'); command.migrate(db, 'apply -v')



class TestDropDependent(unittest.TestCase):

    include_fixtures = {
        'class': ['migration_dir', 'db']
    }
    exclude_fixtures = {
        'test': ['migration_dir', 'db']
    }

    def test0(self):
        db_args, db_kwargs = self.db_params

        db = orm.Database(*db_args, **db_kwargs)

        class T1(db.Entity):
            t2 = orm.Required('T2')
            t3 = orm.Optional('T3')

        class T2(db.Entity):
            t3 = orm.Required('T3')
            t1 = orm.Optional('T1')

        class T3(db.Entity):
            t1 = orm.Required('T1')
            t2 = orm.Optional('T2')

        command.migrate(db, 'make -v'); command.migrate(db, 'apply -v')

    def test1(self):
        db_args, db_kwargs = self.db_params

        db = orm.Database(*db_args, **db_kwargs)

        command.migrate(db, 'make -v'); command.migrate(db, 'apply -v')



class TestDeps2(unittest.TestCase):

    include_fixtures = {
        'class': ['migration_dir', 'db']
    }
    exclude_fixtures = {
        'test': ['migration_dir', 'db']
    }

    def __call__(self, *args):
        class Questioner(questioner.InteractiveMigrationQuestioner):
            def ask_rename(self, model_name, old_name, new_name):
                return False
            def ask_rename_model(self, old_name, new_name):
                return False

        with ExitStack() as stack:
            stack.enter_context(patch(
                'pony.migrate.writer.InteractiveMigrationQuestioner',
                Questioner
            ))
            stack.enter_context(patch(
                'pony.migrate.command.InteractiveMigrationQuestioner',
                Questioner
            ))
            return unittest.TestCase.__call__(self, *args)

    def test0(self):
        db_args, db_kwargs = self.db_params

        db = orm.Database(*db_args, **db_kwargs)

        class E(db.Entity):
            t2 = orm.Required('T2')

        class T1(db.Entity):
            t2 = orm.Required('T2')
            t3 = orm.Optional('T3')

        class T2(db.Entity):
            t3 = orm.Required('T3')
            t1 = orm.Optional('T1')
            e = orm.Optional('E')

        class T3(db.Entity):
            t1 = orm.Required('T1')
            t2 = orm.Optional('T2')

        db.generate_mapping(create_tables=False, check_tables=False)

        command.migrate(db, 'make -v'); command.migrate(db, 'apply -v')

    def test1(self):
        db_args, db_kwargs = self.db_params
        db = orm.Database(*db_args, **db_kwargs)

        class E(db.Entity):
            t1 = orm.Required('T1')

        class T1(db.Entity):
            e = orm.Optional('E')

        db.generate_mapping(create_tables=False, check_tables=False)

        command.migrate(db, 'make -v'); command.migrate(db, 'apply -v')



# class TestM2m(unittest.TestCase):

#     include_fixtures = {
#         'class': ['migration_dir', 'db']
#     }
#     exclude_fixtures = {
#         'test': ['migration_dir', 'db']
#     }

#     def test0(self):
#         db_args, db_kwargs = self.db_params
#         with open('entities.py', 'w') as f:
#             f.write('''\
# from pony.orm import *

# db = Database(*{}, **{})

# class T1(db.Entity):
#     peers = Set('T2')

# class T2(db.Entity):
#     peers = Set('T1')

#             '''.format(repr(db_args), repr(db_kwargs)))

#         invoke(command.migrate)

#     def test1(self):
#         with self.assertRaises(AssertionError):

#             db_args, db_kwargs = self.db_params
#             with open('entities.py', 'w') as f:
#                 f.write('''\
# from pony.orm import *

# db = Database(*{}, **{})

# class T1(db.Entity):
#     peers = Set('T1')
#     peered = Set('T1')

#                 '''.format(repr(db_args), repr(db_kwargs)))

#             s = invoke(command.migrate)

#     def test2(self):
#         db_args, db_kwargs = self.db_params
#         with open('entities.py', 'w') as f:
#             f.write('''\
# from pony.orm import *

# db = Database(*{}, **{})

# class T1(db.Entity):
#     peers = Set('T2')

# class T2(db.Entity):
#     peers = Set('T1')

#             '''.format(repr(db_args), repr(db_kwargs)))

#         s = invoke(command.migrate)


@unittest.skip('pk migrations not supported yet')
class TestAddPk(unittest.TestCase):

    def setUp(self):
        db_args, db_kwargs = self.db_params
        with open('entities.py', 'w') as f:
            f.write('''\
from pony.orm import *

db = Database(*{}, **{})

class MyEntity(db.Entity):
    int_f = Optional(int)
    str_f = Required(str)

            '''.format(repr(db_args), repr(db_kwargs)))

        command.migrate(db, 'make -v'); command.migrate(db, 'apply -v')


    def test_compound(self):
        db_args, db_kwargs = self.db_params
        with open('entities.py', 'w') as f:
            f.write('''\
from pony.orm import *

db = Database(*{}, **{})

class MyEntity(db.Entity):
    int_f = Required(int)
    str_f = Required(str)

    PrimaryKey(int_f, str_f)

                '''.format(repr(db_args), repr(db_kwargs)))

        command.migrate(db, 'make -v'); command.migrate(db, 'apply -v')

    def test_simple(self):
        db_args, db_kwargs = self.db_params
        with open('entities.py', 'w') as f:
            f.write('''\
from pony.orm import *

db = Database(*{}, **{})

class MyEntity(db.Entity):
    int_f = PrimaryKey(int)
    str_f = Required(str)

                '''.format(repr(db_args), repr(db_kwargs)))

        command.migrate(db, 'make -v'); command.migrate(db, 'apply -v')


class TestRenameTable(unittest.TestCase):

    include_fixtures = {
        'class': ['migration_dir', 'db']
    }
    exclude_fixtures = {
        'test': ['migration_dir', 'db']
    }

    def setUp(self):
        pass  # orm.sql_debug(True)

    def test0(self):
        db_args, db_kwargs = self.db_params

        db = orm.Database(*db_args, **db_kwargs)

        class T1(db.Entity):
            peers = orm.Set('T2')
            f = orm.Required(int)

        class T2(db.Entity):
            peers = orm.Set('T1')

        db.generate_mapping(check_tables=False, create_tables=False)

        command.migrate(db, 'make -v'); command.migrate(db, 'apply -v')

    def test1(self):
        db_args, db_kwargs = self.db_params

        db = orm.Database(*db_args, **db_kwargs)

        class U(db.Entity):
            peers = orm.Set('T2')
            f = orm.Required(int)

        class T2(db.Entity):
            peers = orm.Set('U')

        db.generate_mapping(check_tables=False, create_tables=False)

        command.migrate(db, 'make -v'); command.migrate(db, 'apply -v')


class TestDefaults(unittest.TestCase):
    class Questioner(questioner.Questioner):
        def ask_not_null_addition(self, field_name, model_name):
            return 3

    @provider(fixture='questioner')
    @contextmanager
    def set_questioner(test, Questioner=Questioner):
        with ExitStack() as stack:
            stack.enter_context(patch(
                'pony.migrate.writer.InteractiveMigrationQuestioner',
                Questioner
            ))
            stack.enter_context(patch(
                'pony.migrate.command.InteractiveMigrationQuestioner',
                Questioner
            ))
            yield

    include_fixtures = {
        'class': ['questioner', 'db', 'migration_dir']
    }
    exclude_fixtures = {
        'test': ['migration_dir', 'db']
    }

    def test0(self):
        db_args, db_kwargs = self.db_params

        db = orm.Database(*db_args, **db_kwargs)

        class T1(db.Entity):
            peers = orm.Set('T2')
            f = orm.Optional(int)

        class T2(db.Entity):
            peers = orm.Set('T1')

        command.migrate(db, 'make -v'); command.migrate(db, 'apply -v')


    def test1(self):
        db_args, db_kwargs = self.db_params
        db = orm.Database(*db_args, **db_kwargs)

        class T1(db.Entity):
            peers = orm.Set('T2')
            f = orm.Required(int)

        class T2(db.Entity):
            peers = orm.Set('T1')

        command.migrate(db, 'make -v'); command.migrate(db, 'apply -v')


class TestMerge(unittest.TestCase):

    exclude_fixtures = {
        'test': ['db', 'migration_dir', 'clear_tables']
    }
    include_fixtures = {
        'class': ['db', 'migration_dir']
    }

    def setUp(self):
        pass # orm.sql_debug(True)

    def test0(self):
        db_args, db_kwargs = self.db_params
        db = self.__class__.db = orm.Database(*db_args, **db_kwargs)

        class T1(db.Entity):
            peers = orm.Set('T2')

        class T2(db.Entity):
            peers = orm.Set('T1')

        command.migrate(db, 'make -v'); command.migrate(db, 'apply -v')


    def test1(self):
        db_args, db_kwargs = self.db_params
        p = os.path.join('migrations', '0002.py')
        with open(p, 'w') as f:
            f.write('''\
from pony import orm
from pony.migrate import diagram_ops as op

dependencies = ['0001_initial']

operations = [
    op.AddAttr('T1', 'f', orm.Optional(int, initial=True)),
]
    '''.format(db_args, db_kwargs))

        command.migrate(self.db, 'apply -v')

    def test2(self):
        p = os.path.join('migrations', '0003.py')
        with open(p, 'w') as f:
            f.write('''\
from pony import orm
from pony.migrate import diagram_ops as op

dependencies = ['0001_initial']

operations = [
    op.AddAttr('T1', 'g', orm.Optional(int, initial=True)),
]
            ''')

    def test3(self):
        command.migrate(self.db, 'apply -v')


class TestSqlite(unittest.TestCase):

    exclude_fixtures = {
        'test': ['db', 'migration_dir', 'clear_tables']
    }
    include_fixtures = {
        'class': ['db', 'migration_dir']
    }
    # fixture_providers = {
    #     'db': ['sqlite_no_json1']
    # }

    def setUp(self):
        pass  # orm.sql_debug(True)

    def test0(self):
        db_args, db_kwargs = self.db_params
        db = self.__class__.DB = orm.Database(*db_args, **db_kwargs)

        class Data(db.Entity):
            label = orm.Optional(str)
            created = orm.Required(datetime)

        db.generate_mapping(create_tables=False, check_tables=False)

        command.migrate(db, 'make -v'); command.migrate(db, 'apply -v')


    def test1(self):
        db = self.DB

        with orm.db_session:
            db.Data(created=datetime.now(), label='data')
            db.Data(created=datetime.now(), label='data1')

        db.disconnect()

    def test2(self):
        db_args, db_kwargs = self.db_params
        db = orm.Database(*db_args, **db_kwargs)

        class Data(db.Entity):
            created = orm.Required(str)

        command.migrate(db, 'make -v'); command.migrate(db, 'apply -v')


class TestSqliteAddColumn(unittest.TestCase):

    include_fixtures = {
        'class': ['db', 'migration_dir']
    }
    exclude_fixtures = {
        'test': ['db', 'migration_dir']
    }

    def test0(self):
        db_args, db_kwargs = self.db_params
        db = orm.Database(*db_args, **db_kwargs)

        class Data(db.Entity):
            pass

        command.migrate(db, 'make -v'); command.migrate(db, 'apply -v')

    def test1(self):
        db_args, db_kwargs = self.db_params
        db = orm.Database(*db_args, **db_kwargs)

        class Data(db.Entity):
            number = orm.Optional(int)
            created = orm.Optional(int)

        command.migrate(db, 'make -v'); command.migrate(db, 'apply -v')


class TestNoChanges(unittest.TestCase):

    include_fixtures = {
        'class': ['db', 'migration_dir']
    }
    exclude_fixtures = {
        'test': ['db', 'migration_dir']
    }

    def test0(self):
        db_args, db_kwargs = self.db_params
        db = orm.Database(*db_args, **db_kwargs)

        class MyEntity(db.Entity):
            int_f = orm.Required(int)
            str_f = orm.Required(str)


        command.migrate(db, 'make -v'); command.migrate(db, 'apply -v')

    def test1(self):
        db_args, db_kwargs = self.db_params
        db = orm.Database(*db_args, **db_kwargs)

        class MyEntity(db.Entity):
            int_f = orm.Required(int)
            str_f = orm.Required(str)

        command.migrate(db, 'make -v'); command.migrate(db, 'apply -v')



class TestGenerateMapping(unittest.TestCase):

    exclude_fixtures = {
        'test': ['db', 'migration_dir']
    }
    include_fixtures = {
        'class': ['db', 'migration_dir']
    }


    def test0(self):
        db_args, db_kwargs = self.db_params
        db = orm.Database(*db_args, **db_kwargs)

        class Data(db.Entity):
            label = orm.Optional(str)
            created = orm.Required(datetime)

        db.generate_mapping(create_tables=False, check_tables=False)

        command.migrate(db, 'make -v'); command.migrate(db, 'apply -v')

    def test1(self):
        db_args, db_kwargs = self.db_params
        self.__class__.DB = db = orm.Database(*db_args, **db_kwargs)

        class Data(db.Entity):
            label = orm.Optional(str)

        db.generate_mapping(create_tables=False, check_tables=False)

        command.migrate(db, 'make -v'); command.migrate(db, 'apply -v')

    def test_2insert(self):
        db = self.DB
        with ExitStack() as stack:
            stack.callback(db.disconnect)
            stack.enter_context(orm.db_session)

            db.Data(label='label')

            labels = orm.select(d.label for d in db.Data)[:]
            self.assertEqual(labels, ['label'])


class TestOptional(unittest.TestCase):
    exclude_fixtures = {
        'test': ['db', 'migration_dir']
    }
    include_fixtures = {
        'class': ['db', 'migration_dir']
    }

    def test0(self):
        db_args, db_kwargs = self.db_params
        self.__class__.DB = db = orm.Database(*db_args, **db_kwargs)

        class Data(db.Entity):
            created = orm.Required(datetime)

        db.generate_mapping(create_tables=False, check_tables=False)

        command.migrate(db, 'make -v'); command.migrate(db, 'apply -v')

    def test1(self):
        with ExitStack() as stack:
            db = self.DB
            stack.callback(db.disconnect)
            stack.enter_context(orm.db_session)
            db.Data(created=datetime.now())

    def test2(self):
        db_args, db_kwargs = self.db_params
        db = orm.Database(*db_args, **db_kwargs)

        class Data(db.Entity):
            label = orm.Optional(str)
            created = orm.Required(datetime)

        db.generate_mapping(create_tables=False, check_tables=False)

        command.migrate(db, 'make -v'); command.migrate(db, 'apply -v')


class TestFake(unittest.TestCase):

    exclude_fixtures = {
        'test': ['db', 'migration_dir']
    }
    include_fixtures = {
        'class': ['db', 'migration_dir']
    }

    def test0(self):
        db_args, db_kwargs = self.db_params
        db = self.__class__.DB = orm.Database(*db_args, **db_kwargs)

        class Data(db.Entity):
            label = orm.Optional(str)
            created = orm.Required(datetime)


    def test1(self):
        db = self.DB
        with ExitStack() as stack:
            stack.callback(db.disconnect)
            db.generate_mapping(create_tables=True, check_tables=True)

    def test2(self):
        db = self.DB
        command.migrate(db, 'make -v')
        command.migrate(db, 'apply --fake -v')


class TestUnique(unittest.TestCase):

    exclude_fixtures = {
        'test': ['db', 'migration_dir']
    }
    include_fixtures = {
        'class': ['db', 'migration_dir']
    }

    def test0(self):
        db_args, db_kwargs = self.db_params
        db = orm.Database(*db_args, **db_kwargs)

        class Data(db.Entity):
            label = orm.Required(str)

        db.generate_mapping(create_tables=False, check_tables=False)

        command.migrate(db, 'make -v'); command.migrate(db, 'apply -v')

    def test1(self):
        db_args, db_kwargs = self.db_params

        db = orm.Database(*db_args, **db_kwargs)

        class Data(db.Entity):
            label = orm.Required(str, unique=True)

        db.generate_mapping(create_tables=False, check_tables=False)

        command.migrate(db, 'make -v'); command.migrate(db, 'apply -v')



class TestSqliteForeignKey(unittest.TestCase):

    exclude_fixtures = {
        'test': ['db', 'migration_dir']
    }
    include_fixtures = {
        'class': ['db', 'migration_dir']
    }

    def test0(self):
        db_args, db_kwargs = self.db_params
        self.__class__.DB = db = orm.Database(*db_args, **db_kwargs)

        class Info(db.Entity):
            content = orm.Required(str)
            source = orm.Required('Source')

        class Source(db.Entity):
            name = orm.Required(str)
            info = orm.Optional('Info')

        db.generate_mapping(create_tables=False, check_tables=False)

        command.migrate(db, 'make -v'); command.migrate(db, 'apply -v')

    def test1(self):
        db = self.DB

        with orm.db_session:
            src = db.Source(name='BBC')
            news = db.Info(content='News', source=src)
        db.disconnect()

    def test2(self):
        db_args, db_kwargs = self.db_params
        self.__class__.DB = db = orm.Database(*db_args, **db_kwargs)

        class News(db.Entity):
            content = orm.Required(str)
            source = orm.Required('Source')

        class Source(db.Entity):
            name = orm.Required(str)
            info = orm.Optional('News')

        db.generate_mapping(create_tables=False, check_tables=False)

        command.migrate(db, 'make -v'); command.migrate(db, 'apply -v')

    def test3(self):
        with orm.db_session:
            db = self.DB
            news = db.News.select().first()
            db.Source(name='twitter', info=news)
        db.disconnect()


class TestExistingDefault(unittest.TestCase):

    include_fixtures = {
        'class': ['db', 'migration_dir']
    }
    exclude_fixtures = {
        'test': ['migration_dir', 'db']
    }

    def test0(self):
        db_args, db_kwargs = self.db_params

        db = orm.Database(*db_args, **db_kwargs)

        class T1(db.Entity):
            peers = orm.Set('T2')
            f = orm.Required(int, sql_default='1')

        class T2(db.Entity):
            peers = orm.Set('T1')

        command.migrate(db, 'make -v'); command.migrate(db, 'apply -v')


    def test1(self):
        db_args, db_kwargs = self.db_params
        db = orm.Database(*db_args, **db_kwargs)

        class T1(db.Entity):
            peers = orm.Set('T2')
            f = orm.Required(int, sql_default='3')

        class T2(db.Entity):
            peers = orm.Set('T1')

        command.migrate(db, 'make -v'); command.migrate(db, 'apply -v')


class TestAttributeDeclarationId(unittest.TestCase):
    # test Attribute.get_declaration_id

    include_fixtures = {
        'class': ['db', 'migration_dir']
    }
    exclude_fixtures = {
        'test': ['migration_dir', 'db']
    }

    def test0(self):
        class A(self.db.Entity):
            s = orm.Optional(str)

        class B(self.db.Entity):
            s = orm.Optional(str, sql_default="''")

        class C(self.db.Entity):
            s = orm.Optional(str, sql_default="'DEFAULT'")

        self.db.generate_mapping(create_tables=True)

    def test1(self):
        self.assertEqual(
            self.db.A.s.get_declaration_id(), self.db.B.s.get_declaration_id()
        )

    def test2(self):
        self.assertNotEqual(
            self.db.A.s.get_declaration_id(), self.db.C.s.get_declaration_id()
        )


class TestMakeOptional(unittest.TestCase):
    # test Attribute.get_declaration_id

    include_fixtures = {
        'class': ['migration_dir', 'db']
    }
    exclude_fixtures = {
        'test': ['migration_dir', 'db']
    }

    def test0(self):
        db_args, db_kwargs = self.db_params

        db = orm.Database(*db_args, **db_kwargs)

        class Activity(db.Entity):
            s = orm.Required(str)

        command.migrate(db, 'make -v'); command.migrate(db, 'apply -v')


    def test1(self):
        db_args, db_kwargs = self.db_params

        db = orm.Database(*db_args, **db_kwargs)

        class Activity(db.Entity):
            s = orm.Optional(str)

        command.migrate(db, 'make -v'); command.migrate(db, 'apply -v')

        assert len(glob('migrations/*')) == 2
