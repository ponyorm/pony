import glob, unittest

from pony import orm
from pony.migrate import command

# db is created for every test


class TestApply(unittest.TestCase):

    include_fixtures = {
        'class': ['migration_dir', 'db']
    }
    exclude_fixtures = {
        'test': ['migration_dir', 'db']
    }

    def test_0(self):
        db_args, db_kwargs = self.db_params

        self.__class__.DB = db = orm.Database(*db_args, **db_kwargs)

        class MyEntity(db.Entity):
            int_f = orm.Required(int)


        command.migrate(db, 'make')


        # [p] = glob.glob('./migrations/0001*')
        # with open(p) as f:
        #     s  = f.read()
        # print(s)



    def test_1(self):
        db_args, db_kwargs = self.db_params

        db = orm.Database(*db_args, **db_kwargs)

        class MyEntity(db.Entity):
            int_f = orm.Required(int)
            s = orm.Required(str)


        command.migrate(db, 'make -v')
        # [p] = glob.glob('./migrations/0002*')
        # with open(p) as f:
        #     s  = f.read()
        # print(s)

    def test_2(self):
        command.migrate(self.DB, 'apply')


class TestAddEntity(unittest.TestCase):

    include_fixtures = {
        'class': ['migration_dir', 'db']
    }
    exclude_fixtures = {
        'test': ['migration_dir', 'db']
    }

    def test_0(self):
        command.migrate(self.db, 'make')
        # [p] = glob.glob('./migrations/0001*')
        # with open(p) as f:
        #     s  = f.read()

    def test_2(self):
        command.migrate(self.db, 'apply')

    def test_1(self):
        db_args, db_kwargs = self.db_params
        db = orm.Database(*db_args, **db_kwargs)


        class MyEntity(db.Entity):
            int_f = orm.Required(int)
            s = orm.Required(str)

        db.generate_mapping(create_tables=False, check_tables=False)


        command.migrate(db, 'make -v')
        [p] = glob.glob('./migrations/0002*')
        with open(p) as f:
            s  = f.read()
        self.assertTrue('op.AddEntity' in s)




class TestRename(unittest.TestCase):

    include_fixtures = {
        'class': ['migration_dir', 'db']
    }
    exclude_fixtures = {
        'test': ['migration_dir', 'db']
    }

    def test_1(self):
        db_args, db_kwargs = self.db_params
        db = self.__class__.DB = orm.Database(*db_args, **db_kwargs)

        class MyEntity(db.Entity):
            int_f = orm.Required(int)
            s = orm.Required(str)

        command.migrate(db, 'make -v')
        command.migrate(db, 'apply -v')

    def test_2(self):
        with open('migrations/0002.py', 'w') as f:
            f.write('''
from pony.migrate import diagram_ops as op

dependencies = ['0001_initial']

operations = [
    op.RenameAttr('MyEntity', 's', 't'),
    op.RenameEntity('MyEntity', 'YourEntity'),
]
            ''')
        command.migrate(self.DB, 'apply -v')




class TestDefault(unittest.TestCase):

    include_fixtures = {
        'class': ['migration_dir', 'db']
    }
    exclude_fixtures = {
        'test': ['migration_dir', 'db']
    }

    def test_1(self):
        db_args, db_kwargs = self.db_params
        self.__class__.DB = db = orm.Database(*db_args, **db_kwargs)

        class MyEntity(db.Entity):
            int_f = orm.Required(int)

        command.migrate(db, 'make -v')
        command.migrate(db, 'apply -v')

    def test_2(self):
        with open('migrations/0002.py', 'w') as f:
            f.write('''
from pony.migrate import diagram_ops as op
from pony.orm import *

dependencies = ['0001_initial']

operations = [
    op.AddAttr('MyEntity', 's', Required(str, initial='initial')),
]
            ''')

        command.migrate(self.DB, 'apply -v')


class TestRenamesMixed(unittest.TestCase):

    include_fixtures = {
        'class': ['migration_dir', 'db']
    }
    exclude_fixtures = {
        'test': ['migration_dir', 'db']
    }

    def test_1(self):
        db_args, db_kwargs = self.db_params

        self.__class__.DB = db = orm.Database(*db_args, **db_kwargs)

        class MyEntity(db.Entity):
            int_f = orm.Required(int)
            s = orm.Required(str)

        command.migrate(db, 'make -v')
        command.migrate(db, 'apply -v')

    def test_2(self):
        with open('migrations/0002.py', 'w') as f:
            f.write('''
from pony.migrate import diagram_ops as op
from pony.orm import *

dependencies = ['0001_initial']

operations = [
    op.RenameAttr('MyEntity', 's', 't'),
    op.RenameEntity('MyEntity', 'YourEntity'),
    op.AddAttr('YourEntity', 'u', Optional(int)),
]
            ''')

        db = self.DB
        command.migrate(db, 'apply -v')




class TestCustomOp(unittest.TestCase):

    include_fixtures = {
        'class': ['migration_dir', 'db']
    }
    exclude_fixtures = {
        'test': ['migration_dir', 'clear_tables', 'db']
    }

    def test_1(self):
        db_args, db_kwargs = self.db_params

        self.__class__.DB = db = orm.Database(*db_args, **db_kwargs)

        class MyEntity(db.Entity):
            int_f = orm.Required(int)
            s = orm.Required(str)

        command.migrate(db, 'make -v')
        command.migrate(db, 'apply -v')

    def test_2(self):
        with open('migrations/0002.py', 'w') as f:
            f.write('''
from pony.migrate import diagram_ops as op
from pony.orm import *

dependencies = ['0001_initial']

def forward(db):
    db.MyEntity(int_f=5, s='s', d='d')

operations = [
    op.AddAttr('MyEntity', 'd', Optional(str, initial='d')),
    op.Custom(forward=forward),
    op.AddAttr('MyEntity', 'e', Optional(str, initial='e')),
]
            ''')

        db = self.DB
        command.migrate(db, 'apply -v')



class TestCustomizeOp(unittest.TestCase):

    include_fixtures = {
        'class': ['migration_dir', 'db']
    }
    exclude_fixtures = {
        'test': ['migration_dir', 'clear_tables', 'db']
    }

    def test_1(self):
        db_args, db_kwargs = self.db_params

        self.__class__.DB = db = orm.Database(*db_args, **db_kwargs)

        class MyEntity(db.Entity):
            int_f = orm.Required(int)
            s = orm.Required(str)

        command.migrate(db, 'make -v')
        command.migrate(db, 'apply -v')

    def test_2(self):
        with open('migrations/0002.py', 'w') as f:
            f.write('''
from pony.migrate import diagram_ops as op
from pony.orm import *

dependencies = ['0001_initial']

def forward(db):
    print('SUCCESS!!!')

operations = [
    op.AddAttr('MyEntity', 'd', Optional(str, initial='d'), forward=forward),
]
            ''')

        db = self.DB
        command.migrate(db, 'apply -v')


class TestCustomWithChanges(unittest.TestCase):

    include_fixtures = {
        'class': ['migration_dir', 'db']
    }
    exclude_fixtures = {
        'test': ['migration_dir', 'clear_tables', 'db']
    }
    fixture_providers = {
        'db': ['mysql']
    }

    def test_1(self):
        db_args, db_kwargs = self.db_params

        self.__class__.DB = db = orm.Database(*db_args, **db_kwargs)

        class MyEntity(db.Entity):
            int_f = orm.Required(int)
            s = orm.Required(str)

        command.migrate(db, 'make -v')
        command.migrate(db, 'apply -v')

    def test_2(self):
        with open('migrations/0002.py', 'w') as f:
            f.write('''
from pony.migrate import diagram_ops as op
from pony.orm import *

dependencies = ['0001_initial']

def forward(db):
    print('Custom op')
    db.execute("""
ALTER TABLE `myentity` ADD COLUMN `d` VARCHAR(255) NOT NULL DEFAULT 'd'
    """)

operations = [
    op.Custom(forward=forward, changes=[
        op.AddAttr('MyEntity', 'd', Optional(str)),
    ]),
]
            ''')

        db = self.DB
        command.migrate(db, 'apply -v')

    def test_3(self):
        db_args, db_kwargs = self.db_params
        db = orm.Database(*db_args, **db_kwargs)

        class MyEntity(db.Entity):
            int_f = orm.Required(int)
            s = orm.Required(str)
            d = orm.Required(str)

        db.generate_mapping(create_tables=False)

        with orm.db_session:
            db.MyEntity(int_f=0, s='S', d='D')
            self.assertEqual(
                orm.select(o.d for o in MyEntity)[:], ['D']
            )


class TestAskNotNull(unittest.TestCase):

    include_fixtures = {
        'class': ['migration_dir', 'db']
    }
    exclude_fixtures = {
        'test': ['migration_dir', 'clear_tables', 'db']
    }

    def test_1(self):
        db_args, db_kwargs = self.db_params

        self.__class__.DB = db = orm.Database(*db_args, **db_kwargs)

        class MyEntity(db.Entity):
            int_f = orm.Required(int)
            s = orm.Required(str)

        # db.generate_mapping()

        command.migrate(db, 'make -v')
        command.migrate(db, 'apply -v')

        # import ipdb; ipdb.set_trace()

    def test_2(self):
        db = self.DB
        # db.generate_mapping(create_tables=False)

        with orm.db_session:
            db.MyEntity(int_f=1, s='1')

        db.disconnect()

    def test_3(self):
        db_args, db_kwargs = self.db_params

        db = orm.Database(*db_args, **db_kwargs)

        class MyEntity(db.Entity):
            int_f = orm.Required(int)
            s = orm.Required(str)
            d = orm.Required(str)

        command.migrate(db, 'make -v')
        command.migrate(db, 'apply -v')


class TestAskRenameEntity(unittest.TestCase):

    include_fixtures = {
        'class': ['migration_dir', 'db']
    }
    exclude_fixtures = {
        'test': ['migration_dir', 'clear_tables', 'db']
    }

    def test_1(self):
        db_args, db_kwargs = self.db_params
        db = self.__class__.DB = orm.Database(*db_args, **db_kwargs)

        class MyEntity(db.Entity):
            int_f = orm.Required(int)
            s = orm.Required(str)

        command.migrate(db, 'make -v')
        command.migrate(db, 'apply -v')

    def test_2(self):
        db_args, db_kwargs = self.db_params

        db = orm.Database(*db_args, **db_kwargs)

        class YourEntity(db.Entity):
            int_f = orm.Required(int)
            s = orm.Required(str)

        command.migrate(db, 'make -v')
        command.migrate(db, 'apply -v')
        # self.assertTrue('RENAME TO' in s.upper())


class TestAskRenameAttr(unittest.TestCase):

    include_fixtures = {
        'class': ['migration_dir', 'db']
    }
    exclude_fixtures = {
        'test': ['migration_dir', 'clear_tables', 'db']
    }


    def test_1(self):
        db_args, db_kwargs = self.db_params

        db = self.__class__.DB = orm.Database(*db_args, **db_kwargs)

        class MyEntity(db.Entity):
            int_f = orm.Required(int)
            s = orm.Required(str)

        command.migrate(db, 'make -v')
        command.migrate(db, 'apply -v')

    def test_2(self):
        db_args, db_kwargs = self.db_params

        db = orm.Database(*db_args, **db_kwargs)

        class MyEntity(db.Entity):
            int_g = orm.Required(int)
            s = orm.Required(str)

        command.migrate(db, 'make -v')
        command.migrate(db, 'apply -v')



# @unittest.skip('not implemented')
# class TestMakeNotNull(unittest.TestCase):

#     include_fixtures = {
#         'class': ['migration_dir', 'db']
#     }
#     exclude_fixtures = {
#         'test': ['migration_dir', 'clear_tables', 'db']
#     }

#     def test_1(self):
#         db_args, db_kwargs = self.db_params
#         with open('entities.py', 'w') as f:
#             f.write('''
# from pony.orm import *

# db = Database(*{}, **{})

# class MyEntity(db.Entity):
#     int_f = Optional(int)
#     s = Required(str)
#             '''.format(repr(db_args), repr(db_kwargs)))

#         invoke(command.migrate, ['make', '-v'])
#         invoke(command.migrate, ['apply', '-v'])

#     def test_2(self):
#         db_args, db_kwargs = self.db_params
#         with open('entities.py', 'w') as f:
#             f.write('''
# from pony.orm import *

# db = Database(*{}, **{})

# class MyEntity(db.Entity):
#     int_f = Required(int)
#     s = Required(str)
#             '''.format(repr(db_args), repr(db_kwargs)))

#         invoke(command.migrate, ['make', '-v'])
#         invoke(command.migrate, ['apply', '-v'])



class TestMerge(unittest.TestCase):

    include_fixtures = {
        'class': ['migration_dir', 'db']
    }
    exclude_fixtures = {
        'test': ['migration_dir', 'clear_tables', 'db']
    }

    def test_0(self):
        db_args, db_kwargs = self.db_params

        self.__class__.DB = db = orm.Database(*db_args, **db_kwargs)

        class MyEntity(db.Entity):
            int_f = orm.Required(int)

        command.migrate(db, 'make -v')
        command.migrate(db, 'apply -v')

    def test_1(self):
        db_args, db_kwargs = self.db_params
        with open('migrations/0002.py', 'w') as f:
            f.write('''

from pony.migrate import diagram_ops as ops
from pony.orm import *

dependencies = ['0001_initial']

operations = [
    ops.AddAttr('MyEntity', 's', Optional(str, initial='S'))
]
            ''')


    def test_2(self):
        db_args, db_kwargs = self.db_params
        with open('migrations/0003.py', 'w') as f:
            f.write('''

from pony.migrate import diagram_ops as ops
from pony.orm import *

dependencies = ['0001_initial']

operations = [
    ops.AddAttr('MyEntity', 'int_g', Optional(int, initial=1))
]
            ''')


    def test_3(self):
        db = self.DB
        command.migrate(db, 'make -v')
        command.migrate(db, 'apply -v')


class TestLateBind(unittest.TestCase):

    include_fixtures = {
        'class': ['migration_dir', 'db']
    }
    exclude_fixtures = {
        'test': ['migration_dir', 'clear_tables', 'db']
    }

    def test_0(self):
        db_args, db_kwargs = self.db_params

        self.__class__.DB = db = orm.Database()
        db._bind(*db_args, **db_kwargs)

        class MyEntity(db.Entity):
            int_f = orm.Required(int)

        command.migrate(db, 'make -v')
        command.migrate(db, 'apply -v')

    def test_1(self):
        db_args, db_kwargs = self.db_params
        with open('migrations/0002.py', 'w') as f:
            f.write('''

from pony.migrate import diagram_ops as ops
from pony.orm import *

dependencies = ['0001_initial']

operations = [
    ops.AddAttr('MyEntity', 's', Required(str, initial='S'))
]
            ''')

        db = self.DB
        command.migrate(db, 'apply -v')




class TestCustomWithInheritance(unittest.TestCase):

    include_fixtures = {
        'class': ['migration_dir', 'db']
    }
    exclude_fixtures = {
        'test': ['migration_dir', 'clear_tables', 'db']
    }
    fixture_providers = {
        'db': ['mysql']
    }

    def test_1(self):
        db_args, db_kwargs = self.db_params

        self.__class__.DB = db = orm.Database(*db_args, **db_kwargs)


        class Base(db.Entity):
            int_f = orm.Required(int)

        class MyEntity(Base):
            s = orm.Required(str)

        command.migrate(db, 'make')
        command.migrate(db, 'apply')



    # def test(self):
        # command.migrate(db, 'make --empty')
        # command.migrate(db, 'make --empty --custom')


    def test_2(self):
        with open('migrations/0002.py', 'w') as f:
            f.write('''
from pony.migrate import diagram_ops as op
from pony.orm import *

dependencies = ['0001_initial']

def forward(db):
    db.execute('ALTER TABLE base MODIFY COLUMN s INTEGER')
    db.execute('ALTER TABLE base DROP COLUMN classtype')
    db.execute('RENAME TABLE base TO myentity')

def final_state(db):
    class MyEntity(db.Entity):
        int_f = Required(int)
        s = Required(int)

operations = [
    op.Custom(forward=forward, final_state=final_state),
]
            ''')

        db = self.DB
        command.migrate(db, 'apply -v')

    def test_3(self):
        db_args, db_kwargs = self.db_params
        db = orm.Database(*db_args, **db_kwargs)

        class MyEntity(db.Entity):
            s = orm.Required(int)
            int_f = orm.Required(int)

        db.generate_mapping(create_tables=False)

        with orm.db_session:
            db.MyEntity(s=5, int_f=1)

