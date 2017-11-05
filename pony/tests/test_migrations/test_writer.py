import textwrap, unittest
from datetime import datetime

from pony import orm
from pony.utils import cached_property

from pony.migrate.diagram_ops import AddEntity
from pony.migrate.serializer import serialize
from pony.migrate.writer import MigrationWriter


class TestOperation(unittest.TestCase):

    def test(self):
        obj = AddEntity('MyE', ['MyParentE'],
                        {'int': orm.Required(int), 'str': orm.Optional(str)})
        imports = set()
        s = serialize(obj, imports)
        self.assertEqual(s,
            "op.AddEntity('MyE', ['MyParentE'], "
            "{'int': orm.Required(int), 'str': orm.Optional(str)})"
        )
        self.assertIn('from pony.migrate import diagram_ops as op', imports)


class TestWriter(unittest.TestCase):

    @cached_property
    def db_next(self):
        db_args, db_kwargs = self.db_params
        db = orm.Database(*db_args, **db_kwargs)

        class Data(db.Entity):
            number = orm.Required(int)

        return db

    @cached_property
    def db_prev(self):
        db_args, db_kwargs = self.db_params
        db = orm.Database(*db_args, **db_kwargs)

        class Data(db.Entity):
            number = orm.Optional(int)
            created = orm.Required(datetime)
            int = orm.Required(int)

        return db

    def test(self):
        self.writer = MigrationWriter([], self.db_prev, self.db_next)
        s = self.writer.as_string()
        dic = {}
        exec(s, dic)
        self.assertTrue(dic['operations'])


class TestWriterRelated(unittest.TestCase):

    @cached_property
    def db_next(self):
        db_args, db_kwargs = self.db_params
        db = orm.Database(*db_args, **db_kwargs)

        class Post(db.Entity):
            tags = orm.Set('Tag')

        class Tag(db.Entity):
            posts = orm.Set('Post')

        db.generate_mapping(create_tables=False, check_tables=False)
        return db

    @cached_property
    def db_prev(self):
        db_args, db_kwargs = self.db_params
        db = orm.Database(*db_args, **db_kwargs)

        class Post(db.Entity):
            tags = orm.Set('Tag')

        class Tag(db.Entity):
            posts = orm.Required('Post')

        db.generate_mapping(create_tables=False, check_tables=False)
        return db

    def test(self):
        self.writer = MigrationWriter([], self.db_prev, self.db_next)
        s = self.writer.as_string()
        self.assertTrue(
            "from pony.migrate import diagram_ops as op" in s
        )
        dic = {}
        exec(s, dic)
        self.assertEqual(
            str(dic['operations']),
            "[op.ModifyRelation('Post', 'tags', 'tags', orm.Set('Tag'), 'posts', orm.Set('Post'))]"
        )

class TestRemove(unittest.TestCase):

    @cached_property
    def db_prev(self):
        db_args, db_kwargs = self.db_params
        db = orm.Database(*db_args, **db_kwargs)

        class Post(db.Entity):
            tags = orm.Set('Tag')

        class Tag(db.Entity):
            posts = orm.Set('Post')

        db.generate_mapping(create_tables=False, check_tables=False)
        return db

    @cached_property
    def db_next(self):
        db_args, db_kwargs = self.db_params
        db = orm.Database(*db_args, **db_kwargs)

        class Post(db.Entity):
            pass

        class Tag(db.Entity):
            pass

        db.generate_mapping(create_tables=False, check_tables=False)
        return db

    def test(self):
        self.writer = MigrationWriter([], self.db_prev, self.db_next)
        s = self.writer.as_string()
        dic = {}
        exec(s, dic)
        self.assertEqual(
            str(dic['operations']),
            "[op.RemoveRelation('Post', 'tags')]"
        )

class TestAdd(unittest.TestCase):

    @cached_property
    def db_next(self):
        db_args, db_kwargs = self.db_params
        db = orm.Database(*db_args, **db_kwargs)

        class Post(db.Entity):
            tags = orm.Set('Tag')

        class Tag(db.Entity):
            posts = orm.Set('Post')

        db.generate_mapping(create_tables=False, check_tables=False)
        return db

    @cached_property
    def db_prev(self):
        db_args, db_kwargs = self.db_params
        db = orm.Database(*db_args, **db_kwargs)

        class Post(db.Entity):
            pass

        class Tag(db.Entity):
            pass

        db.generate_mapping(create_tables=False, check_tables=False)
        return db

    def test_ops(self):
        self.writer = MigrationWriter([], self.db_prev, self.db_next)
        s = self.writer.as_string()
        dic = {}
        exec(s, dic)
        self.assertEqual(
            str(dic['operations']),
            "[op.AddRelation('Post', 'tags', orm.Set('Tag'), 'posts', orm.Set('Post'))]",
        )

class TestInitial(unittest.TestCase):

    @cached_property
    def db_next(self):
        db_args, db_kwargs = self.db_params
        db = orm.orm.Database(*db_args, **db_kwargs)

        class Post(db.Entity):
            tags = orm.Set('Tag')

        class Tag(db.Entity):
            posts = orm.Set('Post')

        db.generate_mapping(create_tables=False, check_tables=False)
        return db

    db_prev = None

    def test_initial(self):
        self.writer = MigrationWriter([], self.db_prev, self.db_next)
        s = self.writer.as_string()

        part = textwrap.dedent('''\
        def define_entities(db):
            class Post(db.Entity):
                tags = orm.Set('Tag')


            class Tag(db.Entity):
                posts = orm.Set('Post')''')

        self.assertTrue(part in s)