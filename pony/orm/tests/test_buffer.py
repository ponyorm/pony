import unittest

from pony import orm
from pony.orm.tests import setup_database, teardown_database

db = orm.Database()


class Foo(db.Entity):
    id = orm.PrimaryKey(int)
    b = orm.Optional(orm.buffer)


class Bar(db.Entity):
    b = orm.PrimaryKey(orm.buffer)


class Baz(db.Entity):
    id = orm.PrimaryKey(int)
    b = orm.Optional(orm.buffer, unique=True)


buf = orm.buffer(b'123')

class Test(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_database(db)
        with orm.db_session:
            Foo(id=1, b=buf)
            Bar(b=buf)
            Baz(id=1, b=buf)

    @classmethod
    def tearDownClass(cls):
        teardown_database(db)

    def test_1(self):  # Bug #355
        with orm.db_session:
            Bar[buf]

    def test_2(self):  # Regression after #355 fix
        with orm.db_session:
            result = orm.select(bar.b for bar in Foo)[:]
            self.assertEqual(result, [buf])

    def test_3(self):  # Bug #390
        with orm.db_session:
            Baz.get(b=buf)
