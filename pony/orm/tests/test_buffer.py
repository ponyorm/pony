import unittest

from pony import orm
from pony.py23compat import buffer

db = orm.Database('sqlite', ':memory:')

class Foo(db.Entity):
    id = orm.PrimaryKey(int)
    b = orm.Optional(orm.buffer)

class Bar(db.Entity):
    b = orm.PrimaryKey(orm.buffer)

class Baz(db.Entity):
    id = orm.PrimaryKey(int)
    b = orm.Optional(orm.buffer, unique=True)

db.generate_mapping(create_tables=True)

buf = orm.buffer(b'123')

with orm.db_session:
    Foo(id=1, b=buf)
    Bar(b=buf)
    Baz(id=1, b=buf)


class Test(unittest.TestCase):
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
