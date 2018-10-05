import unittest

from pony import orm

class Test(unittest.TestCase):
    def test_1(self):
        db = orm.Database('sqlite', ':memory:')

        class Person(db.Entity):
            name = orm.Required(str)

        db.generate_mapping(create_tables=True)

        with orm.db_session:
            a = Person(name='John')
            a.delete()
            Person.exists(name='Mike')
