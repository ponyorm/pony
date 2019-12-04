import unittest

from pony import orm
from pony.orm.tests import setup_database, teardown_database

db = orm.Database()


class Person(db.Entity):
    name = orm.Required(str)


class Test(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_database(db)

    @classmethod
    def tearDownClass(cls):
        teardown_database(db)

    def test_1(self):
        with orm.db_session:
            a = Person(name='John')
            a.delete()
            Person.exists(name='Mike')
