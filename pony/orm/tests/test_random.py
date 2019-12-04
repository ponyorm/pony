import unittest

from pony.orm import *
from pony.orm.tests.testutils import *
from pony.orm.tests import setup_database, teardown_database

db = Database()


class Person(db.Entity):
    id = PrimaryKey(int)
    name = Required(unicode)


class TestRandom(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_database(db)
        with db_session:
            Person(id=1, name='John')
            Person(id=2, name='Mary')
            Person(id=3, name='Bob')
            Person(id=4, name='Mike')
            Person(id=5, name='Ann')

    @classmethod
    def tearDownClass(cls):
        teardown_database(db)

    @db_session
    def test_1(self):
        persons = Person.select().random(2)
        self.assertEqual(len(persons), 2)
        p1, p2 = persons
        self.assertNotEqual(p1.id, p2.id)
        self.assertTrue(p1.id in range(1, 6))
        self.assertTrue(p2.id in range(1, 6))

    @db_session
    def test_2(self):
        persons = Person.select_random(2)
        self.assertEqual(len(persons), 2)
        p1, p2 = persons
        self.assertNotEqual(p1.id, p2.id)
        self.assertTrue(p1.id in range(1, 6))
        self.assertTrue(p2.id in range(1, 6))

if __name__ == '__main__':
    unittest.main()
