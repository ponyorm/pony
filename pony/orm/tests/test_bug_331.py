import unittest

from pony.orm.tests import setup_database, teardown_database
from pony.orm import *

db = Database()


class Person(db.Entity):
    name = Required(str)
    group = Optional(lambda: Group)


class Group(db.Entity):
    title = PrimaryKey(str)
    persons = Set(Person)

    def __len__(self):
        return len(self.persons)


class Test(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_database(db)

    @classmethod
    def tearDownClass(cls):
        teardown_database(db)

    def test_1(self):
        with db_session:
            p1 = Person(name="Alex")
            p2 = Person(name="Brad")
            p3 = Person(name="Chad")
            p4 = Person(name="Dylan")
            p5 = Person(name="Ethan")

            g1 = Group(title="Foxes")
            g2 = Group(title="Gorillas")

            g1.persons.add(p1)
            g1.persons.add(p2)
            g1.persons.add(p3)
            g2.persons.add(p4)
            g2.persons.add(p5)
            commit()

            foxes = Group['Foxes']
            gorillas = Group['Gorillas']

            self.assertEqual(len(foxes), 3)
            self.assertEqual(len(gorillas), 2)
