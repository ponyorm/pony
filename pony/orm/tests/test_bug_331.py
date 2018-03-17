import unittest

from pony import orm

class Test(unittest.TestCase):
    def test_1(self):
        db = orm.Database('sqlite', ':memory:')

        class Person(db.Entity):
            name = orm.Required(str)
            group = orm.Optional(lambda: Group)

        class Group(db.Entity):
            title = orm.PrimaryKey(str)
            persons = orm.Set(Person)

            def __len__(self):
                return len(self.persons)

        db.generate_mapping(create_tables=True)

        with orm.db_session:
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
            orm.commit()

            foxes = Group['Foxes']
            gorillas = Group['Gorillas']

            self.assertEqual(len(foxes), 3)
            self.assertEqual(len(gorillas), 2)
