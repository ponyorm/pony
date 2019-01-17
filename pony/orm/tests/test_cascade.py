import unittest

from pony.orm import *
from pony.orm.tests.testutils import *

class TestCascade(unittest.TestCase):

    def test_1(self):
        db = self.db = Database('sqlite', ':memory:')

        class Person(self.db.Entity):
            name = Required(str)
            group = Required('Group')

        class Group(self.db.Entity):
            persons = Set(Person)

        db.generate_mapping(create_tables=True)

        self.assertTrue('ON DELETE CASCADE' in self.db.schema.tables['Person'].get_create_command())

    def test_2(self):
        db = self.db = Database('sqlite', ':memory:')

        class Person(self.db.Entity):
            name = Required(str)
            group = Required('Group')

        class Group(self.db.Entity):
            persons = Set(Person, cascade_delete=True)

        db.generate_mapping(create_tables=True)

        self.assertTrue('ON DELETE CASCADE' in self.db.schema.tables['Person'].get_create_command())


    def test_3(self):
        db = self.db = Database('sqlite', ':memory:')

        class Person(self.db.Entity):
            name = Required(str)
            group = Optional('Group')

        class Group(self.db.Entity):
            persons = Set(Person, cascade_delete=True)

        db.generate_mapping(create_tables=True)

        self.assertTrue('ON DELETE CASCADE' in self.db.schema.tables['Person'].get_create_command())

    @raises_exception(TypeError, "'cascade_delete' option cannot be set for attribute Group.persons, because reverse attribute Person.group is collection")
    def test_4(self):
        db = self.db = Database('sqlite', ':memory:')

        class Person(self.db.Entity):
            name = Required(str)
            group = Set('Group')

        class Group(self.db.Entity):
            persons = Set(Person, cascade_delete=True)

        db.generate_mapping(create_tables=True)

    @raises_exception(TypeError, "'cascade_delete' option cannot be set for both sides of relationship (Person.group and Group.persons) simultaneously")
    def test_5(self):
        db = self.db = Database('sqlite', ':memory:')

        class Person(self.db.Entity):
            name = Required(str)
            group = Set('Group', cascade_delete=True)

        class Group(self.db.Entity):
            persons = Required(Person, cascade_delete=True)

        db.generate_mapping(create_tables=True)

    def test_6(self):
        db = self.db = Database('sqlite', ':memory:')

        class Person(self.db.Entity):
            name = Required(str)
            group = Set('Group')

        class Group(self.db.Entity):
            persons = Optional(Person)

        db.generate_mapping(create_tables=True)

        self.assertTrue('ON DELETE SET NULL' in self.db.schema.tables['Group'].get_create_command())

if __name__ == '__main__':
    unittest.main()
