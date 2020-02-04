import unittest

from pony.orm import *
from pony.orm.tests.testutils import *
from pony.orm.tests import setup_database, teardown_database


class TestCascade(unittest.TestCase):
    providers = ['sqlite']  # Implement for other providers

    def tearDown(self):
        if self.db.schema is not None:
            teardown_database(self.db)

    def assert_on_delete(self, table_name, value):
        db = self.db
        if not (db.provider.dialect == 'SQLite' and pony.__version__ < '0.9'):
            table_name = table_name.lower()
        table = db.schema.tables[table_name]
        fkeys = table.foreign_keys
        self.assertEqual(1, len(fkeys))
        if pony.__version__ >= '0.9':
            self.assertEqual(fkeys[0].on_delete, value)
        elif db.provider.dialect == 'SQLite':
            self.assertIn('ON DELETE %s' % value, table.get_create_command())
        else:
            self.assertIn('ON DELETE %s' % value, list(fkeys.values())[0].get_create_command())


    def test_1(self):
        db = self.db = Database()

        class Person(self.db.Entity):
            name = Required(str)
            group = Required('Group')

        class Group(self.db.Entity):
            persons = Set(Person)

        setup_database(db)
        self.assert_on_delete('Person', 'CASCADE')

    def test_2(self):
        db = self.db = Database()

        class Person(self.db.Entity):
            name = Required(str)
            group = Required('Group')

        class Group(self.db.Entity):
            persons = Set(Person, cascade_delete=True)

        setup_database(db)
        self.assert_on_delete('Person', 'CASCADE')

    def test_3(self):
        db = self.db = Database()

        class Person(self.db.Entity):
            name = Required(str)
            group = Optional('Group')

        class Group(self.db.Entity):
            persons = Set(Person, cascade_delete=True)

        setup_database(db)
        self.assert_on_delete('Person', 'CASCADE')

    @raises_exception(TypeError, "'cascade_delete' option cannot be set for attribute Group.persons, because reverse attribute Person.group is collection")
    def test_4(self):
        db = self.db = Database()

        class Person(self.db.Entity):
            name = Required(str)
            group = Set('Group')

        class Group(self.db.Entity):
            persons = Set(Person, cascade_delete=True)

        setup_database(db)

    @raises_exception(TypeError, "'cascade_delete' option cannot be set for both sides of relationship (Person.group and Group.persons) simultaneously")
    def test_5(self):
        db = self.db = Database()

        class Person(self.db.Entity):
            name = Required(str)
            group = Set('Group', cascade_delete=True)

        class Group(self.db.Entity):
            persons = Required(Person, cascade_delete=True)

        setup_database(db)

    def test_6(self):
        db = self.db = Database()

        class Person(self.db.Entity):
            name = Required(str)
            group = Set('Group')

        class Group(self.db.Entity):
            persons = Optional(Person)

        setup_database(db)
        self.assert_on_delete('Group', 'SET NULL')


if __name__ == '__main__':
    unittest.main()
