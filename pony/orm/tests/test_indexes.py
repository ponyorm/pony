import sys, unittest
from decimal import Decimal
from datetime import date

from pony.orm import *
from pony.orm.tests.testutils import *

class TestIndexes(unittest.TestCase):
    def test_1(self):
        db = Database('sqlite', ':memory:')
        class Person(db.Entity):
            name = Required(str)
            age = Required(int)
            composite_key(name, 'age')
        db.generate_mapping(create_tables=True)

        [ i1, i2 ] = Person._indexes_
        self.assertEqual(i1.attrs, (Person.id,))
        self.assertEqual(i1.is_pk, True)
        self.assertEqual(i1.is_unique, True)
        self.assertEqual(i2.attrs, (Person.name, Person.age))
        self.assertEqual(i2.is_pk, False)
        self.assertEqual(i2.is_unique, True)

        table = db.schema.tables['Person']
        name_column = table.column_dict['name']
        age_column = table.column_dict['age']
        self.assertEqual(len(table.indexes), 2)
        db_index = table.indexes[name_column, age_column]
        self.assertEqual(db_index.is_pk, False)
        self.assertEqual(db_index.is_unique, True)

    def test_2(self):
        db = Database('sqlite', ':memory:')
        class Person(db.Entity):
            name = Required(str)
            age = Required(int)
            composite_index(name, 'age')
        db.generate_mapping(create_tables=True)

        [ i1, i2 ] = Person._indexes_
        self.assertEqual(i1.attrs, (Person.id,))
        self.assertEqual(i1.is_pk, True)
        self.assertEqual(i1.is_unique, True)
        self.assertEqual(i2.attrs, (Person.name, Person.age))
        self.assertEqual(i2.is_pk, False)
        self.assertEqual(i2.is_unique, False)

        table = db.schema.tables['Person']
        name_column = table.column_dict['name']
        age_column = table.column_dict['age']
        self.assertEqual(len(table.indexes), 2)
        db_index = table.indexes[name_column, age_column]
        self.assertEqual(db_index.is_pk, False)
        self.assertEqual(db_index.is_unique, False)

        create_script = db.schema.generate_create_script()
        index_sql = 'CREATE INDEX "idx_person__name_age" ON "Person" ("name", "age")'
        self.assertTrue(index_sql in create_script)
        
if __name__ == '__main__':
    unittest.main()
