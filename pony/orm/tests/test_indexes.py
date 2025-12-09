import sys, unittest
from decimal import Decimal
from datetime import date

from pony.orm import *
from pony.orm.tests.testutils import *
from pony.orm.tests import db_params, teardown_database

class TestIndexes(unittest.TestCase):
    def setUp(self):
        self.db = Database(**db_params)
        
    def tearDown(self):
        teardown_database(self.db)
    
    def test_1(self):
        db = self.db
        class Person(db.Entity):
            name = Required(str)
            age = Required(int)
            composite_key(name, 'age')
        db.generate_mapping(create_tables=True)

        i1, i2 = Person._indexes_
        self.assertEqual(i1.attrs, (Person.id,))
        self.assertEqual(i1.is_pk, True)
        self.assertEqual(i1.is_unique, True)
        self.assertEqual(i2.attrs, (Person.name, Person.age))
        self.assertEqual(i2.is_pk, False)
        self.assertEqual(i2.is_unique, True)

        table_name = 'Person' if db.provider.dialect == 'SQLite' and pony.__version__ < '0.9' else 'person'
        table = db.schema.tables[table_name]
        name_column = table.column_dict['name']
        age_column = table.column_dict['age']
        self.assertEqual(len(table.indexes), 2)
        db_index = table.indexes[name_column, age_column]
        self.assertEqual(db_index.is_pk, False)
        self.assertEqual(db_index.is_unique, True)

    def test_2(self):
        db = self.db
        class Person(db.Entity):
            name = Required(str)
            age = Required(int)
            composite_index(name, 'age')
        db.generate_mapping(create_tables=True)

        i1, i2 = Person._indexes_
        self.assertEqual(i1.attrs, (Person.id,))
        self.assertEqual(i1.is_pk, True)
        self.assertEqual(i1.is_unique, True)
        self.assertEqual(i2.attrs, (Person.name, Person.age))
        self.assertEqual(i2.is_pk, False)
        self.assertEqual(i2.is_unique, False)

        table_name = 'Person' if db.provider.dialect == 'SQLite' and pony.__version__ < '0.9' else 'person'
        table = db.schema.tables[table_name]
        name_column = table.column_dict['name']
        age_column = table.column_dict['age']
        self.assertEqual(len(table.indexes), 2)
        db_index = table.indexes[name_column, age_column]
        self.assertEqual(db_index.is_pk, False)
        self.assertEqual(db_index.is_unique, False)

        create_script = db.schema.generate_create_script()


        dialect = self.db.provider.dialect
        if pony.__version__ < '0.9':
            if dialect == 'SQLite':
                index_sql = 'CREATE INDEX "idx_person__name_age" ON "Person" ("name", "age")'
            else:
                index_sql = 'CREATE INDEX "idx_person__name_age" ON "person" ("name", "age")'
        elif dialect == 'MySQL' or dialect == 'SQLite':
            index_sql = 'CREATE INDEX `idx_person__name__age` ON `person` (`name`, `age`)'
        elif dialect == 'PostgreSQL':
            index_sql = 'CREATE INDEX "idx_person__name__age" ON "person" ("name", "age")'
        elif dialect == 'Oracle':
            index_sql = 'CREATE INDEX "IDX_PERSON__NAME__AGE" ON "PERSON" ("NAME", "AGE")'
        else:
            raise NotImplementedError
        self.assertIn(index_sql, create_script)

    def test_3(self):
        db = self.db
        class User(db.Entity):
            name = Required(str, unique=True)

        db.generate_mapping(create_tables=True)

        with db_session:
            u = User(id=1, name='A')

        with db_session:
            u = User[1]
            u.name = 'B'

        with db_session:
            u = User[1]
            self.assertEqual(u.name, 'B')

    def test_4(self):  # issue 321
        db = self.db
        class Person(db.Entity):
            name = Required(str)
            age = Required(int)
            composite_key(name, age)

        db.generate_mapping(create_tables=True)
        with db_session:
            p1 = Person(id=1, name='John', age=19)

        with db_session:
            p1 = Person[1]
            p1.set(name='John', age=19)
            p1.delete()

    def test_5(self):
        db = self.db

        class Table1(db.Entity):
            name = Required(str)
            table2s = Set('Table2')

        class Table2(db.Entity):
            height = Required(int)
            length = Required(int)
            table1 = Optional('Table1')
            composite_key(height, length, table1)

        db.generate_mapping(create_tables=True)

        with db_session:
            Table2(height=2, length=1)
            Table2.exists(height=2, length=1)

if __name__ == '__main__':
    unittest.main()
