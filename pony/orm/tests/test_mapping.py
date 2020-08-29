from __future__ import absolute_import, print_function, division

import unittest

from pony.orm.core import *
from pony.orm.dbschema import DBSchemaError
from pony.orm.tests.testutils import *
from pony.orm.tests import db_params, only_for


@only_for('sqlite')
class TestColumnsMapping(unittest.TestCase):

    def setUp(self):
        self.db = Database(**db_params)

    # raise exception if mapping table by default is not found
    @raises_exception(OperationalError, 'no such table: Student')
    def test_table_check1(self):
        db = self.db
        class Student(db.Entity):
            name = PrimaryKey(str)
        sql = "drop table if exists Student;"
        with db_session:
            db.get_connection().executescript(sql)
        db.generate_mapping()

    # no exception if table was specified
    def test_table_check2(self):
        db = self.db
        class Student(db.Entity):
            name = PrimaryKey(str)
        sql = """
            drop table if exists Student;
            create table Student(
                name varchar(30)
            );
        """
        with db_session:
            db.get_connection().executescript(sql)
        db.generate_mapping()
        self.assertEqual(db.schema.tables['Student'].column_list[0].name, 'name')

    # raise exception if specified mapping table is not found
    @raises_exception(OperationalError, 'no such table: Table1')
    def test_table_check3(self):
        db = self.db
        class Student(db.Entity):
            _table_ = 'Table1'
            name = PrimaryKey(str)
        db.generate_mapping()

    # no exception if table was specified
    def test_table_check4(self):
        db = self.db
        class Student(db.Entity):
            _table_ = 'Table1'
            name = PrimaryKey(str)
        sql = """
            drop table if exists Table1;
            create table Table1(
                name varchar(30)
            );
        """
        with db_session:
            db.get_connection().executescript(sql)
        db.generate_mapping()
        self.assertEqual(db.schema.tables['Table1'].column_list[0].name, 'name')

    # 'id' field created if primary key is not defined
    @raises_exception(OperationalError, 'no such column: Student.id')
    def test_table_check5(self):
        db = self.db
        class Student(db.Entity):
            name = Required(str)
        sql = """
            drop table if exists Student;
            create table Student(
                name varchar(30)
            );
        """
        with db_session:
            db.get_connection().executescript(sql)
        db.generate_mapping()

    # 'id' field created if primary key is not defined
    def test_table_check6(self):
        db = self.db
        class Student(db.Entity):
            name = Required(str)
        sql = """
            drop table if exists Student;
            create table Student(
                id integer primary key,
                name varchar(30)
            );
        """
        with db_session:
            db.get_connection().executescript(sql)
        db.generate_mapping()
        self.assertEqual(db.schema.tables['Student'].column_list[0].name, 'id')

    @raises_exception(DBSchemaError, "Column 'name' already exists in table 'Student'")
    def test_table_check7(self):
        db = self.db
        class Student(db.Entity):
            name = Required(str, column='name')
            record = Required(str, column='name')
        sql = """
            drop table if exists Student;
            create table Student(
                id integer primary key,
                name varchar(30)
            );
        """
        with db_session:
            db.get_connection().executescript(sql)
        db.generate_mapping()

    # user can specify column name for an attribute
    def test_custom_column_name(self):
        db = self.db
        class Student(db.Entity):
            name = PrimaryKey(str, column='name1')
        sql = """
            drop table if exists Student;
            create table Student(
                name1 varchar(30)
            );
        """
        with db_session:
            db.get_connection().executescript(sql)
        db.generate_mapping()
        self.assertEqual(db.schema.tables['Student'].column_list[0].name, 'name1')

    # Required-Required raises exception
    @raises_exception(ERDiagramError,
        'At least one attribute of one-to-one relationship Entity1.attr1 - Entity2.attr2 must be optional')
    def test_relations1(self):
        db = self.db
        class Entity1(db.Entity):
            id = PrimaryKey(int)
            attr1 = Required("Entity2")
        class Entity2(db.Entity):
            id = PrimaryKey(int)
            attr2 = Required(Entity1)
        db.generate_mapping()

    # no exception Optional-Required
    def test_relations2(self):
        db = self.db
        class Entity1(db.Entity):
            id = PrimaryKey(int)
            attr1 = Optional("Entity2")
        class Entity2(db.Entity):
            id = PrimaryKey(int)
            attr2 = Required(Entity1)
        db.generate_mapping(create_tables=True)

    # no exception Optional-Required(column)
    def test_relations3(self):
        db = self.db
        class Entity1(db.Entity):
            id = PrimaryKey(int)
            attr1 = Required("Entity2", column='a')
        class Entity2(db.Entity):
            id = PrimaryKey(int)
            attr2 = Optional(Entity1)
        db.generate_mapping(create_tables=True)

    def test_relations4(self):
        db = self.db
        class Entity1(db.Entity):
            id = PrimaryKey(int)
            attr1 = Required("Entity2")
        class Entity2(db.Entity):
            id = PrimaryKey(int)
            attr2 = Optional(Entity1, column='a')
        db.generate_mapping(create_tables=True)
        self.assertEqual(Entity1.attr1.columns, ['attr1'])
        self.assertEqual(Entity2.attr2.columns, ['a'])

    # no exception Optional-Optional
    def test_relations5(self):
        db = self.db
        class Entity1(db.Entity):
            id = PrimaryKey(int)
            attr1 = Optional("Entity2")
        class Entity2(db.Entity):
            id = PrimaryKey(int)
            attr2 = Optional(Entity1)
        db.generate_mapping(create_tables=True)

    # no exception Optional-Optional(column)
    def test_relations6(self):
        db = self.db
        class Entity1(db.Entity):
            id = PrimaryKey(int)
            attr1 = Optional("Entity2", column='a')
        class Entity2(db.Entity):
            id = PrimaryKey(int)
            attr2 = Optional(Entity1)
        db.generate_mapping(create_tables=True)

    def test_relations7(self):
        db = self.db
        class Entity1(db.Entity):
            id = PrimaryKey(int)
            attr1 = Optional("Entity2", column='a')
        class Entity2(db.Entity):
            id = PrimaryKey(int)
            attr2 = Optional(Entity1, column='a1')
        db.generate_mapping(create_tables=True)
        self.assertEqual(Entity1.attr1.columns, ['a'])
        self.assertEqual(Entity2.attr2.columns, ['a1'])

    def test_columns1(self):
        db = self.db
        class Entity1(db.Entity):
            a = PrimaryKey(int)
            attr1 = Set("Entity2")
        class Entity2(db.Entity):
            id = PrimaryKey(int)
            attr2 = Optional(Entity1)
        db.generate_mapping(create_tables=True)
        column_list = db.schema.tables['Entity2'].column_list
        self.assertEqual(len(column_list), 2)
        self.assertEqual(column_list[0].name, 'id')
        self.assertEqual(column_list[1].name, 'attr2')

    def test_columns2(self):
        db = self.db
        class Entity1(db.Entity):
            a = Required(int)
            b = Required(int)
            PrimaryKey(a, b)
            attr1 = Set("Entity2")
        class Entity2(db.Entity):
            id = PrimaryKey(int)
            attr2 = Optional(Entity1)
        db.generate_mapping(create_tables=True)
        column_list = db.schema.tables['Entity2'].column_list
        self.assertEqual(len(column_list), 3)
        self.assertEqual(column_list[0].name, 'id')
        self.assertEqual(column_list[1].name, 'attr2_a')
        self.assertEqual(column_list[2].name, 'attr2_b')

    def test_columns3(self):
        db = self.db
        class Entity1(db.Entity):
            id = PrimaryKey(int)
            attr1 = Optional('Entity2')
        class Entity2(db.Entity):
            id = PrimaryKey(int)
            attr2 = Optional(Entity1)
        db.generate_mapping(create_tables=True)
        self.assertEqual(Entity1.attr1.columns, ['attr1'])
        self.assertEqual(Entity2.attr2.columns, [])

    def test_columns4(self):
        db = self.db
        class Entity2(db.Entity):
            id = PrimaryKey(int)
            attr2 = Optional('Entity1')
        class Entity1(db.Entity):
            id = PrimaryKey(int)
            attr1 = Optional(Entity2)
        db.generate_mapping(create_tables=True)
        self.assertEqual(Entity1.attr1.columns, ['attr1'])
        self.assertEqual(Entity2.attr2.columns, [])

    @raises_exception(ERDiagramError, "Mapping is not generated for entity 'E1'")
    def test_generate_mapping1(self):
        db = self.db
        class E1(db.Entity):
            a1 = Required(int)
        select(e for e in E1)

    @raises_exception(ERDiagramError, "Mapping is not generated for entity 'E1'")
    def test_generate_mapping2(self):
        db = self.db
        class E1(db.Entity):
            a1 = Required(int)
        e = E1(a1=1)

if __name__ == '__main__':
    unittest.main()
