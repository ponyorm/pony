import unittest
from pony.orm3 import *
from pony.db import *
from testutils import *

class TestColumnsMapping(unittest.TestCase):
    def setUp(self):
        self.db = Database('sqlite', ':memory:')

    # raise exception if mapping table by default is not found
    @raises_exception(OperationalError, 'no such table: Student')
    def test_table_check1(self):
        _diagram_ = Diagram()
        class Student(Entity):
            name = PrimaryKey(str)
        sql = "drop table if exists Student;"
        self.db.get_connection().executescript(sql)
        generate_mapping(self.db, check_tables = True)

    # no exception if table was specified
    def test_table_check2(self):
        _diagram_ = Diagram()
        class Student(Entity):
            name = PrimaryKey(str)
        sql = """
            drop table if exists Student;
            create table Student(
                name varchar(30)
            );
        """
        self.db.get_connection().executescript(sql)
        generate_mapping(self.db, check_tables = True)
        self.assertEqual(_diagram_.mapping.tables['Student'].column_list[0].name, 'name')

    # raise exception if specified mapping table is not found
    @raises_exception(OperationalError, 'no such table: Table1')
    def test_table_check3(self):
        _diagram_ = Diagram()
        class Student(Entity):
            _table_ = 'Table1'
            name = PrimaryKey(str)
        generate_mapping(self.db, check_tables = True)

    # no exception if table was specified
    def test_table_check4(self):
        _diagram_ = Diagram()
        class Student(Entity):
            _table_ = 'Table1'
            name = PrimaryKey(str)
        sql = """
            drop table if exists Table1;
            create table Table1(
                name varchar(30)
            );
        """
        self.db.get_connection().executescript(sql)
        generate_mapping(self.db, check_tables = True)
        self.assertEqual(_diagram_.mapping.tables['Table1'].column_list[0].name, 'name')

    # 'id' field created if primary key is not defined
    @raises_exception(OperationalError, 'no such column: Student.id')
    def test_table_check5(self):
        _diagram_ = Diagram()
        class Student(Entity):
            name = Required(str)
        sql = """
            drop table if exists Student;
            create table Student(
                name varchar(30)
            );
        """
        self.db.get_connection().executescript(sql)
        generate_mapping(self.db, check_tables = True)

    # 'id' field created if primary key is not defined
    def test_table_check6(self):
        _diagram_ = Diagram()
        class Student(Entity):
            name = Required(str)
        sql = """
            drop table if exists Student;
            create table Student(
                id integer primary key,
                name varchar(30)
            );
        """
        self.db.get_connection().executescript(sql)
        generate_mapping(self.db, check_tables = True)
        self.assertEqual(_diagram_.mapping.tables['Student'].column_list[0].name, 'id')

    # user can specify column name for an attribute
    def test_custom_column_name(self):
        _diagram_ = Diagram()
        class Student(Entity):
            name = PrimaryKey(str, column = 'name1')
        sql = """
            drop table if exists Student;
            create table Student(
                name1 varchar(30)
            );
        """
        self.db.get_connection().executescript(sql)
        generate_mapping(self.db, check_tables = True)
        self.assertEqual(_diagram_.mapping.tables['Student'].column_list[0].name, 'name1')

    # Required-Required raises exception
    @raises_exception(DiagramError,
        'At least one attribute of one-to-one relationship Entity2.attr2 - Entity1.attr1 must be optional')
    def test_relations1(self):
        _diagram_ = Diagram()
        class Entity1(Entity):
            id = PrimaryKey(int)
            attr1 = Required("Entity2")
        class Entity2(Entity):
            id = PrimaryKey(int)
            attr2 = Required(Entity1)
        generate_mapping(self.db, check_tables = False)

    # no exception Optional-Required
    def test_relations2(self):
        _diagram_ = Diagram()
        class Entity1(Entity):
            id = PrimaryKey(int)
            attr1 = Optional("Entity2")
        class Entity2(Entity):
            id = PrimaryKey(int)
            attr2 = Required(Entity1)
        generate_mapping(self.db, check_tables = False)
        
    # no exception Optional-Required(column)
    def test_relations3(self):
        _diagram_ = Diagram()
        class Entity1(Entity):
            id = PrimaryKey(int)
            attr1 = Required("Entity2", column='a')
        class Entity2(Entity):
            id = PrimaryKey(int)
            attr2 = Optional(Entity1)
        generate_mapping(self.db, check_tables = False)
        
    # exception Optional(column)-Required
    @raises_exception(MappingError,
                      "Parameter 'column' cannot be specified for attribute Entity2.attr2. "
                      "Specify this parameter for reverse attribute Entity1.attr1 or make Entity1.attr1 optional")
    def test_relations4(self):
        _diagram_ = Diagram()
        class Entity1(Entity):
            id = PrimaryKey(int)
            attr1 = Required("Entity2")
        class Entity2(Entity):
            id = PrimaryKey(int)
            attr2 = Optional(Entity1, column='a')
        generate_mapping(self.db, check_tables = False)
        
    # no exception Optional-Optional
    def test_relations5(self):
        _diagram_ = Diagram()
        class Entity1(Entity):
            id = PrimaryKey(int)
            attr1 = Optional("Entity2")
        class Entity2(Entity):
            id = PrimaryKey(int)
            attr2 = Optional(Entity1)
        generate_mapping(self.db, check_tables = False)
        
    # no exception Optional-Optional(column)
    def test_relations6(self):
        _diagram_ = Diagram()
        class Entity1(Entity):
            id = PrimaryKey(int)
            attr1 = Optional("Entity2", column='a')
        class Entity2(Entity):
            id = PrimaryKey(int)
            attr2 = Optional(Entity1)
        generate_mapping(self.db, check_tables = False)
        
    # exception Optional(column)-Optional(column)
    @raises_exception(MappingError, "Both attributes Entity1.attr1 and Entity2.attr2 have parameter 'column'. "
                                    "Parameter 'column' cannot be specified at both sides of one-to-one relation")
    def test_relations7(self):
        _diagram_ = Diagram()
        class Entity1(Entity):
            id = PrimaryKey(int)
            attr1 = Optional("Entity2", column='a')
        class Entity2(Entity):
            id = PrimaryKey(int)
            attr2 = Optional(Entity1, column='a1')
        generate_mapping(self.db, check_tables = False)
                
if __name__ == '__main__':
    unittest.main()
