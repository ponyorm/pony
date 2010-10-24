import unittest
from pony.orm3 import *
from pony.db import *

def raises_exception(exc_class, msg):
    def decorator(func):
        def wrapper(self, *args, **keyargs):
            try:
                func(self, *args, **keyargs)
                self.assert_(False, "expected exception %s wasn't raised" % exc_class.__name__)
            except exc_class, e:
                self.assertEqual(e.message, msg, "incorrect exception message. expected '%s', got '%s'"
                % (msg, e.message))
        wrapper.__name__ = func.__name__
        return wrapper
    return decorator

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

    #
##    def test_(self):
##        _diagram_ = Diagram()
##        class Student(Entity):
##            name = PrimaryKey(str, column = 'name1')
##        sql = """
##            drop table if exists Student;
##            create table Student(
##                name1 varchar(30)
##            );
##        """
##        self.db.get_connection().executescript(sql)
##        generate_mapping(self.db, check_tables = True)
##        self.assertEqual(_diagram_.mapping.tables['Student'].column_list[0].name, 'name1')

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

if __name__ == '__main__':
    unittest.main()

##1. test number of columns specified for attribute matches attributes: MappingError in generate mapping
##2. All Exceptions
##3. If the key is not composte the column name should include name of attribute only
##Group
## number = Required(str)
## kaf = Required(str)
## PrimaryKey(number, kaf)
##Student
## group = Required(Group)
## name = Required(str)
##
##Student's columns: group_number, group_kaf, name
## as opposite to:
##Group
## number = Required(str)
## PrimaryKey(number, kaf)
##Student
## group = Required(Group)
## name = Required(str)
##
##Student's columns: group, name
##
##4. use _diagram_.mapping.tables.column_list