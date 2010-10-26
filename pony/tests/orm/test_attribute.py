import unittest
from pony.orm3 import *
from pony.db import *
from testutils import *

class TestAttribute(unittest.TestCase):
    def setUp(self):
        self.db = Database('sqlite', ':memory:')

    @raises_exception(TypeError, "Unknown option 'another_option'")
    def test_attribute1(self):
        _diagram_ = Diagram()
        class Entity1(Entity):
            id = PrimaryKey(int, another_option=3)
        generate_mapping(self.db, check_tables = False)
        
    @raises_exception(TypeError, "Parameters 'column' and 'columns' cannot be specified simultaneously")
    def test_columns1(self):
        _diagram_ = Diagram()
        class Entity1(Entity):
            id = PrimaryKey(int)
            attr1 = Optional("Entity2", column='a', columns=['b', 'c'])
        class Entity2(Entity):
            id = PrimaryKey(int)
            attr2 = Optional(Entity1)
        generate_mapping(self.db, check_tables = False)

    def test_columns2(self):
        _diagram_ = Diagram()
        class Entity1(Entity):
            id = PrimaryKey(int, column='a')
        self.assertEqual(Entity1.id.columns, ['a'])

    def test_columns3(self):
        _diagram_ = Diagram()
        class Entity1(Entity):
            id = PrimaryKey(int, columns=['a'])
        self.assertEqual(Entity1.id.column, 'a')

    @raises_exception(TypeError, "Parameter 'columns' must not be empty list")
    def test_columns4(self):
        _diagram_ = Diagram()
        class Entity1(Entity):
            id = PrimaryKey(int, columns=[])
        generate_mapping(self.db, check_tables = False)

    @raises_exception(MappingError, "Too many columns were specified for Entity1.id")
    def test_columns5(self):
        _diagram_ = Diagram()
        class Entity1(Entity):
            id = PrimaryKey(int, columns=['a', 'b'])
        generate_mapping(self.db, check_tables = False)

    @raises_exception(TypeError, "Parameter 'columns' must be a list. Got: set(['a'])'")
    def test_columns6(self):
        _diagram_ = Diagram()
        class Entity1(Entity):
            id = PrimaryKey(int, columns=set(['a']))
        generate_mapping(self.db, check_tables = False)
        
    @raises_exception(TypeError, "Parameter 'column' must be a string. Got: 4")
    def test_columns7(self):
        _diagram_ = Diagram()
        class Entity1(Entity):
            id = PrimaryKey(int, column=4)
        generate_mapping(self.db, check_tables = False)

if __name__ == '__main__':
    unittest.main()