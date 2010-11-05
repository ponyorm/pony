import unittest
from pony.orm import *
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
        generate_mapping(self.db, check_tables=False)

    @raises_exception(TypeError, 'Cannot link attribute to Entity class. Must use Entity subclass instead')
    def test_attribute2(self):
        _diagram_ = Diagram()
        class Entity1(Entity):
            id = PrimaryKey(int)
            b = Required(Entity)

    @raises_exception(TypeError, 'Default value for required attribute cannot be None')
    def test_attribute3(self):
        _diagram_ = Diagram()
        class Entity1(Entity):
            id = PrimaryKey(int)
            b = Required(int, default=None)

    def test_attribute4(self):
        _diagram_ = Diagram()
        class Entity1(Entity):
            id = PrimaryKey(int)
            attr1 = Required('Entity2', reverse='attr2')
        class Entity2(Entity):
            id = PrimaryKey(int)
            attr2 = Optional(Entity1)
        self.assertEqual(Entity1.attr1.reverse, Entity2.attr2)

    def test_attribute5(self):
        _diagram_ = Diagram()
        class Entity1(Entity):
            id = PrimaryKey(int)
            attr1 = Required('Entity2')
        class Entity2(Entity):
            id = PrimaryKey(int)
            attr2 = Optional(Entity1, reverse=Entity1.attr1)
        self.assertEqual(Entity2.attr2.reverse, Entity1.attr1)

    @raises_exception(TypeError, "Value of 'reverse' option must be name of reverse attribute). Got: 123")
    def test_attribute6(self):
        _diagram_ = Diagram()
        class Entity1(Entity):
            id = PrimaryKey(int)
            attr1 = Required('Entity2', reverse=123)

    @raises_exception(TypeError, "Reverse option cannot be set for this type: <type 'str'>")
    def test_attribute7(self):
        _diagram_ = Diagram()
        class Entity1(Entity):
            id = PrimaryKey(int)
            attr1 = Required(str, reverse='attr1')

    @raises_exception(TypeError, "'Attribute' is abstract type")
    def test_attribute8(self):
        _diagram_ = Diagram()
        class Entity1(Entity):
            id = PrimaryKey(int)
            attr1 = Attribute(str)

    @raises_exception(DiagramError, "Attribute name cannot both starts and ends with underscore. Got: _attr1_")
    def test_attribute9(self):
        _diagram_ = Diagram()
        class Entity1(Entity):
            id = PrimaryKey(int)
            _attr1_ = Required(str)

    @raises_exception(DiagramError, "Duplicate use of attribute Entity1.attr1 in entity Entity2")
    def test_attribute10(self):
        _diagram_ = Diagram()
        class Entity1(Entity):
            id = PrimaryKey(int)
            attr1 = Required(str)
        class Entity2(Entity):
            id = PrimaryKey(int)
            attr2 = Entity1.attr1

    @raises_exception(DiagramError, "Invalid use of attribute Entity1.attr1 in entity Entity2")
    def test_attribute11(self):
        _diagram_ = Diagram()
        class Entity1(Entity):
            id = PrimaryKey(int)
            attr1 = Optional(str)
        class Entity2(Entity):
            id = PrimaryKey(int)
            Unique(Entity1.attr1)

    @raises_exception(DiagramError, "Cannot create primary key for Entity1 automatically because name 'id' is alredy in use")
    def test_attribute12(self):
        _diagram_ = Diagram()
        class Entity1(Entity):
            id = Optional(str)

    @raises_exception(DiagramError, "Reverse attribute for Entity1.attr1 was not found")
    def test_attribute13(self):
        _diagram_ = Diagram()
        class Entity1(Entity):
            id = PrimaryKey(int)
            attr1 = Required('Entity2')
        class Entity2(Entity):
            id = PrimaryKey(int)

    @raises_exception(DiagramError, "Reverse attribute Entity1.attr1 not found")
    def test_attribute14(self):
        _diagram_ = Diagram()
        class Entity1(Entity):
            id = PrimaryKey(int)            
        class Entity2(Entity):
            id = PrimaryKey(int)
            attr2 = Required(Entity1, reverse='attr1')

    @raises_exception(DiagramError, "Inconsistent reverse attributes Entity3.attr3 and Entity2.attr2")
    def test_attribute15(self):
        _diagram_ = Diagram()
        class Entity1(Entity):
            id = PrimaryKey(int)
            attr1 = Optional('Entity2')
        class Entity2(Entity):
            id = PrimaryKey(int)
            attr2 = Required(Entity1)
        class Entity3(Entity):
            id = PrimaryKey(int)
            attr3 = Required(Entity2, reverse='attr2')

    @raises_exception(DiagramError, "Inconsistent reverse attributes Entity3.attr3 and Entity2.attr2")
    def test_attribute16(self):
        _diagram_ = Diagram()
        class Entity1(Entity):
            id = PrimaryKey(int)
            attr1 = Optional('Entity2')
        class Entity2(Entity):
            id = PrimaryKey(int)
            attr2 = Required(Entity1)
        class Entity3(Entity):
            id = PrimaryKey(int)
            attr3 = Required(Entity2, reverse=Entity2.attr2)

##    def test_attribute17(self):
##        _diagram_ = Diagram()
##        class Phone(Entity):
##            id = PrimaryKey(int)
##            person = Required('Student')
##        class Person(Entity):
##            id = PrimaryKey(int)
##            phones = Set('Phone')
##        class Student(Person):
##            record = Required(str)

    @raises_exception(DiagramError, 'Reverse attribute for Entity2.attr2 not found')
    def test_attribute18(self):
        _diagram_ = Diagram()
        class Entity1(Entity):
            id = PrimaryKey(int)
        class Entity2(Entity):
            id = PrimaryKey(int)
            attr2 = Required('Entity1')            

    @raises_exception(DiagramError, 'Ambiguous reverse attribute for Entity2.c')
    def test_attribute19(self):
        _diagram_ = Diagram()
        class Entity1(Entity):
            id = PrimaryKey(int)
            a = Required('Entity2')
            b = Optional('Entity2')
        class Entity2(Entity):
            id = PrimaryKey(int)
            c = Set(Entity1)
            d = Set(Entity1)

    @raises_exception(DiagramError, 'Ambiguous reverse attribute for Entity2.c')
    def test_attribute20(self):
        _diagram_ = Diagram()
        class Entity1(Entity):
            id = PrimaryKey(int)
            a = Required('Entity2', reverse='c')
            b = Optional('Entity2', reverse='c')
        class Entity2(Entity):
            id = PrimaryKey(int)
            c = Set(Entity1)

    def test_attribute21(self):
        _diagram_ = Diagram()
        class Entity1(Entity):
            id = PrimaryKey(int)
            a = Required('Entity2', reverse='c')
            b = Optional('Entity2')
        class Entity2(Entity):
            id = PrimaryKey(int)
            c = Set(Entity1)
            d = Set(Entity1)

    def test_attribute22(self):
        _diagram_ = Diagram()
        class Entity1(Entity):
            id = PrimaryKey(int)
            a = Required('Entity2', reverse='c')
            b = Optional('Entity2')
        class Entity2(Entity):
            id = PrimaryKey(int)
            c = Set(Entity1, reverse='a')
            d = Set(Entity1)

##    def test_attribute23(self):
##        _diagram_ = Diagram()
##        class Entity1(Entity):
##            id = PrimaryKey(int)
##            a = Required('Entity2', reverse='c')
##            b = Optional('Entity2')
##        class Entity2(Entity):
##            id = PrimaryKey(int)
##            c = Set(Entity1, reverse='b')
##            #d = Set(Entity1)
            
    @raises_exception(TypeError, "Parameters 'column' and 'columns' cannot be specified simultaneously")
    def test_columns1(self):
        _diagram_ = Diagram()
        class Entity1(Entity):
            id = PrimaryKey(int)
            attr1 = Optional("Entity2", column='a', columns=['b', 'c'])
        class Entity2(Entity):
            id = PrimaryKey(int)
            attr2 = Optional(Entity1)
        generate_mapping(self.db, check_tables=False)

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
        generate_mapping(self.db, check_tables=False)

    @raises_exception(MappingError, "Too many columns were specified for Entity1.id")
    def test_columns5(self):
        _diagram_ = Diagram()
        class Entity1(Entity):
            id = PrimaryKey(int, columns=['a', 'b'])
        generate_mapping(self.db, check_tables=False)

    @raises_exception(TypeError, "Parameter 'columns' must be a list. Got: set(['a'])'")
    def test_columns6(self):
        _diagram_ = Diagram()
        class Entity1(Entity):
            id = PrimaryKey(int, columns=set(['a']))
        generate_mapping(self.db, check_tables=False)
        
    @raises_exception(TypeError, "Parameter 'column' must be a string. Got: 4")
    def test_columns7(self):
        _diagram_ = Diagram()
        class Entity1(Entity):
            id = PrimaryKey(int, column=4)
        generate_mapping(self.db, check_tables=False)

    def test_columns8(self):
        _diagram_ = Diagram()
        class Entity1(Entity):
            a = Required(int)
            b = Required(int)
            attr1 = Optional('Entity2')
            PrimaryKey(a, b)
        class Entity2(Entity):
            attr2 = Required(Entity1, columns=['x', 'y'])
        self.assertEqual(Entity2.attr2.column, None)
        self.assertEqual(Entity2.attr2.columns, ['x', 'y'])

    @raises_exception(MappingError, 'Invalid number of columns specified for Entity2.attr2')
    def test_columns9(self):
        _diagram_ = Diagram()
        class Entity1(Entity):
            a = Required(int)
            b = Required(int)
            attr1 = Optional('Entity2')
            PrimaryKey(a, b)
        class Entity2(Entity):
            attr2 = Required(Entity1, columns=['x', 'y', 'z'])
        generate_mapping(self.db, check_tables=False)

    @raises_exception(MappingError, 'Invalid number of columns specified for Entity2.attr2')        
    def test_columns10(self):
        _diagram_ = Diagram()
        class Entity1(Entity):
            a = Required(int)
            b = Required(int)
            attr1 = Optional('Entity2')
            PrimaryKey(a, b)
        class Entity2(Entity):
            attr2 = Required(Entity1, column='x')
        generate_mapping(self.db, check_tables=False)

    @raises_exception(TypeError, "Items of parameter 'columns' must be strings. Got: [1, 2]")        
    def test_columns11(self):
        _diagram_ = Diagram()
        class Entity1(Entity):
            a = Required(int)
            b = Required(int)
            attr1 = Optional('Entity2')
            PrimaryKey(a, b)
        class Entity2(Entity):
            attr2 = Required(Entity1, columns=[1, 2])
   
if __name__ == '__main__':
    unittest.main()