import unittest
from pony.orm.core import *
from pony.orm.core import Attribute
from testutils import *

class TestAttribute(unittest.TestCase):

    @raises_exception(TypeError, "Attribute Entity1.id has unknown option 'another_option'")
    def test_attribute1(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            id = PrimaryKey(int, another_option=3)
        db.generate_mapping(create_tables=True)

    @raises_exception(TypeError, 'Cannot link attribute to Entity class. Must use Entity subclass instead')
    def test_attribute2(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            id = PrimaryKey(int)
            b = Required(db.Entity)

    @raises_exception(TypeError, 'Default value for required attribute Entity1.b cannot be None')
    def test_attribute3(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            id = PrimaryKey(int)
            b = Required(int, default=None)

    def test_attribute4(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            id = PrimaryKey(int)
            attr1 = Required('Entity2', reverse='attr2')
        class Entity2(db.Entity):
            id = PrimaryKey(int)
            attr2 = Optional(Entity1)
        self.assertEqual(Entity1.attr1.reverse, Entity2.attr2)

    def test_attribute5(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            id = PrimaryKey(int)
            attr1 = Required('Entity2')
        class Entity2(db.Entity):
            id = PrimaryKey(int)
            attr2 = Optional(Entity1, reverse=Entity1.attr1)
        self.assertEqual(Entity2.attr2.reverse, Entity1.attr1)

    @raises_exception(TypeError, "Value of 'reverse' option must be name of reverse attribute). Got: 123")
    def test_attribute6(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            id = PrimaryKey(int)
            attr1 = Required('Entity2', reverse=123)

    @raises_exception(TypeError, "Reverse option cannot be set for this type: <type 'str'>")
    def test_attribute7(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            id = PrimaryKey(int)
            attr1 = Required(str, reverse='attr1')

    @raises_exception(TypeError, "'Attribute' is abstract type")
    def test_attribute8(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            id = PrimaryKey(int)
            attr1 = Attribute(str)

    @raises_exception(ERDiagramError, "Attribute name cannot both start and end with underscore. Got: _attr1_")
    def test_attribute9(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            id = PrimaryKey(int)
            _attr1_ = Required(str)

    @raises_exception(ERDiagramError, "Duplicate use of attribute Entity1.attr1 in entity Entity2")
    def test_attribute10(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            id = PrimaryKey(int)
            attr1 = Required(str)
        class Entity2(db.Entity):
            id = PrimaryKey(int)
            attr2 = Entity1.attr1

    @raises_exception(ERDiagramError, "Invalid use of attribute Entity1.a in entity Entity2")
    def test_attribute11(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            a = Required(str)
        class Entity2(db.Entity):
            b = Required(str)
            composite_key(Entity1.a, b)

    @raises_exception(ERDiagramError, "Cannot create primary key for Entity1 automatically because name 'id' is alredy in use")
    def test_attribute12(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            id = Optional(str)

    @raises_exception(ERDiagramError, "Reverse attribute for Entity1.attr1 was not found")
    def test_attribute13(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            id = PrimaryKey(int)
            attr1 = Required('Entity2')
        class Entity2(db.Entity):
            id = PrimaryKey(int)

    @raises_exception(ERDiagramError, "Reverse attribute Entity1.attr1 not found")
    def test_attribute14(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            id = PrimaryKey(int)
        class Entity2(db.Entity):
            id = PrimaryKey(int)
            attr2 = Required(Entity1, reverse='attr1')

    @raises_exception(ERDiagramError, "Inconsistent reverse attributes Entity3.attr3 and Entity2.attr2")
    def test_attribute15(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            id = PrimaryKey(int)
            attr1 = Optional('Entity2')
        class Entity2(db.Entity):
            id = PrimaryKey(int)
            attr2 = Required(Entity1)
        class Entity3(db.Entity):
            id = PrimaryKey(int)
            attr3 = Required(Entity2, reverse='attr2')

    @raises_exception(ERDiagramError, "Inconsistent reverse attributes Entity3.attr3 and Entity2.attr2")
    def test_attribute16(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            id = PrimaryKey(int)
            attr1 = Optional('Entity2')
        class Entity2(db.Entity):
            id = PrimaryKey(int)
            attr2 = Required(Entity1)
        class Entity3(db.Entity):
            id = PrimaryKey(int)
            attr3 = Required(Entity2, reverse=Entity2.attr2)

    @raises_exception(ERDiagramError, 'Reverse attribute for Entity2.attr2 not found')
    def test_attribute18(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            id = PrimaryKey(int)
        class Entity2(db.Entity):
            id = PrimaryKey(int)
            attr2 = Required('Entity1')

    @raises_exception(ERDiagramError, 'Ambiguous reverse attribute for Entity2.c')
    def test_attribute19(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            id = PrimaryKey(int)
            a = Required('Entity2')
            b = Optional('Entity2')
        class Entity2(db.Entity):
            id = PrimaryKey(int)
            c = Set(Entity1)
            d = Set(Entity1)

    @raises_exception(ERDiagramError, 'Ambiguous reverse attribute for Entity2.c')
    def test_attribute20(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            id = PrimaryKey(int)
            a = Required('Entity2', reverse='c')
            b = Optional('Entity2', reverse='c')
        class Entity2(db.Entity):
            id = PrimaryKey(int)
            c = Set(Entity1)

    def test_attribute21(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            id = PrimaryKey(int)
            a = Required('Entity2', reverse='c')
            b = Optional('Entity2')
        class Entity2(db.Entity):
            id = PrimaryKey(int)
            c = Set(Entity1)
            d = Set(Entity1)

    def test_attribute22(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            id = PrimaryKey(int)
            a = Required('Entity2', reverse='c')
            b = Optional('Entity2')
        class Entity2(db.Entity):
            id = PrimaryKey(int)
            c = Set(Entity1, reverse='a')
            d = Set(Entity1)

    @raises_exception(TypeError, "Parameters 'column' and 'columns' cannot be specified simultaneously")
    def test_columns1(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            id = PrimaryKey(int)
            attr1 = Optional("Entity2", column='a', columns=['b', 'c'])
        class Entity2(db.Entity):
            id = PrimaryKey(int)
            attr2 = Optional(Entity1)
        db.generate_mapping(create_tables=True)

    def test_columns2(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            id = PrimaryKey(int, column='a')
        self.assertEqual(Entity1.id.columns, ['a'])

    def test_columns3(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            id = PrimaryKey(int, columns=['a'])
        self.assertEqual(Entity1.id.column, 'a')

    @raises_exception(MappingError, "Too many columns were specified for Entity1.id")
    def test_columns5(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            id = PrimaryKey(int, columns=['a', 'b'])
        db.generate_mapping(create_tables=True)

    @raises_exception(TypeError, "Parameter 'columns' must be a list. Got: set(['a'])'")
    def test_columns6(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            id = PrimaryKey(int, columns=set(['a']))
        db.generate_mapping(create_tables=True)

    @raises_exception(TypeError, "Parameter 'column' must be a string. Got: 4")
    def test_columns7(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            id = PrimaryKey(int, column=4)
        db.generate_mapping(create_tables=True)

    def test_columns8(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            a = Required(int)
            b = Required(int)
            attr1 = Optional('Entity2')
            PrimaryKey(a, b)
        class Entity2(db.Entity):
            attr2 = Required(Entity1, columns=['x', 'y'])
        self.assertEqual(Entity2.attr2.column, None)
        self.assertEqual(Entity2.attr2.columns, ['x', 'y'])

    @raises_exception(MappingError, 'Invalid number of columns specified for Entity2.attr2')
    def test_columns9(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            a = Required(int)
            b = Required(int)
            attr1 = Optional('Entity2')
            PrimaryKey(a, b)
        class Entity2(db.Entity):
            attr2 = Required(Entity1, columns=['x', 'y', 'z'])
        db.generate_mapping(create_tables=True)

    @raises_exception(MappingError, 'Invalid number of columns specified for Entity2.attr2')
    def test_columns10(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            a = Required(int)
            b = Required(int)
            attr1 = Optional('Entity2')
            PrimaryKey(a, b)
        class Entity2(db.Entity):
            attr2 = Required(Entity1, column='x')
        db.generate_mapping(create_tables=True)

    @raises_exception(TypeError, "Items of parameter 'columns' must be strings. Got: [1, 2]")
    def test_columns11(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            a = Required(int)
            b = Required(int)
            attr1 = Optional('Entity2')
            PrimaryKey(a, b)
        class Entity2(db.Entity):
            attr2 = Required(Entity1, columns=[1, 2])

    def test_columns12(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            attr1 = Set('Entity1', reverse='attr1', column='column1', reverse_column='column2', reverse_columns=['column2'])
        db.generate_mapping(create_tables=True)

    @raises_exception(TypeError, "Parameters 'reverse_column' and 'reverse_columns' cannot be specified simultaneously")
    def test_columns13(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            attr1 = Set('Entity1', reverse='attr1', column='column1', reverse_column='column2', reverse_columns=['column3'])
        db.generate_mapping(create_tables=True)

    @raises_exception(TypeError, "Parameter 'reverse_column' must be a string. Got: 5")
    def test_columns14(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            attr1 = Set('Entity1', reverse='attr1', column='column1', reverse_column=5)
        db.generate_mapping(create_tables=True)

    @raises_exception(TypeError, "Parameter 'reverse_columns' must be a list. Got: 'column3'")
    def test_columns15(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            attr1 = Set('Entity1', reverse='attr1', column='column1', reverse_columns='column3')
        db.generate_mapping(create_tables=True)

    @raises_exception(TypeError, "Parameter 'reverse_columns' must be a list of strings. Got: [5]")
    def test_columns16(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            attr1 = Set('Entity1', reverse='attr1', column='column1', reverse_columns=[5])
        db.generate_mapping(create_tables=True)

    def test_columns17(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            attr1 = Set('Entity1', reverse='attr1', column='column1', reverse_columns=['column2'])
        db.generate_mapping(create_tables=True)

    def test_columns18(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            attr1 = Set('Entity1', reverse='attr1', table='T1')
        db.generate_mapping(create_tables=True)

    @raises_exception(TypeError, "Parameter 'table' must be a string. Got: 5")
    def test_columns19(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            attr1 = Set('Entity1', reverse='attr1', table=5)
        db.generate_mapping(create_tables=True)

    @raises_exception(TypeError, "Each part of table name must be a string. Got: 1")
    def test_columns20(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            attr1 = Set('Entity1', reverse='attr1', table=[1, 'T1'])
        db.generate_mapping(create_tables=True)

    def test_nullable1(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            a = Optional(unicode, unique=True)
        db.generate_mapping(create_tables=True)
        self.assertEqual(Entity1.a.nullable, True)

    def test_nullable2(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            a = Optional(unicode, unique=True)
        db.generate_mapping(create_tables=True)
        with db_session:
            Entity1()
            commit()
            Entity1()
            commit()
        self.assert_(True)

if __name__ == '__main__':
    unittest.main()
