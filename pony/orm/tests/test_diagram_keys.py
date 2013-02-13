import unittest
from pony.orm.core import *
from testutils import *

class TestKeys(unittest.TestCase):

    def test_keys1(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            a = PrimaryKey(int)
            b = Required(str)
        self.assertEqual(Entity1._pk_attrs_, (Entity1.a,))
        self.assertEqual(Entity1._pk_is_composite_, False)
        self.assertEqual(Entity1._pk_, Entity1.a)
        self.assertEqual(Entity1._keys_, [])
        self.assertEqual(Entity1._simple_keys_, [])
        self.assertEqual(Entity1._composite_keys_, [])
        
    def test_keys2(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            a = Required(int)
            b = Required(str)
            PrimaryKey(a, b)
        self.assertEqual(Entity1._pk_attrs_, (Entity1.a, Entity1.b))
        self.assertEqual(Entity1._pk_is_composite_, True)
        self.assertEqual(Entity1._pk_, (Entity1.a, Entity1.b))
        self.assertEqual(Entity1._keys_, [])
        self.assertEqual(Entity1._simple_keys_, [])
        self.assertEqual(Entity1._composite_keys_, [])

    @raises_exception(ERDiagramError, 'Only one primary key can be defined in each entity class')
    def test_keys3(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            a = PrimaryKey(int)
            b = PrimaryKey(int)

    @raises_exception(ERDiagramError, 'Only one primary key can be defined in each entity class')
    def test_keys4(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            a = PrimaryKey(int)
            b = Required(int)
            c = Required(int)
            PrimaryKey(b, c)

    def test_unique1(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            a = PrimaryKey(int)
            b = Unique(int)
        self.assertEqual(Entity1._keys_, [(Entity1.b,)])
        self.assertEqual(Entity1._simple_keys_, [Entity1.b])
        self.assertEqual(Entity1._composite_keys_, [])

    def test_unique2(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            a = PrimaryKey(int)
            b = Optional(int)
            Unique(b)
        self.assertEqual(Entity1._keys_, [(Entity1.b,)])
        self.assertEqual(Entity1._simple_keys_, [Entity1.b])
        self.assertEqual(Entity1._composite_keys_, [])

    def test_unique2_1(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            a = PrimaryKey(int)
            b = Optional(int)
            c = Required(int)
            Unique(b, c)
        self.assertEqual(Entity1._keys_, [(Entity1.b, Entity1.c)])
        self.assertEqual(Entity1._simple_keys_, [])
        self.assertEqual(Entity1._composite_keys_, [(Entity1.b, Entity1.c)])

    @raises_exception(TypeError, 'Invalid count of positional arguments')
    def test_unique3(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            a = PrimaryKey(int)
            b = Unique()

    @raises_exception(TypeError, 'Incorrect type of attribute: 123')
    def test_unique4(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            a = PrimaryKey(int)
            b = Unique(123)

    @raises_exception(TypeError, 'Invalid arguments')
    def test_unique5(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            a = PrimaryKey(int)
            b = Unique(int, a)

    @raises_exception(TypeError, 'Invalid arguments')
    def test_unique6(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            a = PrimaryKey(int)
            b = Unique(a, column='x')

    @raises_exception(TypeError, 'Set attribute Entity1.b cannot be part of unique index')
    def test_unique7(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            a = PrimaryKey(int)
            b = Set('Entity2')
            Unique(b)

    @raises_exception(TypeError, 'Optional attribute Entity1.b cannot be part of primary key')
    def test_unique8(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            a = Required(int)
            b = Optional(int)
            PrimaryKey(a, b)

    @raises_exception(TypeError, 'Invalid declaration: just write attrname = Unique(int)')
    def test_unique9(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            a = PrimaryKey(int)
            b = Required(int)
            Unique(b)

        
if __name__ == '__main__':
    unittest.main()