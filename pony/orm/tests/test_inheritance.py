from __future__ import absolute_import, print_function, division

import unittest

from pony.orm.core import *
from pony.orm.tests.testutils import *

class TestInheritance(unittest.TestCase):

    def test_0(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            id = PrimaryKey(int)

        self.assertTrue(Entity1._root_ is Entity1)
        self.assertEqual(Entity1._discriminator_attr_, None)
        self.assertEqual(Entity1._discriminator_, None)

    def test_1(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            id = PrimaryKey(int)
        class Entity2(Entity1):
            a = Required(int)
        class Entity3(Entity1):
            b = Required(int)
        class Entity4(Entity2, Entity3):
            c = Required(int)

        self.assertTrue(Entity1._root_ is Entity1)
        self.assertTrue(Entity2._root_ is Entity1)
        self.assertTrue(Entity3._root_ is Entity1)
        self.assertTrue(Entity4._root_ is Entity1)
        self.assertTrue(Entity1._discriminator_attr_ is Entity1.classtype)
        self.assertTrue(Entity2._discriminator_attr_ is Entity1._discriminator_attr_)
        self.assertTrue(Entity3._discriminator_attr_ is Entity1._discriminator_attr_)
        self.assertTrue(Entity4._discriminator_attr_ is Entity1._discriminator_attr_)
        self.assertEqual(Entity1._discriminator_, 'Entity1')
        self.assertEqual(Entity2._discriminator_, 'Entity2')
        self.assertEqual(Entity3._discriminator_, 'Entity3')
        self.assertEqual(Entity4._discriminator_, 'Entity4')
        
    @raises_exception(ERDiagramError, "Multiple inheritance graph must be diamond-like. "
        "Entity Entity3 inherits from Entity1 and Entity2 entities which don't have common base class.")
    def test_2(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            a = PrimaryKey(int)
        class Entity2(db.Entity):
            b = PrimaryKey(int)
        class Entity3(Entity1, Entity2):
            c = Required(int)

    @raises_exception(ERDiagramError, 'Attribute "Entity2.a" clashes with attribute "Entity3.a" in derived entity "Entity4"')
    def test_3(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            id = PrimaryKey(int)
        class Entity2(Entity1):
            a = Required(int)
        class Entity3(Entity1):
            a = Required(int)
        class Entity4(Entity2, Entity3):
            c = Required(int)

    @raises_exception(ERDiagramError, "Name 'a' hides base attribute Entity1.a")
    def test_4(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            id = PrimaryKey(int)
            a = Required(int)
        class Entity2(Entity1):
            a = Required(int)

    @raises_exception(ERDiagramError, "Primary key cannot be redefined in derived classes")
    def test_5(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            a = PrimaryKey(int)
        class Entity2(Entity1):
            b = PrimaryKey(int)

    def test_6(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            a = Discriminator(str)
            b = Required(int)
        class Entity2(Entity1):
            c = Required(int)
        
        self.assertTrue(Entity1._discriminator_attr_ is Entity1.a)
        self.assertTrue(Entity2._discriminator_attr_ is Entity1.a)

if __name__ == '__main__':
    unittest.main()
