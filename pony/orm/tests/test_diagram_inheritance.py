import unittest
from pony.orm.core import *
from testutils import *

class TestInheritance(unittest.TestCase):

    def test_inheritance1(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            id = PrimaryKey(int)
        class Entity2(Entity1):
            a = Required(int)
        class Entity3(Entity1):
            b = Required(int)
        class Entity4(Entity2, Entity3):
            c = Required(int)

    @raises_exception(ERDiagramError, 'With multiple inheritance of entities, inheritance graph must be diamond-like')
    def test_inheritance2(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            a = PrimaryKey(int)
        class Entity2(db.Entity):
            b = PrimaryKey(int)
        class Entity3(Entity1, Entity2):
            c = Required(int)

    def test_inheritance3(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            id = PrimaryKey(int)
        db = Database('sqlite', ':memory:')
        class Entity2(Entity1):
            a = Required(int)
        self.assert_(True)

    @raises_exception(ERDiagramError, 'Attribute "Entity2.a" clashes with attribute "Entity3.a" in derived entity "Entity4"')
    def test_inheritance4(self):
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
    def test_inheritance5(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            id = PrimaryKey(int)
            a = Required(int)
        class Entity2(Entity1):
            a = Required(int)

    @raises_exception(ERDiagramError, "Primary key cannot be redefined in derived classes")
    def test_inheritance6(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            a = PrimaryKey(int)
        class Entity2(Entity1):
            b = PrimaryKey(int)

if __name__ == '__main__':
    unittest.main()
