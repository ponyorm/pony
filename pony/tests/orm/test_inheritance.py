import unittest
from pony.orm import *
from testutils import *

class TestInheritance(unittest.TestCase):

    def test_inheritance1(self):
        _diagram_ = Diagram()
        class Entity1(Entity):
            id = PrimaryKey(int)
        class Entity2(Entity1):
            a = Required(int)
        class Entity3(Entity1):
            b = Required(int)
        class Entity4(Entity2, Entity3):
            c = Required(int)

    @raises_exception(DiagramError, 'With multiple inheritance of entities, inheritance graph must be diamond-like')
    def test_inheritance2(self):
        _diagram_ = Diagram()
        class Entity1(Entity):
            a = PrimaryKey(int)
        class Entity2(Entity):
            b = PrimaryKey(int)
        class Entity3(Entity1, Entity2):
            c = Required(int)

    @raises_exception(DiagramError, 'When use inheritance, base and derived entities must belong to same diagram')
    def test_inheritance3(self):
        _diagram_ = Diagram()
        class Entity1(Entity):
            id = PrimaryKey(int)
        _diagram_ = Diagram()
        class Entity2(Entity1):
            a = Required(int)

    @raises_exception(DiagramError, 'Ambiguous attribute name a')
    def test_inheritance4(self):
        _diagram_ = Diagram()
        class Entity1(Entity):
            id = PrimaryKey(int)
        class Entity2(Entity1):
            a = Required(int)
        class Entity3(Entity1):
            a = Required(int)
        class Entity4(Entity2, Entity3):
            c = Required(int)

    @raises_exception(DiagramError, "Name 'a' hides base attribute Entity1.a")
    def test_inheritance5(self):
        _diagram_ = Diagram()
        class Entity1(Entity):
            id = PrimaryKey(int)
            a = Required(int)
        class Entity2(Entity1):
            a = Required(int)            

    @raises_exception(DiagramError, "Primary key cannot be redefined in derived classes")
    def test_inheritance6(self):
        _diagram_ = Diagram()
        class Entity1(Entity):
            a = PrimaryKey(int)
        class Entity2(Entity1):
            b = PrimaryKey(int)
            
if __name__ == '__main__':
    unittest.main()