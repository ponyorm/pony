import unittest
from pony.orm3 import *

class TestDiag(unittest.TestCase):

#### An entity can declare one primary key only
    def test_primarykeys(self):
        try:
            class C1(Entity):
                field1 = PrimaryKey(int)
                field2 = PrimaryKey(int)
            self.assert_(False)
        except DiagramError, e:
            self.assert_(e.message == 'Only one primary key can be defined in each entity class')

if __name__ == '__main__':
    unittest.main()