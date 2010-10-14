import unittest
from pony.orm3 import *
from pony.db import Database
from model1 import *

class TestCollections(unittest.TestCase):
    def setUp(self):
        local.trans = Transaction()

    def test_setwrapper_len(self):
        g = Group.find_one(4145)
        self.assert_(len(g.students) == 3)

    def test_setwrapper_nonzero(self):
        g = Group.find_one(4145)
        self.assert_(bool(g.students) == True)
        self.assert_(len(g.students) == 3)

    def test_many2many(self):
        subjects = Subject.find_all()



if __name__ == '__main__':
    unittest.main()    