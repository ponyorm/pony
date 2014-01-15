import unittest
from testutils import raises_exception
from model1 import *

class TestCollections(unittest.TestCase):

    @db_session
    def test_setwrapper_len(self):
        g = Group.get(number='4145')
        self.assert_(len(g.students) == 3)

    @db_session
    def test_setwrapper_nonzero(self):
        g = Group.get(number='4145')
        self.assert_(bool(g.students) == True)
        self.assert_(len(g.students) == 3)

    @db_session
    @raises_exception(TypeError, 'Collection attribute Group.students cannot be specified as search criteria')
    def test_get_by_collection_error(self):
        Group.get(students=[])

# replace collection items when the old ones are not fully loaded
##>>> from pony.examples.orm.students01.model import *
##>>> s1 = Student[101]
##>>> g = s1.group
##>>> g.__dict__[Group.students].is_fully_loaded
##False
##>>> s2 = Student[104]
##>>> g.students = [s2]
##>>>

# replace collection items when the old ones are not loaded
##>>> from pony.examples.orm.students01.model import *
##>>> g = Group[4145]
##>>> Group.students not in g.__dict__
##True
##>>> s2 = Student[104]
##>>> g.students = [s2]


if __name__ == '__main__':
    unittest.main()
