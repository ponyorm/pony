import unittest

from testutils import *
from model1 import *

class TestQuery(unittest.TestCase):
    @raises_exception(TypeError, 'Cannot iterate over non-entity object')
    def test_query_1(self):
        select(s for s in [])
    @raises_exception(TypeError, 'Cannot iterate over non-entity object X')
    def test_query_2(self):
        X = [1, 2, 3]
        select('x for x in X')
    @db_session
    def test_first1(self):
        q = select(s for s in Student).order_by(Student.record)
        self.assertEquals(q.first(), Student[101])
    @db_session
    def test_first2(self):
        q = select((s.name, s.group) for s in Student)
        self.assertEquals(q.first(), ('Alex', Group['4145']))
    @db_session
    def test_first3(self):
        q = select(s for s in Student)
        self.assertEquals(q.first(), Student[101])