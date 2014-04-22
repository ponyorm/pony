import unittest
from model1 import *

class TestFilter(unittest.TestCase):
    def setUp(self):
        rollback()
        db_session.__enter__()
    def tearDown(self):
        rollback()
        db_session.__exit__()
    def test_filter_1(self):
        q = select(s for s in Student)
        result = set(q.filter(scholarship=0))
        self.assertEqual(result, set([Student[101], Student[103]]))
    def test_filter_2(self):
        q = select(s for s in Student)
        q2 = q.filter(scholarship=500)
        result = set(q2.filter(group=Group['3132']))
        self.assertEqual(result, set([Student[104]]))

