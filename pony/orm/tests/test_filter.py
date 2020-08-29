from __future__ import absolute_import, print_function, division

import unittest

from pony.orm.tests.model1 import *
from pony.orm.tests import setup_database, teardown_database


class TestFilter(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_database(db)
        populate_db()
    @classmethod
    def tearDownClass(cls):
        teardown_database(db)
    def setUp(self):
        rollback()
        db_session.__enter__()
    def tearDown(self):
        rollback()
        db_session.__exit__()
    def test_filter_1(self):
        q = select(s for s in Student)
        result = set(q.filter(scholarship=0))
        self.assertEqual(result, {Student[101], Student[103]})
    def test_filter_2(self):
        q = select(s for s in Student)
        q2 = q.filter(scholarship=500)
        result = set(q2.filter(group=Group['3132']))
        self.assertEqual(result, {Student[104]})
    def test_filter_3(self):
        q = select(s for s in Student)
        q2 = q.filter(lambda s: s.scholarship > 500)
        result = set(q2.filter(lambda s: count(s.marks) > 0))
        self.assertEqual(result, {Student[102]})
    def test_filter_4(self):
        q = select(s for s in Student)
        q2 = q.filter(lambda s: s.scholarship != 500)
        q3 = q2.order_by(1)
        result = list(q3.filter(lambda s: count(s.marks) > 1))
        self.assertEqual(result, [Student[101], Student[103]])
    def test_filter_5(self):
        q = select(s for s in Student)
        q2 = q.filter(lambda s: s.scholarship != 500)
        q3 = q2.order_by(Student.name)
        result = list(q3.filter(lambda s: count(s.marks) > 1))
        self.assertEqual(result, [Student[103], Student[101]])
    def test_filter_6(self):
        q = select(s for s in Student)
        q2 = q.filter(lambda s: s.scholarship != 500)
        q3 = q2.order_by(lambda s: s.name)
        result = list(q3.filter(lambda s: count(s.marks) > 1))
        self.assertEqual(result, [Student[103], Student[101]])
    def test_filter_7(self):
        q = select(s for s in Student)
        q2 = q.filter(scholarship=0)
        result = set(q2.filter(lambda s: count(s.marks) > 1))
        self.assertEqual(result, {Student[103], Student[101]})
    def test_filter_8(self):
        q = select(s for s in Student)
        q2 = q.filter(lambda s: s.scholarship != 500)
        q3 = q2.order_by(lambda s: s.name)
        q4 = q3.order_by(None)
        result = set(q4.filter(lambda s: count(s.marks) > 1))
        self.assertEqual(result, {Student[103], Student[101]})
