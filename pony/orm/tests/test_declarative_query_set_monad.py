from __future__ import with_statement

import unittest
from pony.orm.core import *
from testutils import *

db = Database('sqlite', ':memory:')

class Student(db.Entity):
    name = Required(unicode)
    group = Required('Group')
    scholarship = Required(int, default=0)

class Group(db.Entity):
    id = PrimaryKey(int)
    students = Set(Student)

db.generate_mapping(create_tables=True)

with db_session:
    g1 = Group(id=1)
    g2 = Group(id=2)
    s1 = Student(id=1, name='S1', group=g1, scholarship=0)
    s2 = Student(id=2, name='S2', group=g1, scholarship=100)
    s3 = Student(id=3, name='S3', group=g2, scholarship=500)

class TestQuerySetMonad(unittest.TestCase):
    def setUp(self):
        rollback()
        db_session.__enter__()

    def tearDown(self):
        rollback()
        db_session.__exit__()

    def test_len(self):
        result = set(select(g for g in Group if len(g.students) > 1))
        self.assertEquals(result, set([Group[1]]))

    def test_len2(self):
        result = set(select(g for g in Group if len(s for s in Student if s.group == g) > 1))
        self.assertEquals(result, set([Group[1]]))

    def test_len3(self):
        result = set(select(g for g in Group if len(s.name for s in Student if s.group == g) > 1))
        self.assertEquals(result, set([Group[1]]))

    @raises_exception(TypeError)
    def test_sum1(self):
        result = set(select(g for g in Group if sum(s for s in Student if s.group == g) > 1))
        self.assertEquals(result, set([]))

    @raises_exception(TypeError)
    def test_sum2(self):
        select(g for g in Group if sum(s.name for s in Student if s.group == g) > 1)

    def test_sum3(self):
        result = set(select(g for g in Group if sum(s.scholarship for s in Student if s.group == g) > 500))
        self.assertEquals(result, set([]))

    def test_min1(self):
        result = set(select(g for g in Group if min(s.name for s in Student if s.group == g) == 'S1'))
        self.assertEquals(result, set([Group[1]]))

    @raises_exception(TypeError)
    def test_min2(self):
        select(g for g in Group if min(s for s in Student if s.group == g) == None)

    def test_max1(self):
        result = set(select(g for g in Group if max(s.scholarship for s in Student if s.group == g) > 100))
        self.assertEquals(result, set([Group[2]]))

    @raises_exception(TypeError)
    def test_max2(self):
        select(g for g in Group if max(s for s in Student if s.group == g) == None)

    def test_avg1(self):
        result = select(g for g in Group if avg(s.scholarship for s in Student if s.group == g) == 50)[:]
        self.assertEquals(result, [Group[1]])

    def test_negate(self):
        result = set(select(g for g in Group if not(s.scholarship for s in Student if s.group == g)))
        self.assertEquals(result, set([]))

    def test_no_conditions(self):
        students = set(select(s for s in Student if s.group in (g for g in Group)))
        self.assertEqual(students, set([Student[1], Student[2], Student[3]]))

if __name__ == "__main__":
    unittest.main()
