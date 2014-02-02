from __future__ import with_statement

import unittest
from pony.orm.core import *
from testutils import *

db = Database('sqlite', ':memory:')

class Group(db.Entity):
    id = PrimaryKey(int)
    students = Set('Student')

class Student(db.Entity):
    name = Required(unicode)
    group = Required('Group')
    scholarship = Required(int, default=0)
    courses = Set('Course')

class Course(db.Entity):
    name = Required(unicode)
    semester = Required(int)
    PrimaryKey(name, semester)
    students = Set('Student')

db.generate_mapping(create_tables=True)

with db_session:
    g1 = Group(id=1)
    g2 = Group(id=2)
    s1 = Student(id=1, name='S1', group=g1, scholarship=0)
    s2 = Student(id=2, name='S2', group=g1, scholarship=100)
    s3 = Student(id=3, name='S3', group=g2, scholarship=500)
    c1 = Course(name='C1', semester=1, students=[s1, s2])
    c2 = Course(name='C2', semester=1, students=[s2, s3])
    c3 = Course(name='C3', semester=2, students=[s3])


class TestQuerySetMonad(unittest.TestCase):
    def setUp(self):
        rollback()
        db_session.__enter__()

    def tearDown(self):
        rollback()
        db_session.__exit__()

    def test_len(self):
        result = set(select(g for g in Group if len(g.students) > 1))
        self.assertEqual(result, set([Group[1]]))

    def test_len_2(self):
        result = set(select(g for g in Group if len(s for s in Student if s.group == g) > 1))
        self.assertEqual(result, set([Group[1]]))

    def test_len_3(self):
        result = set(select(g for g in Group if len(s.name for s in Student if s.group == g) > 1))
        self.assertEqual(result, set([Group[1]]))

    def test_count_1(self):
        result = set(select(g for g in Group if count(s.name for s in g.students) > 1))
        self.assertEqual(result, set([Group[1]]))

    def test_count_2(self):
        result = set(select(g for g in Group if select(s.name for s in g.students).count() > 1))
        self.assertEqual(result, set([Group[1]]))

    def test_count_3(self):
        result = set(select(s for s in Student if count(c for c in s.courses) > 1))
        self.assertEqual(result, set([Student[2], Student[3]]))

    def test_count_4(self):
        result = set(select(c for c in Course if count(s for s in c.students) > 1))
        self.assertEqual(result, set([Course['C1', 1], Course['C2', 1]]))

    @raises_exception(TypeError)
    def test_sum_1(self):
        result = set(select(g for g in Group if sum(s for s in Student if s.group == g) > 1))
        self.assertEqual(result, set([]))

    @raises_exception(TypeError)
    def test_sum_2(self):
        select(g for g in Group if sum(s.name for s in Student if s.group == g) > 1)

    def test_sum_3(self):
        result = set(select(g for g in Group if sum(s.scholarship for s in Student if s.group == g) > 500))
        self.assertEqual(result, set([]))

    def test_sum_4(self):
        result = set(select(g for g in Group if select(s.scholarship for s in g.students).sum() > 200))
        self.assertEqual(result, set([Group[2]]))

    def test_min_1(self):
        result = set(select(g for g in Group if min(s.name for s in Student if s.group == g) == 'S1'))
        self.assertEqual(result, set([Group[1]]))

    @raises_exception(TypeError)
    def test_min_2(self):
        select(g for g in Group if min(s for s in Student if s.group == g) == None)

    def test_min_3(self):
        result = set(select(g for g in Group if select(s.scholarship for s in g.students).min() == 0))
        self.assertEqual(result, set([Group[1]]))

    def test_max_1(self):
        result = set(select(g for g in Group if max(s.scholarship for s in Student if s.group == g) > 100))
        self.assertEqual(result, set([Group[2]]))

    @raises_exception(TypeError)
    def test_max_2(self):
        select(g for g in Group if max(s for s in Student if s.group == g) == None)

    def test_max_3(self):
        result = set(select(g for g in Group if select(s.scholarship for s in g.students).max() == 100))
        self.assertEqual(result, set([Group[1]]))

    def test_avg_1(self):
        result = select(g for g in Group if avg(s.scholarship for s in Student if s.group == g) == 50)[:]
        self.assertEqual(result, [Group[1]])

    def test_avg_2(self):
        result = set(select(g for g in Group if select(s.scholarship for s in g.students).avg() == 50))
        self.assertEqual(result, set([Group[1]]))

    def test_exists(self):
        result = set(select(g for g in Group if exists(s for s in g.students if s.name == 'S1')))
        self.assertEqual(result, set([Group[1]]))

    def test_negate(self):
        result = set(select(g for g in Group if not(s.scholarship for s in Student if s.group == g)))
        self.assertEqual(result, set([]))

    def test_no_conditions(self):
        students = set(select(s for s in Student if s.group in (g for g in Group)))
        self.assertEqual(students, set([Student[1], Student[2], Student[3]]))

    def test_no_conditions_2(self):
        students = set(select(s for s in Student if s.scholarship == max(s.scholarship for s in Student)))
        self.assertEqual(students, set([Student[3]]))

    def test_hint_join_1(self):
        result = set(select(s for s in Student if JOIN(s.group in select(g for g in Group if g.id < 2))))
        self.assertEqual(result, set([Student[1], Student[2]]))

    def test_hint_join_2(self):
        result = set(select(s for s in Student if JOIN(s.group not in select(g for g in Group if g.id < 2))))
        self.assertEqual(result, set([Student[3]]))

    def test_hint_join_3(self):
        result = set(select(s for s in Student if JOIN(s.scholarship in
                        select(s.scholarship + 100 for s in Student if s.name != 'S2'))))
        self.assertEqual(result, set([Student[2]]))

    def test_hint_join_4(self):
        result = set(select(g for g in Group if JOIN(g in select(s.group for s in g.students))))
        self.assertEqual(result, set([Group[1], Group[2]]))

if __name__ == "__main__":
    unittest.main()
