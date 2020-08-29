from __future__ import absolute_import, print_function, division

import unittest

from pony.orm.core import *
from pony.orm.tests.testutils import *
from pony.orm.tests import setup_database, teardown_database

db = Database()

class Department(db.Entity):
    number = PrimaryKey(int)
    groups = Set('Group')

class Group(db.Entity):
    id = PrimaryKey(int)
    dept = Required('Department')
    students = Set('Student')

class Student(db.Entity):
    name = Required(unicode)
    age = Required(int)
    group = Required('Group')
    scholarship = Required(int, default=0)
    courses = Set('Course')

class Course(db.Entity):
    name = Required(unicode)
    semester = Required(int)
    PrimaryKey(name, semester)
    students = Set('Student')


class TestDistinct(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_database(db)
        with db_session:
            d1 = Department(number=1)
            d2 = Department(number=2)
            g1 = Group(id=1, dept=d1)
            g2 = Group(id=2, dept=d2)
            s1 = Student(id=1, name='S1', age=20, group=g1, scholarship=0)
            s2 = Student(id=2, name='S2', age=21, group=g1, scholarship=100)
            s3 = Student(id=3, name='S3', age=23, group=g1, scholarship=200)
            s4 = Student(id=4, name='S4', age=21, group=g1, scholarship=100)
            s5 = Student(id=5, name='S5', age=23, group=g2, scholarship=0)
            s6 = Student(id=6, name='S6', age=23, group=g2, scholarship=200)
            c1 = Course(name='C1', semester=1, students=[s1, s2, s3])
            c2 = Course(name='C2', semester=1, students=[s2, s3, s5, s6])
            c3 = Course(name='C3', semester=2, students=[s4, s5, s6])

    @classmethod
    def tearDownClass(cls):
        teardown_database(db)

    def setUp(self):
        db_session.__enter__()

    def tearDown(self):
        db_session.__exit__()

    def test_group_by(self):
        result = set(select((s.age, sum(s.scholarship)) for s in Student if s.scholarship > 0))
        self.assertEqual(result, {(21, 200), (23, 400)})
        self.assertNotIn('distinct', db.last_sql.lower())

    def test_group_by_having(self):
        result = set(select((s.age, sum(s.scholarship)) for s in Student if sum(s.scholarship) < 300))
        self.assertEqual(result, {(20, 0), (21, 200)})
        self.assertNotIn('distinct', db.last_sql.lower())

    def test_aggregation_no_group_by_1(self):
        result = set(select(sum(s.scholarship) for s in Student if s.age < 23))
        self.assertEqual(result, {200})
        self.assertNotIn('distinct', db.last_sql.lower())

    def test_aggregation_no_group_by_2(self):
        result = set(select((sum(s.scholarship), min(s.scholarship)) for s in Student if s.age < 23))
        self.assertEqual(result, {(200, 0)})
        self.assertNotIn('distinct', db.last_sql.lower())

    def test_aggregation_no_group_by_3(self):
        result = set(select((sum(s.scholarship), min(s.scholarship))
                            for s in Student for g in Group
                            if s.group == g and g.dept.number == 1))
        self.assertEqual(result, {(400, 0)})
        self.assertNotIn('distinct', db.last_sql.lower())


if __name__ == "__main__":
    unittest.main()
