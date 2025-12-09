from __future__ import absolute_import, print_function, division
from pony.py23compat import pickle
import unittest


from pony.orm.core import *
from pony.orm.tests import setup_database, teardown_database

db = Database()

class Department(db.Entity):
    number = PrimaryKey(int)
    groups = Set('Group')
    courses = Set('Course')

class Group(db.Entity):
    number = PrimaryKey(int)
    department = Required(Department)
    students = Set('Student')

class Student(db.Entity):
    name = Required(str)
    group = Required(Group)
    courses = Set('Course')

class Course(db.Entity):
    name = PrimaryKey(str)
    department = Required(Department)
    students = Set('Student')


class TestMultiset(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        setup_database(db)

        with db_session:
            d1 = Department(number=1)
            d2 = Department(number=2)
            d3 = Department(number=3)

            g1 = Group(number=101, department=d1)
            g2 = Group(number=102, department=d1)
            g3 = Group(number=201, department=d2)

            c1 = Course(name='C1', department=d1)
            c2 = Course(name='C2', department=d1)
            c3 = Course(name='C3', department=d2)
            c4 = Course(name='C4', department=d2)
            c5 = Course(name='C5', department=d3)

            s1 = Student(name='S1', group=g1, courses=[c1, c2])
            s2 = Student(name='S2', group=g1, courses=[c1, c3])
            s3 = Student(name='S3', group=g1, courses=[c2, c3])

            s4 = Student(name='S4', group=g2, courses=[c1, c2])
            s5 = Student(name='S5', group=g2, courses=[c1, c2])

            s6 = Student(name='A', group=g3, courses=[c5])

    @classmethod
    def tearDownClass(cls):
        teardown_database(db)

    @db_session
    def test_multiset_repr_1(self):
        d = Department[1]
        multiset = d.groups.students
        self.assertEqual(repr(multiset), "<StudentMultiset Department[1].groups.students (5 items)>")

    @db_session
    def test_multiset_repr_2(self):
        g = Group[101]
        multiset = g.students.courses
        self.assertEqual(repr(multiset), "<CourseMultiset Group[101].students.courses (6 items)>")

    @db_session
    def test_multiset_repr_3(self):
        g = Group[201]
        multiset = g.students.courses
        self.assertEqual(repr(multiset), "<CourseMultiset Group[201].students.courses (1 item)>")

    def test_multiset_repr_4(self):
        with db_session:
            g = Group[101]
            multiset = g.students.courses
        self.assertIsNone(multiset._obj_._session_cache_)
        self.assertEqual(repr(multiset), "<CourseMultiset Group[101].students.courses>")

    @db_session
    def test_multiset_str(self):
        g = Group[101]
        multiset = g.students.courses
        self.assertEqual(str(multiset), "CourseMultiset({Course[%r]: 2, Course[%r]: 2, Course[%r]: 2})"
                         % (u'C1', u'C2', u'C3'))

    @db_session
    def test_multiset_distinct(self):
        d = Department[1]
        multiset = d.groups.students.courses
        self.assertEqual(multiset.distinct(), {Course['C1']: 4, Course['C2']: 4, Course['C3']: 2})

    @db_session
    def test_multiset_nonzero(self):
        d = Department[1]
        multiset = d.groups.students
        self.assertEqual(bool(multiset), True)

    @db_session
    def test_multiset_len(self):
        d = Department[1]
        multiset = d.groups.students.courses
        self.assertEqual(len(multiset), 10)

    @db_session
    def test_multiset_eq(self):
        d = Department[1]
        multiset = d.groups.students.courses
        c1, c2, c3 = Course['C1'], Course['C2'], Course['C3']
        self.assertEqual(multiset, multiset)
        self.assertEqual(multiset, {c1: 4, c2: 4, c3: 2})
        self.assertEqual(multiset, [ c1, c1, c1, c2, c2, c2, c2, c3, c3, c1 ])

    @db_session
    def test_multiset_ne(self):
        d = Department[1]
        multiset = d.groups.students.courses
        self.assertFalse(multiset != multiset)

    @db_session
    def test_multiset_contains(self):
        d = Department[1]
        multiset = d.groups.students.courses
        self.assertTrue(Course['C1'] in multiset)
        self.assertFalse(Course['C5'] in multiset)

    def test_multiset_reduce(self):
        with db_session:
            d = Department[1]
            multiset = d.groups.students
            s = pickle.dumps(multiset)
        with db_session:
            d = Department[1]
            multiset_2 = d.groups.students
            multiset_1 = pickle.loads(s)
            self.assertEqual(multiset_1, multiset_2)


if __name__ == '__main__':
    unittest.main()
