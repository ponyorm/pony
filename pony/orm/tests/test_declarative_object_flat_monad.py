from __future__ import absolute_import, print_function, division

import unittest
from pony.orm.core import *
from pony.orm.tests import setup_database, teardown_database

db = Database()

class Student(db.Entity):
    name = Required(unicode)
    scholarship = Optional(int)
    group = Required("Group")
    marks = Set("Mark")

class Group(db.Entity):
    number = PrimaryKey(int)
    department = Required(int)
    students = Set(Student)
    subjects = Set("Subject")

class Subject(db.Entity):
    name = PrimaryKey(unicode)
    groups = Set(Group)
    marks = Set("Mark")

class Mark(db.Entity):
    value = Required(int)
    student = Required(Student)
    subject = Required(Subject)
    PrimaryKey(student, subject)


class TestObjectFlatMonad(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        setup_database(db)
        with db_session:
            Math = Subject(name="Math")
            Physics = Subject(name="Physics")
            History = Subject(name="History")

            g41 = Group(number=41, department=101, subjects=[Math, Physics, History])
            g42 = Group(number=42, department=102, subjects=[Math, Physics])
            g43 = Group(number=43, department=102, subjects=[Physics])

            s1 = Student(id=1, name="Joe", scholarship=None, group=g41)
            s2 = Student(id=2, name="Bob", scholarship=100, group=g41)
            s3 = Student(id=3, name="Beth", scholarship=500, group=g41)
            s4 = Student(id=4, name="Jon", scholarship=500, group=g42)
            s5 = Student(id=5, name="Pete", scholarship=700, group=g42)

            Mark(value=5, student=s1, subject=Math)
            Mark(value=4, student=s2, subject=Physics)
            Mark(value=3, student=s2, subject=Math)
            Mark(value=2, student=s2, subject=History)
            Mark(value=1, student=s3, subject=History)
            Mark(value=2, student=s3, subject=Math)
            Mark(value=2, student=s4, subject=Math)

    @classmethod
    def tearDownClass(cls):
        teardown_database(db)

    @db_session
    def test1(self):
        result = set(select(s.groups for s in Subject if len(s.name) == 4))
        self.assertEqual(result, {Group[41], Group[42]})

    @db_session
    def test2(self):
        result = set(select(g.students for g in Group if g.department == 102))
        self.assertEqual(result, {Student[5], Student[4]})

if __name__ == '__main__':
    unittest.main()
