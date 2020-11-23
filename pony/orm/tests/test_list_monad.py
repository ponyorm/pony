from __future__ import absolute_import, print_function, division

import unittest
from datetime import date

from pony.orm.core import *
from pony.orm.tests.testutils import *
from pony.orm.tests import setup_database, teardown_database

db = Database()

class Department(db.Entity):
    number = PrimaryKey(int, auto=True)
    name = Required(unicode, unique=True)
    groups = Set("Group")
    courses = Set("Course")

class Group(db.Entity):
    number = PrimaryKey(int)
    major = Required(unicode)
    dept = Required("Department")
    students = Set("Student")

class Course(db.Entity):
    name = Required(unicode)
    semester = Required(int)
    lect_hours = Required(int)
    lab_hours = Required(int)
    credits = Required(int)
    dept = Required(Department)
    students = Set("Student")
    PrimaryKey(name, semester)

class Student(db.Entity):
    id = PrimaryKey(int, auto=True)
    name = Required(unicode)
    dob = Required(date)
    tel = Optional(str)
    picture = Optional(buffer, lazy=True)
    gpa = Required(float, default=0)
    phd = Optional(bool)
    group = Required(Group)
    courses = Set(Course)

class TestListMonad(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_database(db)
        with db_session:
            d1 = Department(number=1, name="Department of Computer Science")
            d2 = Department(number=2, name="Department of Mathematical Sciences")
            d3 = Department(number=3, name="Department of Applied Physics")

            c1 = Course(name="Web Design", semester=1, dept=d1,
                        lect_hours=30, lab_hours=30, credits=3)
            c2 = Course(name="Data Structures and Algorithms", semester=3, dept=d1,
                        lect_hours=40, lab_hours=20, credits=4)

            c3 = Course(name="Linear Algebra", semester=1, dept=d2,
                        lect_hours=30, lab_hours=30, credits=4)
            c4 = Course(name="Statistical Methods", semester=2, dept=d2,
                        lect_hours=50, lab_hours=25, credits=5)

            c5 = Course(name="Thermodynamics", semester=2, dept=d3,
                        lect_hours=25, lab_hours=40, credits=4)
            c6 = Course(name="Quantum Mechanics", semester=3, dept=d3,
                        lect_hours=40, lab_hours=30, credits=5)

            g101 = Group(number=101, major='B.E. in Computer Engineering', dept=d1)
            g102 = Group(number=102, major='B.S./M.S. in Computer Science', dept=d2)
            g103 = Group(number=103, major='B.S. in Applied Mathematics and Statistics', dept=d2)
            g104 = Group(number=104, major='B.S./M.S. in Pure Mathematics', dept=d2)
            g105 = Group(number=105, major='B.E in Electronics', dept=d3)
            g106 = Group(number=106, major='B.S./M.S. in Nuclear Engineering', dept=d3)

            Student(id=1, name='John Smith', dob=date(1991, 3, 20), tel='123-456', gpa=3, group=g101, phd=True,
                    courses=[c1, c2, c4, c6])
            Student(id=2, name='Matthew Reed', dob=date(1990, 11, 26), gpa=3.5, group=g101, phd=True,
                    courses=[c1, c3, c4, c5])
            Student(id=3, name='Chuan Qin', dob=date(1989, 2, 5), gpa=4, group=g101,
                    courses=[c3, c5, c6])
            Student(id=4, name='Rebecca Lawson', dob=date(1990, 4, 18), tel='234-567', gpa=3.3, group=g102,
                    courses=[c1, c4, c5, c6])
            Student(id=5, name='Maria Ionescu', dob=date(1991, 4, 23), gpa=3.9, group=g102,
                    courses=[c1, c2, c4, c6])
            Student(id=6, name='Oliver Blakey', dob=date(1990, 9, 8), gpa=3.1, group=g102,
                    courses=[c1, c2, c5])
            Student(id=7, name='Jing Xia', dob=date(1988, 12, 30), gpa=3.2, group=g102,
                    courses=[c1, c3, c5, c6])

    @classmethod
    def tearDownClass(cls):
        teardown_database(db)

    def setUp(self):
        rollback()
        db_session.__enter__()

    def tearDown(self):
        rollback()
        db_session.__exit__()

    def test_in_simple(self):
        q = select(s.id for s in Student if s.name in ('John Smith', 'Matthew Reed'))
        self.assertEqual(set(q), {1, 2})

    def test_not_in_simple(self):
        q = select(s.id for s in Student if s.name not in ('John Smith', 'Matthew Reed'))
        self.assertEqual(set(q), {3, 4, 5, 6, 7})

    def test_in_composite(self):
        q = select(c.name for c in Course if (c.name, c.semester) in [
            ("Web Design", 1), ("Thermodynamics", 2), ("Theology", 3)
        ])
        self.assertEqual(set(q), {"Web Design", "Thermodynamics"})

    def test_not_in_composite(self):
        q = select(c.name for c in Course if (c.name, c.semester) not in [
            ("Web Design", 1), ("Thermodynamics", 2), ("Theology", 3)
        ])
        self.assertEqual(set(q), {
            "Data Structures and Algorithms", "Linear Algebra",
            "Statistical Methods", "Quantum Mechanics"
        })

    def test_in_simple_object(self):
        s1, s2 = Student[1], Student[2]
        q = select(s.id for s in Student if s in (s1, s2))
        self.assertEqual(set(q), {1, 2})

    def test_not_in_simple_object(self):
        s1, s2 = Student[1], Student[2]
        q = select(s.id for s in Student if s not in (s1, s2))
        self.assertEqual(set(q), {3, 4, 5, 6, 7})

    def test_in_composite_object(self):
        c1, c2 = Course["Web Design", 1], Course["Thermodynamics", 2]
        q = select(c.name for c in Course if c in (c1, c2))
        self.assertEqual(set(q), {"Web Design", "Thermodynamics"})

    def test_not_in_composite_object(self):
        c1, c2 = Course["Web Design", 1], Course["Thermodynamics", 2]
        q = select(c.name for c in Course if c not in (c1, c2))
        self.assertEqual(set(q), {
            "Data Structures and Algorithms", "Linear Algebra",
            "Statistical Methods", "Quantum Mechanics"
        })


if __name__ == "__main__":
    unittest.main()
