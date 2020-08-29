from datetime import date
from decimal import Decimal

import unittest

from pony.orm import *
from pony.orm.tests.testutils import *
from pony.orm.tests import setup_database, teardown_database, only_for

db = Database()


class Person(db.Entity):
    id = PrimaryKey(int, auto=True)
    name = Required(str)
    dob = Optional(date)
    ssn = Required(str, unique=True)


class Student(Person):
    group = Required("Group")
    mentor = Optional("Teacher")
    attend_courses = Set("Course")


class Teacher(Person):
    teach_courses = Set("Course")
    apprentices = Set("Student")
    salary = Required(Decimal)


class Assistant(Student, Teacher):
    pass


class Professor(Teacher):
    position = Required(str)


class Group(db.Entity):
    number = PrimaryKey(int)
    students = Set("Student")


class Course(db.Entity):
    name = Required(str)
    semester = Required(int)
    students = Set(Student)
    teachers = Set(Teacher)
    PrimaryKey(name, semester)


class TestVolatile(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_database(db)
        with db_session:
            p = Person(name='Person1', ssn='SSN1')
            g = Group(number=123)
            prof = Professor(name='Professor1', salary=1000, position='position1', ssn='SSN5')
            a1 = Assistant(name='Assistant1', group=g, salary=100, ssn='SSN4', mentor=prof)
            a2 = Assistant(name='Assistant2', group=g, salary=200, ssn='SSN6', mentor=prof)
            s1 = Student(name='Student1', group=g, ssn='SSN2', mentor=a1)
            s2 = Student(name='Student2', group=g, ssn='SSN3')

    @classmethod
    def tearDownClass(cls):
        teardown_database(db)

    @db_session
    def test_1(self):
        q = select(p.name for p in Person if isinstance(p, Student))
        self.assertEqual(set(q), {'Student1', 'Student2', 'Assistant1', 'Assistant2'})

    @db_session
    def test_2(self):
        q = select(p.name for p in Person if not isinstance(p, Student))
        self.assertEqual(set(q), {'Person1', 'Professor1'})

    @db_session
    def test_3(self):
        q = select(p.name for p in Student if isinstance(p, Professor))
        self.assertEqual(set(q), set())

    @db_session
    def test_4(self):
        q = select(p.name for p in Person if not isinstance(p, Person))
        self.assertEqual(set(q), set())

    @db_session
    def test_5(self):
        q = select(p.name for p in Person if isinstance(p, (Student, Teacher)))
        self.assertEqual(set(q), {'Student1', 'Student2', 'Assistant1', 'Assistant2', 'Professor1'})

    @db_session
    def test_6(self):
        q = select(p.name for p in Person if isinstance(p, Student) and isinstance(p, Teacher))
        self.assertEqual(set(q), {'Assistant1', 'Assistant2'})

    @db_session
    def test_7(self):
        q = select(p.name for p in Person
                   if (isinstance(p, Student) and p.ssn == 'SSN2')
                   or (isinstance(p, Professor) and p.salary > 500))
        self.assertEqual(set(q), {'Student1', 'Professor1'})

    @db_session
    def test_8(self):
        q = select(p.name for p in Person if isinstance(p, Person))
        self.assertEqual(set(q), {'Person1', 'Student1', 'Student2', 'Assistant1', 'Assistant2', 'Professor1'})

    @db_session
    def test_9(self):
        q = select(g.number for g in Group if isinstance(g, Group))
        self.assertEqual(set(q), {123})


if __name__ == '__main__':
    unittest.main()
