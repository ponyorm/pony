from __future__ import absolute_import, print_function, division

import unittest
from decimal import Decimal
from datetime import date

from pony.orm.core import *
from pony.orm.tests.testutils import *

db = Database('sqlite', ':memory:')


class Group(db.Entity):
    id = PrimaryKey(int)
    major = Required(unicode)
    students = Set('Student')


class Student(db.Entity):
    name = Required(unicode)
    scholarship = Required(Decimal, default=0)
    picture = Optional(buffer, lazy=True)
    email = Required(unicode, unique=True)
    phone = Optional(unicode, unique=True)
    courses = Set('Course')
    group = Optional('Group')


class Course(db.Entity):
    name = Required(unicode)
    semester = Required(int)
    students = Set(Student)
    composite_key(name, semester)

db.generate_mapping(create_tables=True)

with db_session:
    g1 = Group(id=1, major='Math')
    g2 = Group(id=2, major='Physics')
    s1 = Student(id=1, name='S1', email='s1@example.com', group=g1)
    s2 = Student(id=2, name='S2', email='s2@example.com', group=g1)
    s3 = Student(id=3, name='S3', email='s3@example.com', group=g2)
    c1 = Course(name='Math', semester=1)
    c2 = Course(name='Math', semester=2)
    c3 = Course(name='Physics', semester=1)


class TestCRUD(unittest.TestCase):

    def setUp(self):
        rollback()
        db_session.__enter__()

    def tearDown(self):
        rollback()
        db_session.__exit__()

    def test_getitem_1(self):
        g1 = Group[1]
        self.assertEqual(g1.id, 1)

    @raises_exception(ObjectNotFound, 'Group[333]')
    def test_getitem_2(self):
        g333 = Group[333]

    def test_exists_1(self):
        x = Group.exists(id=1)
        self.assertEqual(x, True)

    def test_exists_2(self):
        x = Group.exists(id=333)
        self.assertEqual(x, False)

    def test_exists_3(self):
        g1 = Group[1]
        x = Student.exists(group=g1)
        self.assertEqual(x, True)

    def test_set1(self):
        s1 = Student[1]
        s1.set(name='New name', scholarship=100)
        self.assertEqual(s1.name, 'New name')
        self.assertEqual(s1.scholarship, 100)

    def test_set2(self):
        g1 = Group[1]
        s3 = Student[3]
        g1.set(students=[s3])
        self.assertEqual(s3.group, Group[1])

    def test_set3(self):
        c1 = Course[1]
        c1.set(name='Algebra', semester=3)

    def test_set4(self):
        s1 = Student[1]
        s1.set(name='New name', email='new_email@example.com')

    def test_validate_1(self):
        s4 = Student(id=3, name='S4', email='s4@example.com', group=1)

    def test_validate_2(self):
        s4 = Student(id=3, name='S4', email='s4@example.com', group='1')

    @raises_exception(TransactionIntegrityError)
    def test_validate_3(self):
        s4 = Student(id=3, name='S4', email='s4@example.com', group=100)
        flush()

    @raises_exception(ValueError, "Value type for attribute Group.id must be int. Got string 'not a number'")
    def test_validate_5(self):
        s4 = Student(id=3, name='S4', email='s4@example.com',
                     group='not a number')

    @raises_exception(TypeError, "Attribute Student.group must be of Group type. Got: datetime.date(2011, 1, 1)")
    def test_validate_6(self):
        s4 = Student(id=3, name='S4', email='s4@example.com',
                     group=date(2011, 1, 1))

    @raises_exception(TypeError, 'Invalid number of columns were specified for attribute Student.group. Expected: 1, got: 2')
    def test_validate_7(self):
        s4 = Student(id=3, name='S4', email='s4@example.com', group=(1, 2))
