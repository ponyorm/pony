from __future__ import with_statement
from decimal import Decimal
import unittest
from pony.orm.core import *
from testutils import *

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
    def test_set1(self):
        s1 = Student[1]
        s1.set(name='New name', scholarship=100)
        self.assertEquals(s1.name, 'New name')
        self.assertEquals(s1.scholarship, 100)
    def test_set2(self):
        g1 = Group[1]
        s3 = Student[3]
        g1.set(students=[s3])
        self.assertEquals(s3.group, Group[1])
    def test_set3(self):
        c1 = Course[1]
        c1.set(name='Algebra', semester=3)
    def test_set4(self):
        s1 = Student[1]
        s1.set(name='New name', email='new_email@example.com')