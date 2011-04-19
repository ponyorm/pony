import unittest
from pony.orm import *

class Student(Entity):
    name = Required(unicode)
    scholarship = Optional(int)
    group = Required("Group")
    marks = Set("Mark")

class Group(Entity):
    number = PrimaryKey(int)
    department = Required(int)
    students = Set(Student)
    subjects = Set("Subject")

class Subject(Entity):
    name = PrimaryKey(unicode)
    groups = Set(Group)
    marks = Set("Mark")

class Mark(Entity):
    value = Required(int)
    student = Required(Student)
    subject = Required(Subject)
    PrimaryKey(student, subject)

db = Database('sqlite', ':memory:')
db.generate_mapping(create_tables=True)

@with_transaction
def populate_db():
    Math = Subject.create("Math")
    Physics = Subject.create("Physics")
    History = Subject.create("History")

    g41 = Group.create(41, department=101, subjects=[ Math, Physics, History ])
    g42 = Group.create(42, department=102, subjects=[ Math, Physics ])
    g43 = Group.create(43, department=102, subjects=[ Physics ])

    s1 = Student.create(1, name="Joe", scholarship=None, group=g41)
    s2 = Student.create(2, name="Bob", scholarship=100, group=g41)
    s3 = Student.create(3, name="Beth", scholarship=500, group=g41)
    s4 = Student.create(4, name="Jon", scholarship=500, group=g42)
    s5 = Student.create(5, name="Pete", scholarship=700, group=g42)

    Mark.create(value=5, student=s1, subject=Math)
    Mark.create(value=4, student=s2, subject=Physics)
    Mark.create(value=3, student=s2, subject=Math)
    Mark.create(value=2, student=s2, subject=History)
    Mark.create(value=1, student=s3, subject=History)
    Mark.create(value=2, student=s3, subject=Math)
    Mark.create(value=2, student=s4, subject=Math)
populate_db()

class TestObjectFlatMonad(unittest.TestCase):
    def setUp(self):
        rollback()

    def tearDown(Self):
        rollback()
        
    def test1(self):
        result = set(select(s.groups for s in Subject if len(s.name) == 4))
        self.assertEquals(result, set([Group(41), Group(42)]))

    def test2(self):
        result = set(select(g.students for g in Group if g.department == 102))
        self.assertEquals(result, set([Student(5), Student(4)]))

if __name__ == '__main__':
    unittest.main()
        