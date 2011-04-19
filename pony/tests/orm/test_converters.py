import unittest
from pony.orm import *
from decimal import Decimal
from datetime import date

class Student(Entity):
    name = Required(unicode)
    scholarship = Required(Decimal, 5, 2)
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
    date = Required(date)
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
    
    s1 = Student.create(1, name="Joe", scholarship=Decimal('99.9'), group=g41)
    s2 = Student.create(2, name="Bob", scholarship=Decimal('100.0'), group=g41)
    s3 = Student.create(3, name="Beth", scholarship=Decimal('500.5'), group=g41)
    s4 = Student.create(4, name="Jon", scholarship=Decimal('500.6'), group=g42)
    s5 = Student.create(5, name="Pete", scholarship=Decimal('700.1'), group=g42)

    Mark.create(value=5, student=s1, subject=Math,    date=date(2010, 10, 01))
    Mark.create(value=4, student=s2, subject=Physics, date=date(2010, 10, 02))
    Mark.create(value=3, student=s2, subject=Math,    date=date(2010, 10, 03))
    Mark.create(value=2, student=s2, subject=History, date=date(2010, 10, 04))
    Mark.create(value=1, student=s3, subject=History, date=date(2010, 10, 05))
    Mark.create(value=2, student=s3, subject=Math,    date=date(2010, 10, 06))
    Mark.create(value=2, student=s4, subject=Math,    date=date(2010, 10, 07))
populate_db()

class TestConverters(unittest.TestCase):
    def setUp(self):
        rollback()
    def tearDown(self):
        rollback()
        
    def test1(self):
        result = set(select(s.scholarship for s in Student if min(s.marks.value) < 2))
        self.assertEquals(result, set([Decimal("500.5")]))

##    def test2(self):
##        Group = self.Group
##        Student = self.Student
##        #result = set(select(s.scholarship for s in Student if min(s.scholarship) < Decimal("100")))
##        #result = set(select(s.scholarship for s in Student if s.marks.date == date(2010, 10, 2)))
##        self.assertEquals(result, set([]))
        
if __name__ == '__main__':
    unittest.main()
        