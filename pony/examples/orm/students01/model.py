import datetime
from decimal import Decimal

from pony.orm import *

class Student(Entity):
    _table_ = "Students"
    zach = PrimaryKey(int, auto=True)
    name = Required(unicode, column="fio")
    group = Required("Group")
    stipendy = Required(Decimal, 10, 2, default=0)
    marks = Set("Mark")

class Group(Entity):
    _table_ = "Groups"
    number = PrimaryKey(str)
    kaf = Required(int)
    students = Set("Student")
    subjects = Set("Subject")

class Subject(Entity):
    _table_ = "Subjects"
    name = PrimaryKey(unicode)
    groups = Set("Group")
    marks = Set("Mark")

class Mark(Entity):
    _table_ = "Exams"
    student = Required(Student)
    subject = Required(Subject)
    value = Required(int)
    date = Required(datetime.date)
    PrimaryKey(student, subject)

db = Database('sqlite', 'students.db3')
sql_debug(False)
db.generate_mapping(check_tables=True)
sql_debug(True)

##g1 = Group(number='4142', kaf=44)
##g2 = Group(number='3137', kaf=33)
##s1 = Student(zach=123, name='John', group=g1)
##s2 = Student(zach=124, name='Mike', group=g1)
##s3 = Student(zach=125, name='Frank', group=g1)
##subj1 = Subject('Physics')
##subj2 = Subject('Math')
##subj3 = Subject('Chemistry')
##m1 = Mark(student=s1, subject=subj1, value=4)
##m2 = Mark(student=s2, subject=subj1, value=5)
##m3 = Mark(student=s1, subject=subj2, value=3)

