from pony.db import Database
from pony.orm3 import *

class Student(Entity):
    _table_ = "Students"
    name = Required(unicode, column="fio")
    passport = Unique(int)
    zach = Optional("Zach")
    group = Optional("Group")
    stipendy = Required(int, default=0)
    marks = Set("Mark")

class Zach(Entity):
    number = PrimaryKey(str)
    student = Required(Student, column="student")

class Group(Entity):
    _table_ = "Groups"
    number = Required(str, column='grnum')
    kaf = Required(int)
    speciality = Required(str)
    PrimaryKey(number, kaf)
    students = Set("Student")
    subjects = Set("Subject")

class Subject(Entity):
    _table_ = "Subjects"
    name = PrimaryKey(unicode)
    groups = Set("Group")
    marks = Set("Mark")

class Mark(Entity):
    _table_ = "Exams"
    student = Required(Student, column="student")
    subject = Required(Subject, column="subject")
    value = Required(int)
    Unique(student, subject)

db = Database('sqlite', 'C:\\Data\\Docs\\Dev\\GAE\\alexander-kozlovsky\\pony\\examples\\orm\\students01\\students2.db3')
generate_mapping(db, check_tables=True)

##g1 = Group.create(number='4142', kaf=44, speciality='230001')
##g2 = Group.create(number='3137', kaf=33, speciality='220102')
##s1 = Student.create(name='John', passport=777, group=g1)
##s2 = Student.create(name='Mike', passport=888, group=g1)
##s3 = Student.create(name='Frank', passport=999, group=g1)
##subj1 = Subject.create('Physics')
##subj2 = Subject.create('Math')
##subj3 = Subject.create('Chemistry')
##m1 = Mark.create(student=s1, subject=subj1, value=4)
##m2 = Mark.create(student=s2, subject=subj1, value=5)
##m3 = Mark.create(student=s1, subject=subj2, value=3)

