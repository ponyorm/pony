from pony.orm import *

class Student(Entity):
    _table_ = "Students"
    record = PrimaryKey(int)
    name = Required(unicode, column="fio")
    group = Required("Group")
    scholarship = Required(int, default=0)
    marks = Set("Mark")

class Group(Entity):
    _table_ = "Groups"
    number = PrimaryKey(str)
    department = Required(int)
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
    PrimaryKey(student, subject)

db = Database('sqlite', ':memory:')

db.generate_mapping(create_tables=True)

@with_transaction
def populate_db():
    Physics = Subject.create('Physics')
    Chemistry = Subject.create('Chemistry')
    Math = Subject.create('Math')

    g3132 = Group.create('3132', department=33, subjects=[ Physics, Math ])
    g4145 = Group.create('4145', department=44, subjects=[ Physics, Chemistry, Math ])
    g4146 = Group.create('4146', department=44)

    s101 = Student.create(101, name='Bob', group=g4145, scholarship=0)
    s102 = Student.create(102, name='Joe', group=g4145, scholarship=800)
    s103 = Student.create(103, name='Alex', group=g4145, scholarship=0)
    s104 = Student.create(104, name='Brad', group=g3132, scholarship=500)
    s105 = Student.create(105, name='John', group=g3132, scholarship=1000)

    Mark.create(s101, Physics, value=4)
    Mark.create(s101, Math, value=3)
    Mark.create(s102, Chemistry, value=5)
    Mark.create(s103, Physics, value=2)
    Mark.create(s103, Chemistry, value=4)
populate_db()
