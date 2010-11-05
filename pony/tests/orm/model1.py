import os.path
from pony.db import Database
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
path = os.path.split(__file__)[0]
script_filename = os.path.join(path, 'model1-database.sql')
script_sql = file(script_filename).read()
db.get_connection().executescript(script_sql)
db.commit()
generate_mapping(db, check_tables=True)

