from decimal import Decimal
from datetime import date

from pony.orm import *

db = Database('sqlite', 'inheritance1.sqlite', create_db=True)

class Person(db.Entity):
    id = PrimaryKey(int, auto=True)
    name = Required(unicode)
    dob = Required(date)
    ssn = Unique(str)

class Student(Person):
    group = Required("Group")
    attend_courses = Set("Course")

class Teacher(Person):
    teach_courses = Set("Course")
    salary = Required(Decimal)

class Assistant(Student, Teacher):
    professor = Required("Professor")

class Professor(Teacher):
    position = Required(unicode)
    assistants = Set(Assistant)

class Group(db.Entity):
    number = PrimaryKey(int)
    students = Set("Student")

class Course(db.Entity):
    name = Required(unicode)
    semester = Required(int)
    students = Set(Student)
    teachers = Set(Teacher)
    PrimaryKey(name, semester)

db.generate_mapping(create_tables=True)
