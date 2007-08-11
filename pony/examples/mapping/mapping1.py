from pony.orm import *
from datetime import date

class Group(Entity):
    number = PrimaryKey(int)
    students = Set("Student")
    subjects = Set("Subject")

class Subject(Entity):
    name = PrimaryKey(str)
    groups = Set("Group")
    marks = Set("Mark")

class Person(Entity):
    first_name = Required(unicode)
    last_name = Required(unicode)

class Professor(Entity):
    title = Required(str)

class Student(Entity):
    group = Required("Group")
    marks = Set("Mark")

class Mark(Entity):
    student = Required("Student")
    subject = Required("Subject")
    PrimaryKey(student, subject)
    date = Required(date)
    value = Required(int)
