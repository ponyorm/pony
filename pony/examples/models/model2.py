import datetime

from pony.main import *

class Faculty(Entity):
    _table_ = 'Faculties'
    number = PrimaryKey(int)
    subfaculties = Set('Subfaculty')
    # groups = IndirectSet('Group', 'subfaculties.groups')

class Subfaculty(Entity):
    _table_ = 'Subfaculties'
    number = PrimaryKey(int)
    faculty = Required(Faculty)
    specs = Set('Speciality')
    groups = Set('Group')
    teachers = Set('Teacher')

class Speciality(Entity):
    _table_ = 'Specialities'
    number = PrimaryKey(str)
    subfaculties = Set('Subfaculty')
    subjects = Set('Subject')
    groups = Set('Group')

class Subject(Entity):
    _table_ = 'Subjects'
    name = PrimaryKey(unicode)
    specs = Set(Speciality)
    lessons = Set('Lesson')
    marks = Set('Mark')

class Person(Entity):
    _table_ = 'Persons'
    first_name = Required(unicode)
    mid_name = Optional(unicode)
    last_name = Required(unicode)
    birth_date = Optional(datetime.date)

class Teacher(Entity):
    _table_ = 'Teacher'
    subfaculties = Set(Subfaculty)
    lessons = Set('Lesson')

class Lesson(Entity):
    _table_ = 'Lessons'
    subject = Required(Subject)
    teacher = Required(Teacher)
    group = Required('Group')
    PrimaryKey(subject, teacher, group)

class Group(Entity):
    _table_ = 'Groups'
    number = PrimaryKey(str)
    graduate_year = Required(int)
    # faculty = Indirect(Faculty, 'subfaculty.faculty')
    subfaculty = Required(Subfaculty)
    speciality = Required(Speciality)
    students = Set('Student')
    lessons = Set('Lesson')

class Student(Person):
    _table_ = 'Students'
    number = PrimaryKey(int)
    group = Required(Group)
    marks = Set('Mark')

class Mark(Entity):
    _table_ = 'Marks'
    student = Required(Student)
    subject = Required(Subject)
    value = Required(int)
    PrimaryKey(student, subject)



    