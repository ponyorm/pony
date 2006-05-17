# -*- coding: cp1251 -*-

import datetime

from pony.main import *

class Faculty(Persistent):
    number = PrimaryKey(int)
    subfaculties = Set('Subfaculty')
    # groups = IndirectSet('Group', 'subfaculties.groups')

class Subfaculty(Persistent):
    number = PrimaryKey(int)
    faculty = Required(Faculty)
    specs = Set('Speciality')
    groups = Set('Group')
    teachers = Set('Teacher')

class Speciality(Persistent):
    number = PrimaryKey(str)
    subfaculties = Set('Subfaculty')
    subjects = Set('Subject')
    groups = Set('Group')

class Subject(Persistent):
    name = PrimaryKey(unicode)
    specs = Set(Speciality)
    lessons = Set('Lesson')
    marks = Set('Mark')

class Person(Persistent):
    first_name = Required(unicode)
    mid_name = Optional(unicode)
    last_name = Required(unicode)
    birth_date = Optional(datetime.date)

class Teacher(Persistent):
    subfaculties = Set(Subfaculty)
    lessons = Set('Lesson')

class Lesson(Persistent):
    subject = Required(Subject)
    teacher = Required(Teacher)
    group = Required('Group')
    PrimaryKey(subject, teacher, group)

class Group(Persistent):
    number = PrimaryKey(str)
    graduate_year = Required(int)
    # faculty = Indirect(Faculty, 'subfaculty.faculty')
    subfaculty = Required(Subfaculty)
    speciality = Required(Speciality)
    students = Set('Student')
    lessons = Set('Lesson')

class Student(Person):
    number = PrimaryKey(int)
    group = Required(Group)
    marks = Set('Mark')

class Mark(Persistent):
    student = Required(Student)
    subject = Required(Subject)
    value = Required(int)
    PrimaryKey(student, subject)



    