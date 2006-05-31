# -*- coding: cp1251 -*-

from pony.main import *

class Group(Persistent):
    number = PrimaryKey(str)
    faculty = Required(int)
    graduate_year = Required(int)
    speciality = Required(int)
    subjects = Set('Subject')
    students = Set('Student')

class Subject(Persistent):
    name = PrimaryKey(unicode)
    groups = Set(Group)
    marks = Set('Mark')

class Student(Persistent):
    first_name = Required(unicode)
    mid_name = Optional(unicode)
    last_name = Required(unicode)
    group = Required(Group)
    marks = Set('Mark')

class Mark(Persistent):
    student = Required(Student)
    subject = Required(Subject)
    value = Required(int)
    PrimaryKey(student, subject)
    
