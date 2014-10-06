from __future__ import absolute_import

from datetime import date
from pony.orm.core import *

db = Database('sqlite', 'complex.sqlite', create_db=True)

class Group(db.Entity):
    dept = Required('Department')
    year = Required(int)
    spec = Required(int)
    students = Set('Student')
    courses = Set('Course')
    lessons = Set('Lesson', columns=['building', 'number', 'dt'])
    PrimaryKey(dept, year, spec)

class Department(db.Entity):
    number = PrimaryKey(int)
    faculty = Required('Faculty')
    name = Required(str)
    groups = Set(Group)
    teachers = Set('Teacher')

class Faculty(db.Entity):
    number = PrimaryKey(int)
    name = Required(str)
    depts = Set(Department)

class Student(db.Entity):
    name = Required(str)
    group = Required(Group)
    dob = Optional(date)
    grades = Set('Grade')
    PrimaryKey(name, group)

class Grade(db.Entity):
    student = Required(Student, columns=['student_name', 'dept', 'year', 'spec'])
    task = Required('Task')
    date = Required(date)
    value = Required(int)
    PrimaryKey(student, task)

class Task(db.Entity):
    course = Required('Course')
    type = Required(str)
    number = Required(int)
    descr = Optional(str)
    grades = Set(Grade)
    PrimaryKey(course, type, number)

class Course(db.Entity):
    subject = Required('Subject')
    semester = Required(int)
    groups = Set(Group)
    tasks = Set(Task)
    lessons = Set('Lesson')
    teachers = Set('Teacher')
    PrimaryKey(subject, semester)

class Subject(db.Entity):
    name = PrimaryKey(str)
    descr = Optional(str)
    courses = Set(Course)

class Room(db.Entity):
    building = Required(str)
    number = Required(str)
    floor = Optional(int)
    schedules = Set('Lesson')
    PrimaryKey(building, number)

class Teacher(db.Entity):
    dept = Required(Department)
    name = Required(str)
    courses = Set(Course)
    lessons = Set('Lesson')

class Lesson(db.Entity):
    _table_ = 'Schedule'
    groups = Set(Group)
    course = Required(Course)
    room = Required(Room)
    teacher = Required(Teacher)
    date = Required(date)
    PrimaryKey(room, date)
    composite_key(teacher, date)

db.generate_mapping(create_tables=True)

def test_queries():
    select(grade for grade in Grade if grade.task.type == 'Lab')[:]
    select(grade for grade in Grade if grade.task.descr.startswith('Intermediate'))[:]
    select(grade for grade in Grade if grade.task.course.semester == 2)[:]
    select(grade for grade in Grade if grade.task.course.subject.name == 'Math')[:]
    select(grade for grade in Grade if 'elementary' in grade.task.course.subject.descr.lower())[:]
    select(grade for grade in Grade if 'elementary' in grade.task.course.subject.descr.lower() and grade.task.descr.startswith('Intermediate'))[:]
    select(grade for grade in Grade if grade.task.descr.startswith('Intermediate') and 'elementary' in grade.task.course.subject.descr.lower())[:]
    select(s for s in Student if s.group.dept.faculty.name == 'Abc')[:]
    select(g for g in Group if avg(g.students.grades.value) > 4)[:]
    select(g for g in Group if avg(g.students.grades.value) > 4 and max(g.students.grades.date) < date(2011, 3, 2))[:]
    select(g for g in Group if '4-A' in g.lessons.room.number)[:]
    select(g for g in Group if 1 in g.lessons.room.floor)[:]
    select(t for t in Teacher if t not in t.courses.groups.lessons.teacher)[:]

sql_debug(True)
