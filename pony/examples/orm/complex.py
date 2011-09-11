from datetime import date
from pony.orm import *

_diagram_ = Diagram()

class Group(Entity):
    dept = Required('Department')
    year = Required(int)
    spec = Required(int)
    students = Set('Student')
    courses = Set('Course')
    lessons = Set('Lesson', columns=['building', 'number', 'dt'])
    PrimaryKey(dept, year, spec)

class Department(Entity):
    number = PrimaryKey(int)
    faculty = Required('Faculty')
    name = Required(unicode)
    groups = Set(Group)
    teachers = Set('Teacher')

class Faculty(Entity):
    number = PrimaryKey(int)
    name = Required(unicode)
    depts = Set(Department)

class Student(Entity):
    name = Required(unicode)
    group = Required(Group)
    dob = Optional(date)
    grades = Set('Grade')
    PrimaryKey(name, group)

class Grade(Entity):
    student = Required(Student, columns=['student_name', 'dept', 'year', 'spec'])
    task = Required('Task')
    date = Required(date)
    value = Required(int)
    PrimaryKey(student, task)

class Task(Entity):
    course = Required('Course')
    type = Required(unicode)
    number = Required(int)
    descr = Optional(unicode)
    grades = Set(Grade)
    PrimaryKey(course, type, number)

class Course(Entity):
    subject = Required('Subject')
    semester = Required(int)
    groups = Set(Group)
    tasks = Set(Task)
    lessons = Set('Lesson')
    teachers = Set('Teacher')
    PrimaryKey(subject, semester)

class Subject(Entity):
    name = PrimaryKey(unicode)
    descr = Optional(unicode)
    courses = Set(Course)

class Room(Entity):
    building = Required(unicode)
    number = Required(unicode)
    floor = Optional(int)
    schedules = Set('Lesson')
    PrimaryKey(building, number)

class Teacher(Entity):
    dept = Required(Department)
    name = Required(unicode)
    courses = Set(Course)
    lessons = Set('Lesson')

class Lesson(Entity):
    _table_ = 'Schedule'
    groups = Set(Group)
    course = Required(Course)
    room = Required(Room)
    teacher = Required(Teacher)
    date = Required(date)
    PrimaryKey(room, date)
    Unique(teacher, date)

db = Database('sqlite', 'complex.sqlite', create_db=True)
db.generate_mapping(create_tables=True)

def test_queries():
    select(grade for grade in Grade if grade.task.type == 'Lab').all()
    select(grade for grade in Grade if grade.task.descr.startswith('Intermediate')).all()
    select(grade for grade in Grade if grade.task.course.semester == 2).all()
    select(grade for grade in Grade if grade.task.course.subject.name == 'Math').all()
    select(grade for grade in Grade if 'elementary' in grade.task.course.subject.descr.lower()).all()
    select(grade for grade in Grade if 'elementary' in grade.task.course.subject.descr.lower() and grade.task.descr.startswith('Intermediate')).all()
    select(grade for grade in Grade if grade.task.descr.startswith('Intermediate') and 'elementary' in grade.task.course.subject.descr.lower()).all()
    select(s for s in Student if s.group.dept.faculty.name == 'Abc').all()
    select(g for g in Group if avg(g.students.grades.value) > 4).all()
    select(g for g in Group if avg(g.students.grades.value) > 4 and max(g.students.grades.date) < date(2011, 3, 2)).all()
    select(g for g in Group if '4-A' in g.lessons.room.number).all()
    select(g for g in Group if 1 in g.lessons.room.floor).all()
    select(t for t in Teacher if t not in t.courses.groups.lessons.teacher).all()

sql_debug(True)
