import pony.db
pony.db.debug = True

from pony.db import Database
from pony.orm import *

_diagram_ = Diagram()

class Faculty(Entity):
    _table_ = 'Faculties'
    number = PrimaryKey(int)
    name = Unique(str)
    departments = Set('Department')

class Department(Entity):
    _table_ = 'Departments'
    number = PrimaryKey(int)
    name = Unique(str)
    faculty = Required(Faculty)
    teachers = Set('Teacher')
    majors = Set('Major')
    groups = Set('Group')

class Group(Entity):
    _table_ = 'Groups'
    number = PrimaryKey(int)
    grad_year = Required(int)
    department = Required(Department)
    lessons = Set('Lesson')
    students = Set('Student')

class Student(Entity):
    _table_ = 'Students'
    name = Required(unicode)
    scholarship = Required(int, default=0)
    group = Required(Group)
    grades = Set('Grade')

class Major(Entity):
    _table_ = 'Majors'
    name = PrimaryKey(str)
    department = Required(Department)
    courses = Set('Course')

class Subject(Entity):
    _table_ = 'Subjects'
    name = PrimaryKey(str)
    courses = Set('Course')
    teachers = Set('Teacher')
    
class Course(Entity):
    _table_ = 'Courses'
    major = Required(Major)
    subject = Required(Subject)
    semester = Required(int)
    Unique(major, subject, semester)
    lect_hours = Required(int)    
    pract_hours = Required(int)
    credit = Required(int)
    lessons = Set('Lesson')
    grades = Set('Grade')

class Lesson(Entity):
    _table_ = 'Lessons'
    day_of_week = Required(int)
    meeting_time = Required(int)
    classroom = Required('Classroom')
    PrimaryKey(day_of_week, meeting_time, classroom)
    course = Required(Course)
    teacher = Required('Teacher')
    groups = Set(Group)
    
class Grade(Entity):
    _table_ = 'Grades'
    student = Required(Student)
    course = Required(Course)
    PrimaryKey(student, course)
    teacher = Required('Teacher')
    date = Required(str)
    value = Required(str)
    
class Teacher(Entity):
    _table_ = 'Teachers'
    name = Required(str)
    degree = Optional(str)
    department = Required(Department)
    subjects = Set(Subject)
    lessons = Set(Lesson)
    grades = Set(Grade)

class Classroom(Entity):
    _table_ = 'Classrooms'
    number = PrimaryKey(str)
    description = Optional(str)
    lessons = Set(Lesson)

db = Database('sqlite', r'C:\Data\Docs\Dev\GAE\alexander-kozlovsky\pony\examples\orm\university\university.db3')
generate_mapping(db, check_tables=True)
