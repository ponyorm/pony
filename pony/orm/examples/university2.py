from datetime import date
from decimal import Decimal
from typing import TYPE_CHECKING

from pony.orm.core import (
    Database,
    Optional,
    PrimaryKey,
    Required,
    Set,
    composite_key,
    select,
    sql_debug,
)

db = Database()

if TYPE_CHECKING:
    from pony.orm.core import Entity
else:
    Entity = db.Entity


class Faculty(Entity):
    _table_ = "Faculties"
    number = PrimaryKey(int)
    name = Required(str, unique=True)
    departments = Set("Department")


class Department(Entity):
    _table_ = "Departments"
    number = PrimaryKey(int)
    name = Required(str, unique=True)
    faculty = Required(Faculty)
    teachers = Set("Teacher")
    majors = Set("Major")
    groups = Set("Group")


class Group(Entity):
    _table_ = "Groups"
    number = PrimaryKey(int)
    grad_year = Required(int)
    department = Required(Department, column="dep")
    lessons = Set(
        "Lesson",
        columns=["day_of_week", "meeting_time", "classroom_number", "building"],
    )
    students = Set("Student")


class Student(Entity):
    _table_ = "Students"
    name = Required(str)
    scholarship = Required(Decimal, 10, 2, default=Decimal("0.0"))
    group = Required(Group)
    grades = Set("Grade")


class Major(Entity):
    _table_ = "Majors"
    name = PrimaryKey(str)
    department = Required(Department)
    courses = Set("Course")


class Subject(Entity):
    _table_ = "Subjects"
    name = PrimaryKey(str)
    courses = Set("Course")
    teachers = Set("Teacher")


class Course(Entity):
    _table_ = "Courses"
    major = Required(Major)
    subject = Required(Subject)
    semester = Required(int)
    composite_key(major, subject, semester)
    lect_hours = Required(int)
    pract_hours = Required(int)
    credit = Required(int)
    lessons = Set("Lesson")
    grades = Set("Grade")


class Lesson(Entity):
    _table_ = "Lessons"
    day_of_week = Required(int)
    meeting_time = Required(int)
    classroom = Required("Classroom")
    PrimaryKey(day_of_week, meeting_time, classroom)
    course = Required(Course)
    teacher = Required("Teacher")
    groups = Set(Group)


class Grade(Entity):
    _table_ = "Grades"
    student = Required(Student)
    course = Required(Course)
    PrimaryKey(student, course)
    teacher = Required("Teacher")
    date = Required(date)
    value = Required(str)


class Teacher(Entity):
    _table_ = "Teachers"
    name = Required(str)
    degree = Optional(str)
    department = Required(Department)
    subjects = Set(Subject)
    lessons = Set(Lesson)
    grades = Set(Grade)


class Building(Entity):
    _table_ = "Buildings"
    number = PrimaryKey(str)
    description = Optional(str)
    classrooms = Set("Classroom")


class Classroom(Entity):
    _table_ = "Classrooms"
    building = Required(Building)
    number = Required(str)
    PrimaryKey(building, number)
    description = Optional(str)
    lessons = Set(Lesson)


db.bind("sqlite", "university2.sqlite", create_db=True)
# db.bind('mysql', host='localhost', user='pony', passwd='pony', db='university2')
# db.bind('postgres', user='pony', password='pony', host='localhost', database='university2')
# db.bind('oracle', 'university2/pony@localhost')

db.generate_mapping(create_tables=True)

sql_debug(True)


def test_queries():
    # very simple query
    select(s for s in Student)[:]

    # one condition
    select(s for s in Student if s.scholarship > 0)[:]

    # multiple conditions
    select(s for s in Student if s.scholarship > 0 and s.group.number == 4142)[:]

    # no join here - attribute can be found in table Students
    select(s for s in Student if s.group.number == 4142)[:]

    # automatic join of two tables because grad_year is stored in table Groups
    select(s for s in Student if s.group.grad_year == 2011)[:]

    # still two tables are joined
    select(s for s in Student if s.group.department.number == 44)[:]

    # automatic join of tree tables
    select(s for s in Student if s.group.department.name == "Ancient Philosophy")[:]

    # manual join of tables will produce equivalent query
    select(
        s
        for s in Student
        for g in Group
        if s.group == g and g.department.name == "Ancient Philosophy"
    )[:]

    # join two tables by composite foreign key
    select(
        c
        for c in Classroom
        for lesson in Lesson
        if lesson.classroom == c and lesson.course.subject.name == "Physics"
    )[:]

    # Lessons  will be joined with Buildings directly without Classrooms
    select(
        s
        for s in Subject
        for lesson in Lesson
        if s == lesson.course.subject
        and lesson.classroom.building.description == "some description"
    )[:]

    # just another example of join of many tables
    select(c for c in Course if c.major.department.faculty.number == 4)[:]
