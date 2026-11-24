from datetime import date
from typing import TYPE_CHECKING

from pony.orm.core import (
    Database,
    Optional,
    PrimaryKey,
    Required,
    Set,
    avg,
    composite_key,
    select,
    sql_debug,
)

db = Database("sqlite", "complex.sqlite", create_db=True)

if TYPE_CHECKING:
    from pony.orm.core import Entity
else:
    Entity = db.Entity


class Group(Entity):
    dept = Required("Department")
    year = Required(int)
    spec = Required(int)
    students = Set("Student")
    courses = Set("Course")
    lessons = Set("Lesson", columns=["building", "number", "dt"])
    PrimaryKey(dept, year, spec)


class Department(Entity):
    number = PrimaryKey(int)
    faculty = Required("Faculty")
    name = Required(str)
    groups = Set(Group)
    teachers = Set("Teacher")


class Faculty(Entity):
    number = PrimaryKey(int)
    name = Required(str)
    depts = Set(Department)


class Student(Entity):
    name = Required(str)
    group = Required(Group)
    dob = Optional(date)
    grades = Set("Grade")
    PrimaryKey(name, group)


class Grade(Entity):
    student = Required(Student, columns=["student_name", "dept", "year", "spec"])
    task = Required("Task")
    date = Required(date)
    value = Required(int)
    PrimaryKey(student, task)


class Task(Entity):
    course = Required("Course")
    type = Required(str)
    number = Required(int)
    descr = Optional(str)
    grades = Set(Grade)
    PrimaryKey(course, type, number)


class Course(Entity):
    subject = Required("Subject")
    semester = Required(int)
    groups = Set(Group)
    tasks = Set(Task)
    lessons = Set("Lesson")
    teachers = Set("Teacher")
    PrimaryKey(subject, semester)


class Subject(Entity):
    name = PrimaryKey(str)
    descr = Optional(str)
    courses = Set(Course)


class Room(Entity):
    building = Required(str)
    number = Required(str)
    floor = Optional(int)
    schedules = Set("Lesson")
    PrimaryKey(building, number)


class Teacher(Entity):
    dept = Required(Department)
    name = Required(str)
    courses = Set(Course)
    lessons = Set("Lesson")


class Lesson(Entity):
    _table_ = "Schedule"
    groups = Set(Group)
    course = Required(Course)
    room = Required(Room)
    teacher = Required(Teacher)
    date = Required(date)
    PrimaryKey(room, date)
    composite_key(teacher, date)


db.generate_mapping(create_tables=True)


def test_queries():
    select(grade for grade in Grade if grade.task.type == "Lab")[:]
    select(grade for grade in Grade if grade.task.descr.startswith("Intermediate"))[:]
    select(grade for grade in Grade if grade.task.course.semester == 2)[:]
    select(grade for grade in Grade if grade.task.course.subject.name == "Math")[:]
    select(
        grade
        for grade in Grade
        if "elementary" in grade.task.course.subject.descr.lower()
    )[:]
    select(
        grade
        for grade in Grade
        if "elementary" in grade.task.course.subject.descr.lower()
        and grade.task.descr.startswith("Intermediate")
    )[:]
    select(
        grade
        for grade in Grade
        if grade.task.descr.startswith("Intermediate")
        and "elementary" in grade.task.course.subject.descr.lower()
    )[:]
    select(s for s in Student if s.group.dept.faculty.name == "Abc")[:]
    select(g for g in Group if avg(g.students.grades.value) > 4)[:]
    select(
        g
        for g in Group
        if avg(g.students.grades.value) > 4
        and max(g.students.grades.date) < date(2011, 3, 2)
    )[:]
    select(g for g in Group if "4-A" in g.lessons.room.number)[:]
    select(g for g in Group if 1 in g.lessons.room.floor)[:]
    select(t for t in Teacher if t not in t.courses.groups.lessons.teacher)[:]


sql_debug(True)
