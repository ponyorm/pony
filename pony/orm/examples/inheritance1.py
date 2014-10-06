from __future__ import absolute_import, print_function

from decimal import Decimal
from datetime import date

from pony import options
options.CUT_TRACEBACK = False

from pony.orm.core import *

sql_debug(False)

db = Database('sqlite', 'inheritance1.sqlite', create_db=True)

class Person(db.Entity):
    id = PrimaryKey(int, auto=True)
    name = Required(str)
    dob = Optional(date)
    ssn = Required(str, unique=True)

class Student(Person):
    group = Required("Group")
    mentor = Optional("Teacher")
    attend_courses = Set("Course")

class Teacher(Person):
    teach_courses = Set("Course")
    apprentices = Set("Student")
    salary = Required(Decimal)

class Assistant(Student, Teacher):
    pass

class Professor(Teacher):
    position = Required(str)

class Group(db.Entity):
    number = PrimaryKey(int)
    students = Set("Student")

class Course(db.Entity):
    name = Required(str)
    semester = Required(int)
    students = Set(Student)
    teachers = Set(Teacher)
    PrimaryKey(name, semester)

db.generate_mapping(create_tables=True)

@db_session
def populate_database():
    if Person.select().first():
        return # already populated

    p = Person(name='Person1', ssn='SSN1')
    g = Group(number=123)
    prof = Professor(name='Professor1', salary=1000, position='position1', ssn='SSN5')
    a1 = Assistant(name='Assistant1', group=g, salary=100, ssn='SSN4', mentor=prof)
    a2 = Assistant(name='Assistant2', group=g, salary=200, ssn='SSN6', mentor=prof)
    s1 = Student(name='Student1', group=g, ssn='SSN2', mentor=a1)
    s2 = Student(name='Student2', group=g, ssn='SSN3')
    commit()

def show_all_persons():
    for obj in Person.select():
        print(obj)
        for attr in obj._attrs_:
            print(attr.name, "=", attr.__get__(obj))
        print()

if __name__ == '__main__':
    populate_database()
    # show_all_persons()

    sql_debug(True)

    with db_session:
        s1 = Student.get(name='Student1')
        if s1 is None:
            print('Student1 not found')
        else:
            mentor = s1.mentor
            print(mentor.name, 'is mentor of Student1')
            print('Is he assistant?', isinstance(mentor, Assistant))
        print()

        for s in Student.select(lambda s: s.mentor.salary == 1000):
            print(s.name)
