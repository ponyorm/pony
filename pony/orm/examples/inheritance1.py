from decimal import Decimal
from datetime import date

from pony import options
options.CUT_TRACEBACK = False

from pony.orm.core import *

sql_debug(True)

db = Database('sqlite', 'inheritance1.sqlite', create_db=True)

class Person(db.Entity):
    id = PrimaryKey(int, auto=True)
    name = Required(unicode)
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
    position = Required(unicode)

class Group(db.Entity):
    number = PrimaryKey(int)
    students = Set("Student")

class Course(db.Entity):
    name = Required(unicode)
    semester = Required(int)
    students = Set(Student)
    teachers = Set(Teacher)
    PrimaryKey(name, semester)

db.generate_mapping(create_tables=True)

def populate_database():
    p = Person(name='Person1', ssn='SSN1')
    g = Group(number=123)
    prof = Professor(name='Professor1', salary=1000, position='position1', ssn='SSN5')
    a1 = Assistant(name='Assistant1', group=g, salary=100, ssn='SSN4', mentor=prof)
    a2 = Assistant(name='Assistant2', group=g, salary=200, ssn='SSN6', mentor=prof)
    s1 = Student(name='Student1', group=g, ssn='SSN2', mentor=a1)
    s2 = Student(name='Student2', group=g, ssn='SSN3')
    commit()

def show_all_persons():
    for obj in select(p for p in Person):
        print obj
        print obj._attrs_
        for attr in obj._attrs_:
            print attr.name, "=", attr.__get__(obj)
        print

if __name__ == '__main__':
    # populate_database()
    # show_all_persons()
    s1 = Student.get(name='Student1')
    mentor = s1.mentor
    print isinstance(mentor, Assistant)
    print mentor.name
