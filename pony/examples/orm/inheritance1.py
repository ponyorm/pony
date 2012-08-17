from decimal import Decimal
from datetime import date

from pony import options
options.CUT_TRACEBACK = False

from pony.orm import *

db = Database('sqlite', 'inheritance1.sqlite', create_db=True)

class Person(db.Entity):
    id = PrimaryKey(int, auto=True)
    name = Required(unicode)
    dob = Optional(date)
    ssn = Unique(str)

class Student(Person):
    group = Required("Group")
    attend_courses = Set("Course")

class Teacher(Person):
    teach_courses = Set("Course")
    salary = Required(Decimal)

class Assistant(Student, Teacher):
    professor = Required("Professor")

class Professor(Teacher):
    position = Required(unicode)
    assistants = Set(Assistant)

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
    s1 = Student(name='Student1', group=g, ssn='SSN2')
    s2 = Student(name='Student2', group=g, ssn='SSN3')
    prof = Professor(name='Professor1', salary=1000, position='position1', ssn='SSN5')
    a1 = Assistant(name='Assistant1', group=g, salary=100, ssn='SSN4', professor=prof)
    commit()

if __name__ == '__main__':
    populate_database()