from decimal import Decimal
from datetime import date

from pony.orm import *

class Department(Entity):
    number = PrimaryKey(int)
    name = Unique(unicode)
    groups = Set("Group")
    courses = Set("Course")

class Group(Entity):
    number = PrimaryKey(int)
    major = Required(unicode)
    dept = Required("Department")
    students = Set("Student")

class Course(Entity):
    name = Required(unicode)
    semester = Required(int)
    lect_hours = Required(int)
    lab_hours = Required(int)
    credits = Required(int)
    dept = Required(Department)
    students = Set("Student")
    PrimaryKey(name, semester)
    
class Student(Entity):
    id = PrimaryKey(int, auto=True)
    name = Required(unicode)
    dob = Required(date)
    picture = Optional(buffer)
    gpa = Required(float, default=0)
    group = Required(Group)
    courses = Set(Course)

db = Database('sqlite', 'presentation.sqlite', create_db=True)
# db = Database('mysql', host="localhost", user="root", passwd="root", db="university")
db.generate_mapping(create_tables=True)

sql_debug(True)

select(s for s in Student).all()

select(s for s in Student if s.gpa > 3.5).all()

select(s for s in Student
         if s.gpa > 3.5 and s.dob.year == 1995).all()

select(s for s in Student
         if s.gpa > 3.5 and len(s.courses) > 5).all()

select(s for s in Student if s.picture is None).all()

select(s for s in Student if s.name.startswith("A")).all()

select(s for s in Student if "Smith" in s.name).all()

select(s for s in Student
         if "Finance" in (c.name for c in s.courses)).all()

select(s for s in Student 
         if "Finance" in s.courses.name).all()

select.avg(s.gpa for s in Student)

select(s for s in Student 
         if sum(c.credits for c in s.courses) < 12).all()

select(s for s in Student 
         if s.group.major == "Computer Science").all()

select(s for s in Student 
         if s.group.dept.name == "Digital Arts").all()

select(s for s in Student).orderby(Student.name).all()

select(s for s in Student).orderby(Student.name.desc).all()

select(s for s in Student).orderby(Student.group, Student.name).all()

students = select(s for s in Student if s.name.startswith('A')).all()

select(s for s in Student).orderby(Student.name)[20:30]

select(s for s in Student 
         if s.group.dept.name == "Digital Arts"
            and s.gpa > 3.5
            and len(s.courses) > 5).all()
