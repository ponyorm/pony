from decimal import Decimal
from datetime import date

from pony.orm import *

class Group(Entity):
    number = PrimaryKey(int)
    major = Required(unicode)
    dept = Required("Department")
    students = Set("Student")
    
class Student(Entity):
    id = PrimaryKey(int, auto=True)
    name = Required(unicode)
    dob = Required(date)
    picture = Optional(buffer)
    scholarship = Required(Decimal, 7, 2, default=Decimal(0))
    group = Required(Group)
    courses = Set("Course")

class Department(Entity):
    number = PrimaryKey(int)
    name = Unique(unicode)
    groups = Set(Group)
    courses = Set("Course")

class Course(Entity):
    name = Required(unicode)
    semester = Required(int)
    lect_hours = Required(int)
    lab_hours = Required(int)
    credits = Required(int)
    dept = Required(Department)
    students = Set(Student)
    PrimaryKey(name, semester)

sql_debug(False)
db = Database('sqlite', 'presentation.sqlite', create_db=True)
db.generate_mapping(create_tables=True)

sql_debug(True)

students = select(s for s in Student).all()

Student.all()

##    SELECT "s"."id", "s"."name", "s"."dob", "s"."passport", "s"."picture", "s"."scholarship", "s"."group"
##    FROM "Student" AS "s"

students = select(s for s in Student
                    if s.scholarship > 0).all()

students = Student.all(lambda s: s.scholarship > 0)

students = Student.where(lambda s: s.scholarship > 0).all()

##    SELECT "s"."id", "s"."name", "s"."dob", "s"."passport", "s"."picture", "s"."scholarship", "s"."group"
##    FROM "Student" AS "s"
##    WHERE ("s"."scholarship" > 0)

students = select(s for s in Student
                    if s.scholarship > 0
                    and s.dob > date(1995, 1, 1)).all()

students = Student.all(lambda s: s.scholarship > 0
                                 and s.dob > date(1995, 1, 1))

students = Student.where(lambda s: s.scholarship > 0
                                   and s.dob > date(1995, 1, 1)).all()

##    SELECT "s"."id", "s"."name", "s"."dob", "s"."passport", "s"."picture", "s"."scholarship", "s"."group"
##    FROM "Student" AS "s"
##    WHERE ("s"."scholarship" > 0)
##      AND ("s"."dob" > '1995-01-01')

students = select(s for s in Student
                    if s.picture is None).all()

students = Student.all(lambda s: s.picture is None)

##    SELECT "s"."id", "s"."name", "s"."dob", "s"."passport", "s"."picture", "s"."scholarship", "s"."group"
##    FROM "Student" AS "s"
##    WHERE "s"."picture" IS NOT NULL

students = select(s for s in Student if len(s.courses) > 5).all() # todo

students = Student.all(lambda s: len(s.courses) > 5)

students = Student.where(lambda s: len(s.courses) > 5).all()

##    SELECT "s"."id", "s"."name", "s"."dob", "s"."passport", "s"."picture", "s"."scholarship", "s"."group"
##    FROM "Student" AS "s"
##    WHERE ((
##        SELECT COUNT(*)
##        FROM "Course_Student" AS "m2m--1"
##        WHERE ("s"."id" = "m2m--1"."student")
##    ) > 5)

students = select(s for s in Student if s.name.startswith('A')).all()

students = Student.all(lambda s: s.name.startswith('A'))

##    SELECT "s"."id", "s"."name", "s"."dob", "s"."passport", "s"."picture", "s"."scholarship", "s"."group"
##    FROM "Student" AS "s"
##    WHERE "s"."name" LIKE 'A%'

courses = select(c for c in Course if 'Economics' in c.name).all()

courses = Course.all(lambda c: 'Economics' in c.name)

##    SELECT "c"."name", "c"."semester", "c"."lect_hours", "c"."lab_hours", "c"."credits", "c"."dept"
##    FROM "Course" AS "c"
##    WHERE "c"."name" LIKE '%Economics%'

select(s for s in Student if s.group.major == 'Computer Science').all()

Student.all(lambda s: s.group.major == 'Computer Science')

##    SELECT "s"."id", "s"."name", "s"."dob", "s"."passport", "s"."picture", "s"."scholarship", "s"."group"
##    FROM "Student" AS "s", "Group" AS "s-group"
##    WHERE ("s"."group" = "s-group"."number")
##      AND ("s-group"."major" = 'Computer Science')

select(s for s in Student if s.group.dept.name == 'Digital Arts').all()

Student.all(lambda s: s.group.dept.name == 'Digital Arts')

##    SELECT "s"."id", "s"."name", "s"."dob", "s"."passport", "s"."picture", "s"."scholarship", "s"."group"
##    FROM "Student" AS "s", "Group" AS "s-group", "Department" AS "s-group-dept"
##    WHERE ("s"."group" = "s-group"."number")
##      AND ("s-group"."dept" = "s-group-dept"."id")
##      AND ("s-group-dept"."name" = 'Digital Arts')

select(s for s in Student if s.group.dept.number == 33).all()

Student.all(lambda s: s.group.dept.number == 33)

##    SELECT "s"."id", "s"."name", "s"."dob", "s"."picture", "s"."scholarship", "s"."group"
##    FROM "Student" AS "s", "Group" AS "s-group"
##    WHERE ("s"."group" = "s-group"."number")
##      AND ("s-group"."dept" = 33)

dept_name = 'Biology'
select(s for s in Student if s.group.dept.name == dept_name).all()

Student.all(lambda s: s.group.dept.name == dept_name)

##    SELECT "s"."id", "s"."name", "s"."dob", "s"."picture", "s"."scholarship", "s"."group"
##    FROM "Student" AS "s", "Group" AS "s-group", "Department" AS "s-group-dept"
##    WHERE ("s"."group" = "s-group"."number")
##      AND ("s-group"."dept" = "s-group-dept"."number")
##      AND ("s-group-dept"."name" = ?)

select(s for s in Student if exists(c for c in s.courses if c.name == 'Math')).all()
select(s for s in Student if 'Math' in (c.name for c in s.courses)).all()
select(s for s in Student if 'Math' in s.courses.name).all()

Student.all(lambda s: exists(c for c in s.courses if c.name == 'Math'))
Student.all(lambda s: 'Math' in (c.name for c in s.courses))
Student.all(lambda s: 'Math' in s.courses.name)

##    SELECT "s"."id", "s"."name", "s"."dob", "s"."picture", "s"."scholarship", "s"."group"
##    FROM "Student" AS "s"
##    WHERE 'Math' IN (
##        SELECT "s--c"."course_name"
##        FROM "Course_Student" AS "s--c"
##        WHERE ("s"."id" = "s--c"."student")
##    )

select.sum(s.scholarship for s in Student)

##    SELECT coalesce(SUM("s"."scholarship"), 0)
##    FROM "Student" AS "s"

select.sum(s.scholarship for s in Student if s.group.number == 123)

##    SELECT coalesce(SUM("s"."scholarship"), 0)
##    FROM "Student" AS "s"
##    WHERE ("s"."group" = 123)

select(g for g in Group if sum(s.scholarship for s in g.students) > 10000).all()
select(g for g in Group if sum(g.students.scholarship) > 10000).all()

Group.all(lambda g: sum(s.scholarship for s in g.students) > 10000)
Group.all(lambda g: sum(g.students.scholarship) > 10000)

##    SELECT "g"."number", "g"."major", "g"."dept"
##    FROM "Group" AS "g"
##    WHERE ((
##        SELECT coalesce(SUM("s"."scholarship"), 0)
##        FROM "Student" AS "s"
##        WHERE ("g"."number" = "s"."group")
##    ) > 10000)

select.sum(c.lab_hours for c in Course if c.dept.number == 123)

##    SELECT coalesce(SUM("c"."lab_hours"), 0)
##    FROM "Course" AS "c"
##    WHERE ("c"."dept" = 123)

select(d for d in Department if sum(c.lab_hours for c in d.courses) > 100).all()
select(d for d in Department if sum(d.courses.lab_hours) > 100).all()

Department.all(lambda d: sum(c.lab_hours for c in d.courses) > 100)
Department.all(lambda d: sum(d.courses.lab_hours) > 100)

##    SELECT "d"."number", "d"."name"
##    FROM "Department" AS "d"
##    WHERE ((
##        SELECT coalesce(SUM("c"."lab_hours"), 0)
##        FROM "Course" AS "c"
##        WHERE ("d"."number" = "c"."dept")
##    ) > 100)

select(s for s in Student).orderby(Student.name).all()

Student.orderby(Student.name).all()

##    SELECT "s"."id", "s"."name", "s"."dob", "s"."picture", "s"."scholarship", "s"."group"
##    FROM "Student" AS "s"
##    ORDER BY "s"."name" ASC

select(s for s in Student).orderby(Student.name.desc).all()

Student.orderby(Student.name.desc).all()

##    SELECT "s"."id", "s"."name", "s"."dob", "s"."picture", "s"."scholarship", "s"."group"
##    FROM "Student" AS "s"
##    ORDER BY "s"."name" DESC

select(s for s in Student).orderby(Student.group, Student.name).all()

Student.orderby(Student.group, Student.name).all()

##    SELECT "s"."id", "s"."name", "s"."dob", "s"."picture", "s"."scholarship", "s"."group"
##    FROM "Student" AS "s"
##    ORDER BY "s"."group" ASC, "s"."name" ASC

select(s for s in Student if s.scholarship > 0).orderby(Student.name).all()

Student.where(lambda s: s.scholarship > 0).orderby(Student.name).all()

##    SELECT "s"."id", "s"."name", "s"."dob", "s"."picture", "s"."scholarship", "s"."group"
##    FROM "Student" AS "s"
##    WHERE ("s"."scholarship" > 0)
##    ORDER BY "s"."name" ASC

select(s for s in Student).orderby(Student.name)[20:30]

Student.orderby(Student.name)[20:30]

##    SELECT "s"."id", "s"."name", "s"."dob", "s"."picture", "s"."scholarship", "s"."group"
##    FROM "Student" AS "s"
##    ORDER BY "s"."name" ASC
##    LIMIT 10 OFFSET 20
