from pony.orm import *

class Student(Entity):
    _table_ = "Students"
    name = Required(unicode, column="fio")
    passport = Unique(int)
    zach = Optional("Zach")
    group = Optional("Group")
    stipendy = Required(int, default=0)
    marks = Set("Mark")

class Zach(Entity):
    number = PrimaryKey(str)
    student = Required(Student, column="student")

class Group(Entity):
    _table_ = "Groups"
    number = Required(str, column='grnum')
    kaf = Required("Kaf", column='dep')
    speciality = Required(str)
    PrimaryKey(number, kaf)
    students = Set("Student")
    subjects = Set("Subject")

class Kaf(Entity):
    _table_ = "Departments"
    number = PrimaryKey(int)
    name = Required(unicode)
    faculty = Required("Faculty", column="fnum")
    groups = Set(Group)

class Faculty(Entity):
    _table_ = "Faculties"
    number = PrimaryKey(int, column="fnum")
    name = Required(unicode, column="fname")
    kafs = Set(Kaf)

class Subject(Entity):
    _table_ = "Subjects"
    name = PrimaryKey(unicode)
    groups = Set("Group")
    marks = Set("Mark")

class Mark(Entity):
    _table_ = "Exams"
    student = Required(Student, column="student")
    subject = Required(Subject, column="subject")
    value = Required(int)
    Unique(student, subject)

db = Database('sqlite', 'C:\\Data\\Docs\\Dev\\GAE\\alexander-kozlovsky\\pony\\examples\\orm\\students01\\students2.db3')

sql_debug(False)
db.generate_mapping(check_tables=True)
sql_debug(True)

##g1 = Group(number='4142', kaf=44, speciality='230001')
##g2 = Group(number='3137', kaf=33, speciality='220102')
##s1 = Student(name='John', passport=777, group=g1)
##s2 = Student(name='Mike', passport=888, group=g1)
##s3 = Student(name='Frank', passport=999, group=g1)
##subj1 = Subject('Physics')
##subj2 = Subject('Math')
##subj3 = Subject('Chemistry')
##m1 = Mark(student=s1, subject=subj1, value=4)
##m2 = Mark(student=s2, subject=subj1, value=5)
##m3 = Mark(student=s1, subject=subj2, value=3)

