import unittest
from datetime import date
from pony.orm import *
from testutils import *

db = TestDatabase('sqlite', ':memory:')

class Department(db.Entity):
    number = PrimaryKey(int, auto=True)
    name = Unique(unicode)
    groups = Set("Group")
    courses = Set("Course")

class Group(db.Entity):
    number = PrimaryKey(int)
    major = Required(unicode)
    dept = Required("Department")
    students = Set("Student")

class Course(db.Entity):
    name = Required(unicode)
    semester = Required(int)
    lect_hours = Required(int)
    lab_hours = Required(int)
    credits = Required(int)
    dept = Required(Department)
    students = Set("Student")
    PrimaryKey(name, semester)
    
class Student(db.Entity):
    id = PrimaryKey(int, auto=True)
    name = Required(unicode)
    dob = Required(date)
    tel = Optional(str)
    picture = Optional(buffer, lazy=True)
    gpa = Required(float, default=0)
    group = Required(Group)
    courses = Set(Course)

@with_transaction
def populate_db():
    d1 = Department(name="Department of Computer Science")
    d2 = Department(name="Department of Mathematical Sciences")
    d3 = Department(name="Department of Applied Physics")

    c1 = Course(name="Web Design", semester=1, dept=d1,
                       lect_hours=30, lab_hours=30, credits=3)
    c2 = Course(name="Data Structures and Algorithms", semester=3, dept=d1,
                       lect_hours=40, lab_hours=20, credits=4)

    c3 = Course(name="Linear Algebra", semester=1, dept=d2,
                       lect_hours=30, lab_hours=30, credits=4)
    c4 = Course(name="Statistical Methods", semester=2, dept=d2,
                       lect_hours=50, lab_hours=25, credits=5)

    c5 = Course(name="Thermodynamics", semester=2, dept=d3,
                       lect_hours=25, lab_hours=40, credits=4)
    c6 = Course(name="Quantum Mechanics", semester=3, dept=d3,
                       lect_hours=40, lab_hours=30, credits=5)

    g101 = Group(number=101, major='B.E. in Computer Engineering', dept=d1)
    g102 = Group(number=102, major='B.S./M.S. in Computer Science', dept=d2)
    g103 = Group(number=103, major='B.S. in Applied Mathematics and Statistics', dept=d2)
    g104 = Group(number=104, major='B.S./M.S. in Pure Mathematics', dept=d2)
    g105 = Group(number=105, major='B.E in Electronics', dept=d3)
    g106 = Group(number=106, major='B.S./M.S. in Nuclear Engineering', dept=d3)

    s1 = Student(name='John Smith', dob=date(1991, 3, 20), tel='123-456', gpa=3, group=g101,
                        courses=[c1, c2, c4, c6])
    s1 = Student(name='Matthew Reed', dob=date(1990, 11, 26), gpa=3.5, group=g101,
                        courses=[c1, c3, c4, c5])
    s1 = Student(name='Chuan Qin', dob=date(1989, 2, 5), gpa=4, group=g101,
                        courses=[c3, c5, c6])
    s1 = Student(name='Rebecca Lawson', dob=date(1990, 4, 18), tel='234-567', gpa=3.3, group=g102,
                        courses=[c1, c4, c5, c6])
    s1 = Student(name='Maria Ionescu', dob=date(1991, 4, 23), gpa=3.9, group=g102,
                        courses=[c1, c2, c4, c6])
    s1 = Student(name='Oliver Blakey', dob=date(1990, 9, 8), gpa=3.1, group=g102,
                        courses=[c1, c2, c5])
    s1 = Student(name='Jing Xia', dob=date(1988, 12, 30), gpa=3.2, group=g102,
                        courses=[c1, c3, c5, c6])

db.generate_mapping(create_tables=True)
populate_db()

class TestSQLTranslator2(unittest.TestCase):
    def setUp(self):
        rollback()
    def tearDown(self):
        rollback()
    def test_distinct1(self):
        q = query(c.students for c in Course)
        self.assertEquals(q._translator.distinct, True)
        self.assertEquals(q.count(), 7)
    def test_distinct2(self):
        q = query(d for d in Department if len(d.courses.students) > len(s for s in Student))
        self.assertEquals("DISTINCT" in flatten(q._translator.conditions), True)
        self.assertEquals(q.fetch_all(), [])
    def test_distinct3(self):
        q = query(d for d in Department if len(s for c in d.courses for s in c.students) > len(s for s in Student))
        self.assertEquals("DISTINCT" in flatten(q._translator.conditions), True)
        self.assertEquals(q.fetch_all(), [])
    def test_distinct4(self):
        q = query(d for d in Department if len(d.groups.students) > 3)
        self.assertEquals("DISTINCT" not in flatten(q._translator.conditions), True)
        self.assertEquals(q.fetch_all(), [Department[2]])
    def test_not_null1(self):
        q = query(g for g in Group if '123-45-67' not in g.students.tel and g.dept == Department[1])
        not_null = "IS_NOT_NULL COLUMN student-1 tel" in (" ".join(str(i) for i in flatten(q._translator.conditions)))
        self.assertEquals(not_null, True)
        self.assertEquals(q.fetch_all(), [Group[101]])
    def test_not_null2(self):
        q = query(g for g in Group if 'John' not in g.students.name and g.dept == Department[1])
        not_null = "IS_NOT_NULL COLUMN student-1 name" in (" ".join(str(i) for i in flatten(q._translator.conditions)))
        self.assertEquals(not_null, False)
        self.assertEquals(q.fetch_all(), [Group[101]])
    def test_chain_of_attrs_inside_for1(self):
        result = fetch(s for d in Department if d.number == 2 for s in d.groups.students)
        self.assertEquals(result, [Student[4], Student[5], Student[6], Student[7]])
    def test_chain_of_attrs_inside_for2(self):
        pony.options.SIMPLE_ALIASES = False
        result = fetch(s for d in Department if d.number == 2 for s in d.groups.students)
        self.assertEquals(result, [Student[4], Student[5], Student[6], Student[7]])
        pony.options.SIMPLE_ALIASES = True

if __name__ == "__main__":
    unittest.main()
