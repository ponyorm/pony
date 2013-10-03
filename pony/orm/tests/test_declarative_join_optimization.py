import unittest
from datetime import date
from pony.orm.core import *
from testutils import *

db = Database('sqlite', ':memory:')

class Department(db.Entity):
    name = Required(str)
    groups = Set('Group')
    courses = Set('Course')

class Group(db.Entity):
    number = PrimaryKey(int)
    dept = Required(Department)
    major = Required(unicode)
    students = Set("Student")

class Course(db.Entity):
    name = Required(unicode)
    dept = Required(Department)
    semester = Required(int)
    credits = Required(int)
    students = Set("Student")
    PrimaryKey(name, semester)

class Student(db.Entity):
    id = PrimaryKey(int, auto=True)
    name = Required(unicode)
    dob = Required(date)
    picture = Optional(buffer)
    gpa = Required(float, default=0)
    group = Required(Group)
    courses = Set(Course)


db.generate_mapping(create_tables=True)

class TestM2MOptimization(unittest.TestCase):
    def setUp(self):
        rollback()
        db_session.__enter__()
    def tearDown(self):
        rollback()
        db_session.__exit__()
    def test1(self):
        q = select(s for s in Student if len(s.courses) > 2)
        self.assertEqual(Course._table_ not in flatten(q._translator.conditions), True)
    def test2(self):
        q = select(s for s in Student if max(s.courses.semester) > 2)
        self.assertEqual(Course._table_ not in flatten(q._translator.conditions), True)
    # def test3(self):
    #     q = select(s for s in Student if max(s.courses.credits) > 2)
    #     self.assertEqual(Course._table_ in flatten(q._translator.conditions), True)
    #     self.assertEqual(Course.students.table in flatten(q._translator.conditions), True)
    def test4(self):
        q = select(g for g in Group if sum(g.students.gpa) > 5)
        self.assertEqual(Group._table_ not in flatten(q._translator.conditions), True)
    def test5(self):
        q = select(s for s in Student if s.group.number == 1 or s.group.major == '1')
        self.assertEqual(Group._table_ in flatten(q._translator.subquery.from_ast), True)
    # def test6(self): ###  Broken with ExprEvalError: Group[101] raises ObjectNotFound: Group[101]
    #    q = select(s for s in Student if s.group == Group[101])
    #    self.assertEqual(Group._table_ not in flatten(q._translator.subquery.from_ast), True)
    def test7(self):
        q = select(s for s in Student if sum(c.credits for c in Course if s.group.dept == c.dept) > 10)
        objects = q[:]
        self.assertEqual(str(q._translator.subquery.from_ast),
            "['FROM', ['s', 'TABLE', 'Student'], ['group-1', 'TABLE', 'Group', ['EQ', ['COLUMN', 's', 'group'], ['COLUMN', 'group-1', 'number']]]]")


if __name__ == '__main__':
    unittest.main()
