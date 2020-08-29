from __future__ import absolute_import, print_function, division

import unittest
from datetime import date
from decimal import Decimal

from pony.orm.core import *
from pony.orm.sqltranslation import IncomparableTypesError
from pony.orm.tests.testutils import *
from pony.orm.tests import setup_database, teardown_database

db = Database()

class Department(db.Entity):
    number = PrimaryKey(int, auto=True)
    name = Required(unicode, unique=True)
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
    phd = Optional(bool)
    group = Required(Group)
    courses = Set(Course)

class TestSQLTranslator2(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_database(db)
        with db_session:
            d1 = Department(number=1, name="Department of Computer Science")
            d2 = Department(number=2, name="Department of Mathematical Sciences")
            d3 = Department(number=3, name="Department of Applied Physics")

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

            Student(id=1, name='John Smith', dob=date(1991, 3, 20), tel='123-456', gpa=3, group=g101, phd=True,
                    courses=[c1, c2, c4, c6])
            Student(id=2, name='Matthew Reed', dob=date(1990, 11, 26), gpa=3.5, group=g101, phd=True,
                    courses=[c1, c3, c4, c5])
            Student(id=3, name='Chuan Qin', dob=date(1989, 2, 5), gpa=4, group=g101,
                    courses=[c3, c5, c6])
            Student(id=4, name='Rebecca Lawson', dob=date(1990, 4, 18), tel='234-567', gpa=3.3, group=g102,
                    courses=[c1, c4, c5, c6])
            Student(id=5, name='Maria Ionescu', dob=date(1991, 4, 23), gpa=3.9, group=g102,
                    courses=[c1, c2, c4, c6])
            Student(id=6, name='Oliver Blakey', dob=date(1990, 9, 8), gpa=3.1, group=g102,
                    courses=[c1, c2, c5])
            Student(id=7, name='Jing Xia', dob=date(1988, 12, 30), gpa=3.2, group=g102,
                    courses=[c1, c3, c5, c6])

    @classmethod
    def tearDownClass(cls):
        teardown_database(db)

    def setUp(self):
        rollback()
        db_session.__enter__()
    def tearDown(self):
        rollback()
        db_session.__exit__()
    def test_distinct1(self):
        q = select(c.students for c in Course)
        self.assertEqual(q._translator.distinct, True)
        self.assertEqual(q.count(), 7)
    def test_distinct3(self):
        q = select(d for d in Department if len(s for c in d.courses for s in c.students) > len(s for s in Student))
        self.assertEqual(q[:], [])
        self.assertTrue('DISTINCT' in db.last_sql)
    def test_distinct4(self):
        q = select(d for d in Department if len(d.groups.students) > 3)
        self.assertEqual(q[:], [Department[2]])
        self.assertTrue("DISTINCT" not in db.last_sql)
    def test_distinct5(self):
        result = set(select(s for s in Student))
        self.assertEqual(result, {Student[1], Student[2], Student[3], Student[4], Student[5], Student[6], Student[7]})
    def test_distinct6(self):
        result = set(select(s for s in Student).distinct())
        self.assertEqual(result, {Student[1], Student[2], Student[3], Student[4], Student[5], Student[6], Student[7]})
    def test_not_null1(self):
        q = select(g for g in Group if '123-45-67' not in g.students.tel and g.dept == Department[1])
        not_null = "IS_NOT_NULL COLUMN student tel" in (" ".join(str(i) for i in flatten(q._translator.conditions)))
        self.assertEqual(not_null, True)
        self.assertEqual(q[:], [Group[101]])
    def test_not_null2(self):
        q = select(g for g in Group if 'John' not in g.students.name and g.dept == Department[1])
        not_null = "IS_NOT_NULL COLUMN student name" in (" ".join(str(i) for i in flatten(q._translator.conditions)))
        self.assertEqual(not_null, False)
        self.assertEqual(q[:], [Group[101]])
    def test_chain_of_attrs_inside_for1(self):
        result = set(select(s for d in Department if d.number == 2 for s in d.groups.students))
        self.assertEqual(result, {Student[4], Student[5], Student[6], Student[7]})
    def test_chain_of_attrs_inside_for2(self):
        pony.options.SIMPLE_ALIASES = False
        result = set(select(s for d in Department if d.number == 2 for s in d.groups.students))
        self.assertEqual(result, {Student[4], Student[5], Student[6], Student[7]})
        pony.options.SIMPLE_ALIASES = True
    def test_non_entity_result1(self):
        result = select((s.name, s.group.number) for s in Student if s.name.startswith("J"))[:]
        self.assertEqual(sorted(result), [(u'Jing Xia', 102), (u'John Smith', 101)])
    def test_non_entity_result2(self):
        result = select((s.dob.year, s.group.number) for s in Student)[:]
        self.assertEqual(sorted(result), [(1988, 102), (1989, 101), (1990, 101), (1990, 102), (1991, 101), (1991, 102)])
    def test_non_entity_result3(self):
        result = select(s.dob.year for s in Student).without_distinct()
        self.assertEqual(sorted(result), [1988, 1989, 1990, 1990, 1990, 1991, 1991])
        result = select(s.dob.year for s in Student)[:]  # test the last query didn't override the cached one
        self.assertEqual(sorted(result), [1988, 1989, 1990, 1991])
    def test_non_entity_result3a(self):
        result = select(s.dob.year for s in Student)[:]
        self.assertEqual(sorted(result), [1988, 1989, 1990, 1991])
    def test_non_entity_result4(self):
        result = set(select(s.name for s in Student if s.name.startswith('M')))
        self.assertEqual(result, {u'Matthew Reed', u'Maria Ionescu'})
    def test_non_entity_result5(self):
        result = select((s.group, s.dob) for s in Student if s.group == Group[101])[:]
        self.assertEqual(sorted(result), [(Group[101], date(1989, 2, 5)), (Group[101], date(1990, 11, 26)), (Group[101], date(1991, 3, 20))])
    def test_non_entity_result6(self):
        result = select((c, s) for s in Student for c in Course if c.semester == 1 and s.id < 3)[:]
        self.assertEqual(sorted(result), sorted([(Course[u'Linear Algebra',1], Student[1]), (Course[u'Linear Algebra',1],
            Student[2]), (Course[u'Web Design',1], Student[1]), (Course[u'Web Design',1], Student[2])]))
    def test_non_entity7(self):
        result = set(select(s for s in Student if (s.name, s.dob) not in (((s2.name, s2.dob) for s2 in Student if s.group.number == 101))))
        self.assertEqual(result, {Student[4], Student[5], Student[6], Student[7]})
    @raises_exception(IncomparableTypesError, "Incomparable types 'int' and 'Set of Student' in expression: g.number == g.students")
    def test_incompartible_types(self):
        select(g for g in Group if g.number == g.students)
    @raises_exception(TranslationError, "External parameter 'x' cannot be used as query result")
    def test_external_param1(self):
        x = Student[1]
        select(x for s in Student)
    def test_external_param2(self):
        x = Student[1]
        result = set(select(s for s in Student if s.name != x.name))
        self.assertEqual(result, {Student[2], Student[3], Student[4], Student[5], Student[6], Student[7]})
    @raises_exception(TypeError, "Use select(...) function or Group.select(...) method for iteration")
    def test_exception1(self):
        for g in Group:
            pass
    @raises_exception(MultipleObjectsFoundError, "Multiple objects were found. Use select(...) to retrieve them")
    def test_exception2(self):
         get(s for s in Student)
    def test_exists(self):
        result = exists(s for s in Student)
    @raises_exception(ExprEvalError, "`db.FooBar` raises AttributeError: 'Database' object has no attribute 'FooBar'")
    def test_entity_not_found(self):
        select(s for s in db.Student for g in db.FooBar)
    def test_keyargs1(self):
        result = set(select(s for s in Student if s.dob < date(year=1990, month=10, day=20)))
        self.assertEqual(result, {Student[3], Student[4], Student[6], Student[7]})
    def test_query_as_string1(self):
        result = set(select('s for s in Student if 3 <= s.gpa < 4'))
        self.assertEqual(result, {Student[1], Student[2], Student[4], Student[5], Student[6], Student[7]})
    def test_query_as_string2(self):
        result = set(select('s for s in db.Student if 3 <= s.gpa < 4'))
        self.assertEqual(result, {Student[1], Student[2], Student[4], Student[5], Student[6], Student[7]})
    def test_str_subclasses(self):
        result = select(d for d in Department for g in d.groups for c in d.courses if g.number == 106 and c.name.startswith('T'))[:]
        self.assertEqual(result, [Department[3]])
    def test_unicode_subclass(self):
        class Unicode2(unicode):
            pass
        u2 = Unicode2(u'\xf0')
        select(s for s in Student if len(u2) == 1)
    def test_bool(self):
        result = set(select(s for s in Student if s.phd == True))
        self.assertEqual(result, {Student[1], Student[2]})
    def test_bool2(self):
        result = list(select(s for s in Student if s.phd + 1 == True))
        self.assertEqual(result, [])
    def test_bool3(self):
        result = list(select(s for s in Student if s.phd + 1.1 == True))
        self.assertEqual(result, [])
    def test_bool4(self):
        result = list(select(s for s in Student if s.phd + Decimal('1.1') == True))
        self.assertEqual(result, [])
    def test_bool5(self):
        x = True
        result = set(select(s for s in Student if s.phd == True and (False or (True and x))))
        self.assertEqual(result, {Student[1], Student[2]})
    def test_bool6(self):
        x = False
        result = list(select(s for s in Student if s.phd == (False or (True and x)) and s.phd is True))
        self.assertEqual(result, [])

if __name__ == "__main__":
    unittest.main()
