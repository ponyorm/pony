from __future__ import absolute_import, print_function, division

import unittest
from datetime import date
from decimal import Decimal

from pony.orm.core import *
from pony.orm.tests.testutils import *

db = Database('sqlite', ':memory:')

class Student(db.Entity):
    name = Required(unicode)
    scholarship = Optional(int)
    gpa = Optional(Decimal,3,1)
    group = Required('Group')
    dob = Optional(date)

class Group(db.Entity):
    number = PrimaryKey(int)
    students = Set(Student)

db.generate_mapping(create_tables=True)

with db_session:
    g1 = Group(number=1)
    Student(id=1, name='S1', group=g1, gpa=3.1)
    Student(id=2, name='S2', group=g1, gpa=3.2, scholarship=100, dob=date(2000, 1, 1))
    Student(id=3, name='S3', group=g1, gpa=3.3, scholarship=200, dob=date(2001, 1, 2))

class TestQuery(unittest.TestCase):
    def setUp(self):
        rollback()
        db_session.__enter__()
    def tearDown(self):
        rollback()
        db_session.__exit__()
    @raises_exception(TypeError, 'Cannot iterate over non-entity object')
    def test1(self):
        select(s for s in [])
    @raises_exception(TypeError, 'Cannot iterate over non-entity object X')
    def test2(self):
        X = [1, 2, 3]
        select('x for x in X')
    @raises_exception(TypeError, "Cannot iterate over non-entity object")
    def test3(self):
        g = Group[1]
        select(s for s in g.students)
    @raises_exception(ExprEvalError, "a raises NameError: name 'a' is not defined")
    def test4(self):
        select(a for s in Student)
    @raises_exception(TypeError, "Incomparable types '%s' and 'list' in expression: s.name == x" % unicode.__name__)
    def test5(self):
        x = ['A']
        select(s for s in Student if s.name == x)
    @raises_exception(TypeError, "Function 'f1' cannot be used this way: f1(s.gpa)")
    def test6(self):
        def f1(x):
            return x + 1
        select(s for s in Student if f1(s.gpa) > 3)
    @raises_exception(NotImplementedError, "m1")
    def test7(self):
        class C1(object):
            def method1(self, a, b):
                return a + b
        c = C1()
        m1 = c.method1
        select(s for s in Student if m1(s.gpa, 1) > 3)
    @raises_exception(TypeError, "Expression `x` has unsupported type 'complex'")
    def test8(self):
        x = 1j
        select(s for s in Student if s.gpa == x)
    def test9(self):
        select(g for g in Group for s in db.Student)
    def test10(self):
        avg_gpa = avg(s.gpa for s in Student)
        self.assertEqual(round(avg_gpa, 6), 3.2)
    def test11(self):
        avg_gpa = avg(s.gpa for s in Student if s.id < 0)
        self.assertEqual(avg_gpa, None)
    def test12(self):
        sum_ss = sum(s.scholarship for s in Student)
        self.assertEqual(sum_ss, 300)
    def test13(self):
        sum_ss = sum(s.scholarship for s in Student if s.id < 0)
        self.assertEqual(sum_ss, 0)
    @raises_exception(TypeError, "'avg' is valid for numeric attributes only")
    def test14(self):
        avg(s.name for s in Student)
    def wrapper(self):
        return count(s for s in Student if s.scholarship > 0)
    def test15(self):
        c = self.wrapper()
        c = self.wrapper()
        self.assertEqual(c, 2)
    def test16(self):
        c = count(s.scholarship for s in Student if s.scholarship > 0)
        self.assertEqual(c, 2)
    def test17(self):
        s = get(s.scholarship for s in Student if s.id == 3)
        self.assertEqual(s, 200)
    def test18(self):
        s = get(s.scholarship for s in Student if s.id == 4)
        self.assertEqual(s, None)
    def test19(self):
        s = select(s for s in Student if s.id == 4).exists()
        self.assertEqual(s, False)
    def test20(self):
        r = min(s.scholarship for s in Student)
        self.assertEqual(r, 100)
    def test21(self):
        r = min(s.scholarship for s in Student if s.id < 2)
        self.assertEqual(r, None)
    def test22(self):
        r = max(s.scholarship for s in Student)
        self.assertEqual(r, 200)
    def test23(self):
        r = max(s.dob.year for s in Student)
        self.assertEqual(r, 2001)
    @db_session
    def test_first1(self):
        q = select(s for s in Student).order_by(Student.gpa)
        self.assertEqual(q.first(), Student[1])
    @db_session
    def test_first2(self):
        q = select((s.name, s.group) for s in Student)
        self.assertEqual(q.first(), ('S1', Group[1]))
    @db_session
    def test_first3(self):
        q = select(s for s in Student)
        self.assertEqual(q.first(), Student[1])
    @db_session
    def test_closures_1(self):
        def find_by_gpa(gpa):
            return lambda s: s.gpa > gpa
        fn = find_by_gpa(Decimal('3.1'))
        students = list(Student.select(fn))
        self.assertEqual(students, [ Student[2], Student[3] ])
    @db_session
    def test_closures_2(self):
        def find_by_gpa(gpa):
            return lambda s: s.gpa > gpa
        fn = find_by_gpa(Decimal('3.1'))
        q = select(s for s in Student)
        q = q.filter(fn)
        self.assertEqual(list(q), [ Student[2], Student[3] ])

if __name__ == '__main__':
    unittest.main()
