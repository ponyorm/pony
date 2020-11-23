from __future__ import absolute_import, print_function, division
from pony.py23compat import PYPY, PYPY2

import sys, unittest
from datetime import date
from decimal import Decimal

from pony.orm.core import *
from pony.orm.sqltranslation import IncomparableTypesError
from pony.orm.tests.testutils import *
from pony.orm.tests import setup_database, teardown_database

db = Database()

class Student(db.Entity):
    name = Required(unicode)
    dob = Optional(date)
    gpa = Optional(float)
    scholarship = Optional(Decimal, 7, 2)
    group = Required('Group')
    courses = Set('Course')

class Group(db.Entity):
    number = PrimaryKey(int)
    students = Set(Student)
    dept = Required('Department')

class Department(db.Entity):
    number = PrimaryKey(int)
    groups = Set(Group)

class Course(db.Entity):
    name = Required(unicode)
    semester = Required(int)
    PrimaryKey(name, semester)
    students = Set(Student)


class TestSQLTranslatorExceptions(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_database(db)
        with db_session:
            d1 = Department(number=44)
            g1 = Group(number=101, dept=d1)
            Student(name='S1', group=g1)
            Student(name='S2', group=g1)
            Student(name='S3', group=g1)
    @classmethod
    def tearDownClass(cls):
        teardown_database(db)
    def setUp(self):
        rollback()
        db_session.__enter__()
    def tearDown(self):
        rollback()
        db_session.__exit__()
    @raises_exception(NotImplementedError, 'for x in s.name')
    def test1(self):
        x = 10
        select(s for s in Student for x in s.name)
    @raises_exception(TranslationError, "Inside declarative query, iterator must be entity or query. Got: for i in x")
    def test2(self):
        x = [1, 2, 3]
        select(s for s in Student for i in x)
    @raises_exception(TranslationError, "Inside declarative query, iterator must be entity or query. Got: for s2 in g.students")
    def test3(self):
        g = Group[101]
        select(s for s in Student for s2 in g.students)
    @raises_exception(NotImplementedError, "*args is not supported")
    def test4(self):
        args = 'abc'
        select(s for s in Student if s.name.upper(*args))

    if sys.version_info[:2] < (3, 5): # TODO
        @raises_exception(NotImplementedError) # "**{'a': 'b', 'c': 'd'} is not supported
        def test5(self):
            select(s for s in Student if s.name.upper(**{'a':'b', 'c':'d'}))

    @raises_exception(ExprEvalError, "`1 in 2` raises TypeError: argument of type 'int' is not iterable" if not PYPY else
                                     "`1 in 2` raises TypeError: 'int' object is not iterable")
    def test6(self):
        select(s for s in Student if 1 in 2)
    @raises_exception(NotImplementedError, 'Group[s.group.number]')
    def test7(self):
        select(s for s in Student if Group[s.group.number].dept.number == 44)
    @raises_exception(ExprEvalError, "`Group[123, 456].dept.number == 44` raises TypeError: Invalid count of attrs in Group primary key (2 instead of 1)")
    def test8(self):
        select(s for s in Student if Group[123, 456].dept.number == 44)
    @raises_exception(ExprEvalError, "`Course[123]` raises TypeError: Invalid count of attrs in Course primary key (1 instead of 2)")
    def test9(self):
        select(s for s in Student if Course[123] in s.courses)
    @raises_exception(TypeError, "Incomparable types '%s' and 'float' in expression: s.name < s.gpa" % unicode.__name__)
    def test10(self):
        select(s for s in Student if s.name < s.gpa)
    @raises_exception(ExprEvalError, "`Group(101)` raises TypeError: Group constructor accept only keyword arguments. Got: 1 positional argument")
    def test11(self):
        select(s for s in Student if s.group == Group(101))
    @raises_exception(ExprEvalError, "`Group[date(2011, 1, 2)]` raises TypeError: Value type for attribute Group.number must be int. Got: %r" % date)
    def test12(self):
        select(s for s in Student if s.group == Group[date(2011, 1, 2)])
    @raises_exception(TypeError, "Unsupported operand types 'int' and '%s' for operation '+' in expression: s.group.number + s.name" % unicode.__name__)
    def test13(self):
        select(s for s in Student if s.group.number + s.name < 0)
    @raises_exception(TypeError, "Unsupported operand types 'Decimal' and 'float' for operation '+' in expression: s.scholarship + 1.1")
    def test14(self):
        select(s for s in Student if s.scholarship + 1.1 > 10)
    @raises_exception(TypeError, "Unsupported operand types 'Decimal' and '%s' for operation '**' "
                                 "in expression: s.scholarship ** 'abc'" % unicode.__name__)
    def test15(self):
        select(s for s in Student if s.scholarship ** 'abc' > 10)
    @raises_exception(TypeError, "Unsupported operand types '%s' and 'int' for operation '+' in expression: s.name + 2" % unicode.__name__)
    def test16(self):
        select(s for s in Student if s.name + 2 > 10)
    @raises_exception(TypeError, "Step is not supported in s.name[1:3:5]")
    def test17(self):
        select(s for s in Student if s.name[1:3:5] == 'A')
    @raises_exception(TypeError, "Invalid type of start index (expected 'int', got '%s') in string slice s.name['a':1]"
                                 % unicode.__name__)
    def test18(self):
        select(s for s in Student if s.name['a':1] == 'A')
    @raises_exception(TypeError, "Invalid type of stop index (expected 'int', got '%s') in string slice s.name[1:'a']"
                                 % unicode.__name__)
    def test19(self):
        select(s for s in Student if s.name[1:'a'] == 'A')
    @raises_exception(TypeError, "String indices must be integers. Got '%s' in expression s.name['a']" % unicode.__name__)
    def test21(self):
        select(s.name for s in Student if s.name['a'] == 'h')
    @raises_exception(TypeError, "Incomparable types 'int' and '%s' in expression: 1 in s.name" % unicode.__name__)
    def test22(self):
        select(s.name for s in Student if 1 in s.name)
    @raises_exception(TypeError, "Expected '%s' argument but got 'int' in expression s.name.startswith(1)" % unicode.__name__)
    def test23(self):
        select(s.name for s in Student if s.name.startswith(1))
    @raises_exception(TypeError, "Expected '%s' argument but got 'int' in expression s.name.endswith(1)" % unicode.__name__)
    def test24(self):
        select(s.name for s in Student if s.name.endswith(1))
    @raises_exception(TypeError, "'chars' argument must be of '%s' type in s.name.strip(1), got: 'int'" % unicode.__name__)
    def test25(self):
        select(s.name for s in Student if s.name.strip(1))
    @raises_exception(AttributeError, "'%s' object has no attribute 'unknown': s.name.unknown" % unicode.__name__)
    def test26(self):
        result = set(select(s for s in Student if s.name.unknown() == "joe"))
    @raises_exception(AttributeError, "Entity Group does not have attribute foo: s.group.foo")
    def test27(self):
        select(s.name for s in Student if s.group.foo.bar == 10)
    @raises_exception(ExprEvalError, "`g.dept.foo.bar` raises AttributeError: 'Department' object has no attribute 'foo'")
    def test28(self):
        g = Group[101]
        select(s for s in Student if s.name == g.dept.foo.bar)
    @raises_exception(TypeError, "'year' argument of date(year, month, day) function must be of 'int' type. "
                                 "Got: '%s'" % unicode.__name__)
    def test29(self):
        select(s for s in Student if s.dob < date('2011', 1, 1))
    @raises_exception(NotImplementedError, "date(s.id, 1, 1)")
    def test30(self):
        select(s for s in Student if s.dob < date(s.id, 1, 1))
    @raises_exception(
        ExprEvalError,
        "`max()` raises TypeError: max() expects at least one argument" if PYPY else
        "`max()` raises TypeError: max expected 1 arguments, got 0" if sys.version_info[:2] < (3, 8) else
        "`max()` raises TypeError: max expected 1 argument, got 0" if sys.version_info[:2] < (3, 9) else
        "`max()` raises TypeError: max expected at least 1 argument, got 0")
    def test31(self):
        select(s for s in Student if s.id < max())
    @raises_exception(TypeError, "Incomparable types 'Student' and 'Course' in expression: s in s.courses")
    def test32(self):
        select(s for s in Student if s in s.courses)
    @raises_exception(AttributeError, "s.courses.name.foo")
    def test33(self):
        select(s for s in Student if 'x' in s.courses.name.foo.bar)
    @raises_exception(AttributeError, "s.courses.foo")
    def test34(self):
        select(s for s in Student if 'x' in s.courses.foo.bar)
    @raises_exception(TypeError, "Function sum() expects query or items of numeric type, got '%s' in sum(s.courses.name)" % unicode.__name__)
    def test35(self):
        select(s for s in Student if sum(s.courses.name) > 10)
    @raises_exception(TypeError, "Function sum() expects query or items of numeric type, got '%s' in sum(c.name for c in s.courses)" % unicode.__name__)
    def test36(self):
        select(s for s in Student if sum(c.name for c in s.courses) > 10)
    @raises_exception(TypeError, "Function sum() expects query or items of numeric type, got '%s' in sum(c.name for c in s.courses)" % unicode.__name__)
    def test37(self):
        select(s for s in Student if sum(c.name for c in s.courses) > 10)
    @raises_exception(TypeError, "Function avg() expects query or items of numeric type, got '%s' in avg(c.name for c in s.courses)" % unicode.__name__)
    def test38(self):
        select(s for s in Student if avg(c.name for c in s.courses) > 10 and len(s.courses) > 1)
    @raises_exception(TypeError, "strip() takes at most 1 argument (3 given)")
    def test39(self):
        select(s for s in Student if s.name.strip(1, 2, 3))
    @raises_exception(ExprEvalError,
                      "`len(1, 2) == 3` raises TypeError: len() takes exactly 1 argument (2 given)" if PYPY2 else
                      "`len(1, 2) == 3` raises TypeError: len() takes 1 positional argument but 2 were given" if PYPY else
                      "`len(1, 2) == 3` raises TypeError: len() takes exactly one argument (2 given)")
    def test40(self):
        select(s for s in Student if len(1, 2) == 3)
    @raises_exception(TypeError, "Function sum() expects query or items of numeric type, got 'Student' in sum(s for s in Student if s.group == g)")
    def test41(self):
        select(g for g in Group if sum(s for s in Student if s.group == g) > 1)
    @raises_exception(TypeError, "Function avg() expects query or items of numeric type, got 'Student' in avg(s for s in Student if s.group == g)")
    def test42(self):
        select(g for g in Group if avg(s for s in Student if s.group == g) > 1)
    @raises_exception(TypeError, "Function min() cannot be applied to type 'Student' in min(s for s in Student if s.group == g)")
    def test43(self):
        select(g for g in Group if min(s for s in Student if s.group == g) > 1)
    @raises_exception(TypeError, "Function max() cannot be applied to type 'Student' in max(s for s in Student if s.group == g)")
    def test44(self):
        select(g for g in Group if max(s for s in Student if s.group == g) > 1)
    @raises_exception(TypeError, "Attribute should be specified for 'max' aggregate function")
    def test45(self):
        max(s for s in Student)
    @raises_exception(TypeError, "Single attribute should be specified for 'max' aggregate function")
    def test46(self):
        max((s.name, s.gpa) for s in Student)
    @raises_exception(TypeError, "Attribute should be specified for 'sum' aggregate function")
    def test47(self):
        sum(s for s in Student)
    @raises_exception(TypeError, "Single attribute should be specified for 'sum' aggregate function")
    def test48(self):
        sum((s.name, s.gpa) for s in Student)
    @raises_exception(TypeError, "'sum' is valid for numeric attributes only")
    def test49(self):
        sum(s.name for s in Student)
    @raises_exception(TypeError, "Cannot compare whole JSON value, you need to select specific sub-item: s.name == {'a':'b'}")
    def test50(self):
        # cannot compare JSON value to dynamic string,
        # because a database does not provide json.dumps(s.name) functionality
        select(s for s in Student if s.name == {'a': 'b'})
    @raises_exception(IncomparableTypesError, "Incomparable types '%s' and 'int' in expression: s.name > a & 2" % unicode.__name__)
    def test51(self):
        a = 1
        select(s for s in Student if s.name > a & 2)
    @raises_exception(TypeError, "Incomparable types '%s' and 'float' in expression: s.name > 1 / a - 3" % unicode.__name__)
    def test52(self):
        a = 1
        select(s for s in Student if s.name > 1 / a - 3)
    @raises_exception(TypeError, "Incomparable types '%s' and 'int' in expression: s.name > 1 // a - 3" % unicode.__name__)
    def test53(self):
        a = 1
        select(s for s in Student if s.name > 1 // a - 3)
    @raises_exception(TypeError, "Incomparable types '%s' and 'int' in expression: s.name > -a" % unicode.__name__)
    def test54(self):
        a = 1
        select(s for s in Student if s.name > -a)
    @raises_exception(TypeError, "Incomparable types '%s' and 'list' in expression: s.name == [1, (2,)]" % unicode.__name__)
    def test55(self):
        select(s for s in Student if s.name == [1, (2,)])
    @raises_exception(TypeError, "Delete query should be applied to a single entity. Got: (s, g)")
    def test56(self):
        delete((s, g) for g in Group for s in g.students if s.gpa > 3)

if __name__ == '__main__':
    unittest.main()
