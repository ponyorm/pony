from __future__ import with_statement

import unittest
from datetime import date
from decimal import Decimal
from pony.orm.core import *
from pony.orm.sqltranslation import IncomparableTypesError
from testutils import *

db = Database('sqlite', ':memory:')

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

db.generate_mapping(create_tables=True)

with db_session:
    d1 = Department(number=44)
    g1 = Group(number=101, dept=d1)
    Student(name='S1', group=g1)
    Student(name='S2', group=g1)
    Student(name='S3', group=g1)

class TestSQLTranslatorExceptions(unittest.TestCase):
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
    @raises_exception(TranslationError, "Inside declarative query, iterator must be entity. Got: for i in x")
    def test2(self):
        x = [1, 2, 3]
        select(s for s in Student for i in x)
    @raises_exception(TranslationError, "Inside declarative query, iterator must be entity. Got: for s2 in g.students")
    def test3(self):
        g = Group[101]
        select(s for s in Student for s2 in g.students)
    @raises_exception(NotImplementedError, "*args is not supported")
    def test4(self):
        args = 'abc'
        select(s for s in Student if s.name.upper(*args))
    @raises_exception(TypeError, "Expression {'a':'b', 'c':'d'} has unsupported type 'dict'")
    def test5(self):
        select(s for s in Student if s.name.upper(**{'a':'b', 'c':'d'}))
    @raises_exception(ExprEvalError, "1 in 2 raises TypeError: argument of type 'int' is not iterable")
    def test6(self):
        select(s for s in Student if 1 in 2)
    @raises_exception(NotImplementedError, 'Group[s.group.number]')
    def test7(self):
        select(s for s in Student if Group[s.group.number].dept.number == 44)
    # @raises_exception(TypeError, "Invalid count of attrs in Group primary key (2 instead of 1)")
    # def test8(self):
    #     select(s for s in Student if Group[s.group.number, 123].dept.number == 44)
    @raises_exception(ExprEvalError, "Group[123, 456].dept.number == 44 raises TypeError: Invalid count of attrs in Group primary key (2 instead of 1)")
    def test9(self):
        select(s for s in Student if Group[123, 456].dept.number == 44)
    @raises_exception(ExprEvalError, "Course[123] raises TypeError: Invalid count of attrs in Course primary key (1 instead of 2)")
    def test10(self):
        select(s for s in Student if Course[123] in s.courses)
    @raises_exception(TypeError, "Incomparable types 'unicode' and 'float' in expression: s.name < s.gpa")
    def test11(self):
        select(s for s in Student if s.name < s.gpa)
    @raises_exception(ExprEvalError, "Group(101) raises TypeError: Group constructor accept only keyword arguments. Got: 1 positional argument")
    def test12(self):
        select(s for s in Student if s.group == Group(101))
    @raises_exception(ExprEvalError, "Group[date(2011, 1, 2)] raises TypeError: Value type for attribute Group.number must be int. Got: <type 'datetime.date'>")
    def test13(self):
        select(s for s in Student if s.group == Group[date(2011, 1, 2)])
    @raises_exception(TypeError, "Unsupported operand types 'int' and 'unicode' for operation '+' in expression: s.group.number + s.name")
    def test14(self):
        select(s for s in Student if s.group.number + s.name < 0)
    @raises_exception(TypeError, "Unsupported operand types 'Decimal' and 'float' for operation '+' in expression: s.scholarship + 1.1")
    def test15(self):
        select(s for s in Student if s.scholarship + 1.1 > 10)
    @raises_exception(TypeError, "Unsupported operand types 'Decimal' and 'AsciiStr' for operation '**' in expression: s.scholarship ** 'abc'")
    def test16(self):
        select(s for s in Student if s.scholarship ** 'abc' > 10)
    @raises_exception(TypeError, "Unsupported operand types 'unicode' and 'int' for operation '+' in expression: s.name + 2")
    def test17(self):
        select(s for s in Student if s.name + 2 > 10)
    @raises_exception(TypeError, "Step is not supported in s.name[1:3:5]")
    def test18(self):
        select(s for s in Student if s.name[1:3:5] == 'A')
    @raises_exception(TypeError, "Invalid type of start index (expected 'int', got 'AsciiStr') in string slice s.name['a':1]")
    def test19(self):
        select(s for s in Student if s.name['a':1] == 'A')
    @raises_exception(TypeError, "Invalid type of stop index (expected 'int', got 'AsciiStr') in string slice s.name[1:'a']")
    def test20(self):
        select(s for s in Student if s.name[1:'a'] == 'A')
    @raises_exception(NotImplementedError, "Negative indices are not supported in string slice s.name[-1:1]")
    def test21(self):
        select(s for s in Student if s.name[-1:1] == 'A')
    @raises_exception(TypeError, "String indices must be integers. Got 'AsciiStr' in expression s.name['a']")
    def test22(self):
        select(s.name for s in Student if s.name['a'] == 'h')
    @raises_exception(TypeError, "Incomparable types 'int' and 'unicode' in expression: 1 in s.name")
    def test23(self):
        select(s.name for s in Student if 1 in s.name)
    @raises_exception(TypeError, "Expected 'unicode' argument but got 'int' in expression s.name.startswith(1)")
    def test24(self):
        select(s.name for s in Student if s.name.startswith(1))
    @raises_exception(TypeError, "Expected 'unicode' argument but got 'int' in expression s.name.endswith(1)")
    def test25(self):
        select(s.name for s in Student if s.name.endswith(1))
    @raises_exception(TypeError, "'chars' argument must be of 'unicode' type in s.name.strip(1), got: 'int'")
    def test26(self):
        select(s.name for s in Student if s.name.strip(1))
    @raises_exception(AttributeError, "s.group.foo")
    def test27(self):
        select(s.name for s in Student if s.group.foo.bar == 10)
    @raises_exception(ExprEvalError, "g.dept.foo.bar raises AttributeError: 'Department' object has no attribute 'foo'")
    def test28(self):
        g = Group[101]
        select(s for s in Student if s.name == g.dept.foo.bar)
    @raises_exception(ExprEvalError, "date('2011', 1, 1) raises TypeError: an integer is required")
    def test29(self):
        select(s for s in Student if s.dob < date('2011', 1, 1))
    @raises_exception(NotImplementedError, "date(s.id, 1, 1)")
    def test30(self):
        select(s for s in Student if s.dob < date(s.id, 1, 1))
    @raises_exception(ExprEvalError, "max() raises TypeError: max expected 1 arguments, got 0")
    def test31(self):
        select(s for s in Student if s.id < max())
    #@raises_exception(TypeError, "Value of type 'buffer' is not valid as argument of 'max' function in expression max(x, y)")
    # def test32(self):
    #     x = buffer('a')
    #     y = buffer('b')
    #    select(s for s in Student if max(x, y) == x)
    # @raises_exception(TypeError, "Incomparable types 'int' and 'AsciiStr' in expression: min(1, 'a')")
    # def test33(self):
    #     select(s for s in Student if min(1, 'a') == 1)
    # @raises_exception(TypeError, "Incomparable types 'AsciiStr' and 'int' in expression: min('a', 1)")
    # def test33a(self):
    #     select(s for s in Student if min('a', 1) == 1)
    # @raises_exception(TypeError, "'select' function expects generator expression, got: select('* from Students')")
    # def test34(self):
    #    select(s for s in Student if s.group in select("* from Students"))
    # @raises_exception(TypeError, "'exists' function expects generator expression or collection, got: exists('g for g in Group')")
    # def test35(self): ###
    #    select(s for s in Student if exists("g for g in Group"))
    @raises_exception(TypeError, "Incomparable types 'Student' and 'Course' in expression: s in s.courses")
    def test36(self):
        select(s for s in Student if s in s.courses)
    @raises_exception(AttributeError, "s.courses.name.foo")
    def test37(self):
        select(s for s in Student if 'x' in s.courses.name.foo.bar)
    @raises_exception(AttributeError, "s.courses.foo")
    def test38(self):
        select(s for s in Student if 'x' in s.courses.foo.bar)
    @raises_exception(TypeError, "Function sum() expects query or items of numeric type, got 'unicode' in sum(s.courses.name)")
    def test39(self):
        select(s for s in Student if sum(s.courses.name) > 10)
    @raises_exception(TypeError, "Function sum() expects query or items of numeric type, got 'unicode' in sum(c.name for c in s.courses)")
    def test40(self):
        select(s for s in Student if sum(c.name for c in s.courses) > 10)
    @raises_exception(TypeError, "Function sum() expects query or items of numeric type, got 'unicode' in sum(c.name for c in s.courses)")
    def test41(self):
        select(s for s in Student if sum(c.name for c in s.courses) > 10)
    @raises_exception(TypeError, "Function avg() expects query or items of numeric type, got 'unicode' in avg(c.name for c in s.courses)")
    def test42(self):
        select(s for s in Student if avg(c.name for c in s.courses) > 10 and len(s.courses) > 1)
    @raises_exception(TypeError, "strip() takes at most 1 argument (3 given)")
    def test43(self):
        select(s for s in Student if s.name.strip(1, 2, 3))
    @raises_exception(ExprEvalError, "len(1, 2) == 3 raises TypeError: len() takes exactly one argument (2 given)")
    def test44(self):
        select(s for s in Student if len(1, 2) == 3)
    # @raises_exception(NotImplementedError, "Group[101].students")
    # def test45(self):
    #     select(s for s in Student if s in Group[101].students)
    @raises_exception(TypeError, "Function sum() expects query or items of numeric type, got 'Student' in sum(s for s in Student if s.group == g)")
    def test46(self):
        select(g for g in Group if sum(s for s in Student if s.group == g) > 1)
    @raises_exception(TypeError, "Function avg() expects query or items of numeric type, got 'Student' in avg(s for s in Student if s.group == g)")
    def test47(self):
        select(g for g in Group if avg(s for s in Student if s.group == g) > 1)
    @raises_exception(TypeError, "Function min() cannot be applied to type 'Student' in min(s for s in Student if s.group == g)")
    def test48(self):
        select(g for g in Group if min(s for s in Student if s.group == g) > 1)
    @raises_exception(TypeError, "Function max() cannot be applied to type 'Student' in max(s for s in Student if s.group == g)")
    def test49(self):
        select(g for g in Group if max(s for s in Student if s.group == g) > 1)
    # @raises_exception(TypeError, "Incomparable types 'Decimal' and 'bool' in expression: s.scholarship == (True or False and not True)")
    # def test50(self):
    #     select(s for s in Student if s.scholarship == (True or False and not True))
    @raises_exception(IncomparableTypesError, "Incomparable types 'unicode' and 'int' in expression: s.name > +3")
    def test51(self): ###
        select(s for s in Student if s.name > +3)
    @raises_exception(TypeError, "Expression {'a':'b'} has unsupported type 'dict'")
    def test52(self):
        select(s for s in Student if s.name == {'a' : 'b'})
    @raises_exception(IncomparableTypesError, "Incomparable types 'unicode' and 'int' in expression: s.name > a ^ 2")
    def test53(self): ###
        a = 1
        select(s for s in Student if s.name > a ^ 2)
    @raises_exception(IncomparableTypesError, "Incomparable types 'unicode' and 'int' in expression: s.name > a | 2")
    def test54(self): ###
        a = 1
        select(s for s in Student if s.name > a | 2)
    @raises_exception(IncomparableTypesError, "Incomparable types 'unicode' and 'int' in expression: s.name > a & 2")
    def test55(self):
        a = 1
        select(s for s in Student if s.name > a & 2)
    @raises_exception(IncomparableTypesError, "Incomparable types 'unicode' and 'int' in expression: s.name > a << 2")
    def test56(self): ###
        a = 1
        select(s for s in Student if s.name > a << 2)
    @raises_exception(IncomparableTypesError, "Incomparable types 'unicode' and 'int' in expression: s.name > a >> 2")
    def test57(self): ###
        a = 1
        select(s for s in Student if s.name > a >> 2)
    @raises_exception(IncomparableTypesError, "Incomparable types 'unicode' and 'int' in expression: s.name > (a * 2) % 4")
    def test58(self): ###
        a = 1
        select(s for s in Student if s.name > a * 2 % 4)
    @raises_exception(IncomparableTypesError, "Incomparable types 'unicode' and 'int' in expression: s.name > ~a")
    def test59(self): ###
        a = 1
        select(s for s in Student if s.name > ~a)
    @raises_exception(TypeError, "Incomparable types 'unicode' and 'int' in expression: s.name > 1 / a - 3")
    def test60(self):
        a = 1
        select(s for s in Student if s.name > 1 / a - 3)
    @raises_exception(TypeError, "Incomparable types 'unicode' and 'int' in expression: s.name > -a")
    def test61(self):
        a = 1
        select(s for s in Student if s.name > -a)
    @raises_exception(TypeError, "Incomparable types 'unicode' and 'list' in expression: s.name == [1, (2,)]")
    def test62(self):
        select(s for s in Student if s.name == [1, (2,)])

if __name__ == '__main__':
    unittest.main()
