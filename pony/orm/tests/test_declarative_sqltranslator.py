from __future__ import absolute_import, print_function, division
from pony.py23compat import PY2

import unittest
from datetime import date

from pony.orm.core import *
from pony.orm.tests.testutils import *
from pony.orm.tests import setup_database, teardown_database

db = Database()

class Department(db.Entity):
    number = PrimaryKey(int)
    groups = Set('Group')
    courses = Set('Course')

class Student(db.Entity):
    name = Required(unicode)
    group = Required('Group')
    scholarship = Required(int, default=0)
    picture = Optional(buffer)
    courses = Set('Course')
    grades = Set('Grade')

class Group(db.Entity):
    id = PrimaryKey(int)
    students = Set(Student)
    dept = Required(Department)
    rooms = Set('Room')

class Course(db.Entity):
    dept = Required(Department)
    name = Required(unicode)
    credits = Optional(int)
    semester = Required(int)
    PrimaryKey(name, semester)
    grades = Set('Grade')
    students = Set(Student)

class Grade(db.Entity):
    student = Required(Student)
    course = Required(Course)
    PrimaryKey(student, course)
    value = Required(str)
    date = Optional(date)
    teacher = Required('Teacher')

class Teacher(db.Entity):
    name = Required(unicode)
    grades = Set(Grade)

class Room(db.Entity):
    name = PrimaryKey(unicode)
    groups = Set(Group)


db2 = Database()

class Room2(db2.Entity):
    name = PrimaryKey(unicode)

name1 = 'S1'


class TestSQLTranslator(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_database(db)
        with db_session:
            d1 = Department(number=44)
            d2 = Department(number=43)
            g1 = Group(id=1, dept=d1)
            g2 = Group(id=2, dept=d2)
            s1 = Student(id=1, name='S1', group=g1, scholarship=0)
            s2 = Student(id=2, name='S2', group=g1, scholarship=100)
            s3 = Student(id=3, name='S3', group=g2, scholarship=500)
            c1 = Course(name='Math', semester=1, dept=d1)
            c2 = Course(name='Economics', semester=1, dept=d1, credits=3)
            c3 = Course(name='Physics', semester=2, dept=d2)
            t1 = Teacher(id=101, name="T1")
            t2 = Teacher(id=102, name="T2")
            Grade(student=s1, course=c1, value='C', teacher=t2, date=date(2011, 1, 1))
            Grade(student=s1, course=c3, value='A', teacher=t1, date=date(2011, 2, 1))
            Grade(student=s2, course=c2, value='B', teacher=t1)
            r1 = Room(name='Room1')
            r2 = Room(name='Room2')
            r3 = Room(name='Room3')
            g1.rooms = [r1, r2]
            g2.rooms = [r2, r3]
            c1.students.add(s1)
            c1.students.add(s2)
            c2.students.add(s2)
        setup_database(db2)
    @classmethod
    def tearDownClass(cls):
        teardown_database(db)
        teardown_database(db2)
    def setUp(self):
        rollback()
        db_session.__enter__()
    def tearDown(self):
        rollback()
        db_session.__exit__()
    def test_select1(self):
        result = set(select(s for s in Student))
        self.assertEqual(result, {Student[1], Student[2], Student[3]})
    def test_select_param(self):
        result = select(s for s in Student if s.name == name1)[:]
        self.assertEqual(result, [Student[1]])
    def test_select_object_param(self):
        stud1 = Student[1]
        result = set(select(s for s in Student if s != stud1))
        self.assertEqual(result, {Student[2], Student[3]})
    def test_select_deref(self):
        x = 'S1'
        result = select(s for s in Student if s.name == x)[:]
        self.assertEqual(result, [Student[1]])
    def test_select_composite_key(self):
        grade1 = Grade[Student[1], Course['Physics', 2]]
        result = select(g for g in Grade if g != grade1)
        grades = [ grade.value for grade in result ]
        grades.sort()
        self.assertEqual(grades, ['B', 'C'])
    def test_function_max1(self):
        result = select(s for s in Student if max(s.grades.value) == 'C')[:]
        self.assertEqual(result, [Student[1]])
    @raises_exception(TypeError)
    def test_function_max2(self):
        grade1 = Grade[Student[1], Course['Physics', 2]]
        select(s for s in Student if max(s.grades) == grade1)
    def test_function_min(self):
        result = select(s for s in Student if min(s.grades.value) == 'B')[:]
        self.assertEqual(result, [Student[2]])
    @raises_exception(TypeError)
    def test_function_min2(self):
        grade1 = Grade[Student[1], Course['Physics', 2]]
        select(s for s in Student if min(s.grades) == grade1)
    def test_min3(self):
        d = date(2011, 1, 1)
        result = set(select(g for g in Grade if min(g.date, d) == d and g.date is not None))
        self.assertEqual(result, {Grade[Student[1], Course[u'Math', 1]], Grade[Student[1], Course[u'Physics', 2]]})
    def test_function_len1(self):
        result = select(s for s in Student if len(s.grades) == 1)[:]
        self.assertEqual(result, [Student[2]])
    def test_function_len2(self):
        result = select(s for s in Student if max(s.grades.value) == 'C')[:]
        self.assertEqual(result, [Student[1]])
    def test_function_sum1(self):
        result = select(g for g in Group if sum(g.students.scholarship) == 100)[:]
        self.assertEqual(result, [Group[1]])
    def test_function_avg1(self):
        result = select(g for g in Group if avg(g.students.scholarship) == 50)[:]
        self.assertEqual(result, [Group[1]])
    @raises_exception(TypeError)
    def test_function_sum2(self):
        select(g for g in Group if sum(g.students) == 100)
    @raises_exception(TypeError)
    def test_function_sum3(self):
        select(g for g in Group if sum(g.students.name) == 100)
    def test_function_abs(self):
        result = select(s for s in Student if abs(s.scholarship) == 100)[:]
        self.assertEqual(result, [Student[2]])
    def test_builtin_in_locals(self):
        x = max
        gen = (s.group for s in Student if x(s.grades.value) == 'C')
        result = select(gen)[:]
        self.assertEqual(result, [Group[1]])
        x = min
        result = select(gen)[:]
        self.assertEqual(result, [])
    # @raises_exception(TranslationError, "Name 'g' must be defined in query")
    # def test_name(self):
    #     select(s for s in Student for g in g.subjects)
    def test_chain1(self):
        result = set(select(g for g in Group for s in g.students if s.name.endswith('3')))
        self.assertEqual(result, {Group[2]})
    def test_chain2(self):
        result = set(select(s for g in Group if g.dept.number == 44 for s in g.students if s.name.startswith('S')))
        self.assertEqual(result, {Student[1], Student[2]})
    def test_chain_m2m(self):
        result = set(select(g for g in Group for r in g.rooms if r.name == 'Room2'))
        self.assertEqual(result, {Group[1], Group[2]})
    @raises_exception(TranslationError, 'All entities in a query must belong to the same database')
    def test_two_diagrams(self):
        select(g for g in Group for r in Room2 if r.name == 'Room2')
    def test_add_sub_mul_etc(self):
        result = select(s for s in Student if ((-s.scholarship + 200) * 10 / 5 - 100) ** 2 == 10000 or 5 == 2)[:]
        self.assertEqual(result, [Student[2]])
    def test_subscript(self):
        result = set(select(s for s in Student if s.name[1] == '2'))
        self.assertEqual(result, {Student[2]})
    def test_slice(self):
        result = set(select(s for s in Student if s.name[:1] == 'S'))
        self.assertEqual(result, {Student[3], Student[2], Student[1]})
    def test_attr_chain(self):
        s1 = Student[1]
        result = select(s for s in Student if s == s1)[:]
        self.assertEqual(result, [Student[1]])
        result = select(s for s in Student if not s == s1)[:]
        self.assertEqual(result, [Student[2], Student[3]])
        result = select(s for s in Student if s.group == s1.group)[:]
        self.assertEqual(result, [Student[1], Student[2]])
        result = select(s for s in Student if s.group.dept == s1.group.dept)[:]
        self.assertEqual(result, [Student[1], Student[2]])
    def test_list_monad1(self):
        result = select(s for s in Student if s.name in ['S1'])[:]
        self.assertEqual(result, [Student[1]])
    def test_list_monad2(self):
        result = select(s for s in Student if s.name not in ['S1', 'S2'])[:]
        self.assertEqual(result, [Student[3]])
    def test_list_monad3(self):
        grade1 = Grade[Student[1], Course['Physics', 2]]
        grade2 = Grade[Student[1], Course['Math', 1]]
        result = set(select(g for g in Grade if g in [grade1, grade2]))
        self.assertEqual(result, {grade1, grade2})
        result = set(select(g for g in Grade if g not in [grade1, grade2]))
        self.assertEqual(result, {Grade[Student[2], Course['Economics', 1]]})
    def test_tuple_monad1(self):
        n1 = 'S1'
        n2 = 'S2'
        result = select(s for s in Student if s.name in (n1, n2))[:]
        self.assertEqual(result, [Student[1], Student[2]])
    def test_None_value(self):
        result = select(s for s in Student if s.name is None)[:]
        self.assertEqual(result, [])
    def test_None_value2(self):
        result = select(s for s in Student if None == s.name)[:]
        self.assertEqual(result, [])
    def test_None_value3(self):
        n = None
        result = select(s for s in Student if s.name == n)[:]
        self.assertEqual(result, [])
    def test_None_value4(self):
        n = None
        result = select(s for s in Student if n == s.name)[:]
        self.assertEqual(result, [])
    @raises_exception(TranslationError, "External parameter 'a' cannot be used as query result")
    def test_expr1(self):
        a = 100
        result = select(a for s in Student)
    def test_expr2(self):
        result = set(select(s.group for s in Student))
        self.assertEqual(result, {Group[1], Group[2]})
    def test_numeric_binop(self):
        i = 100
        f = 2.0
        result = select(s for s in Student if s.scholarship > i + f)[:]
        self.assertEqual(result, [Student[3]])
    def test_string_const_monad(self):
        result = select(s for s in Student if len(s.name) > len('ABC'))[:]
        self.assertEqual(result, [])
    def test_numeric_to_bool1(self):
        result = set(select(s for s in Student if s.name != 'John' or s.scholarship))
        self.assertEqual(result, {Student[1], Student[2], Student[3]})
    def test_numeric_to_bool2(self):
        result = set(select(s for s in Student if not s.scholarship))
        self.assertEqual(result, {Student[1]})
    def test_not_monad1(self):
        result = set(select(s for s in Student if not (s.scholarship > 0 and s.name != 'S1')))
        self.assertEqual(result, {Student[1]})
    def test_not_monad2(self):
        result = set(select(s for s in Student if not not (s.scholarship > 0 and s.name != 'S1')))
        self.assertEqual(result, {Student[2], Student[3]})
    def test_subquery_with_attr(self):
        result = set(select(s for s in Student if max(g.value for g in s.grades) == 'C'))
        self.assertEqual(result, {Student[1]})
    def test_query_reuse(self):
        q = select(s for s in Student if s.scholarship > 0)
        q.count()
        self.assertTrue("ORDER BY" not in db.last_sql.upper())
        objects = q[:] # should not throw exception, query can be reused
    def test_select(self):
        result = Student.select(lambda s: s.scholarship > 0)[:]
        self.assertEqual(result, [Student[2], Student[3]])
    def test_get(self):
        result = Student.get(lambda s: s.scholarship == 500)
        self.assertEqual(result, Student[3])
    def test_order_by(self):
        result = list(Student.select().order_by(Student.name))
        self.assertEqual(result, [Student[1], Student[2], Student[3]])
    def test_read_inside_query(self):
        result = set(select(s for s in Student if Group[1].dept.number == 44))
        self.assertEqual(result, {Student[1], Student[2], Student[3]})
    def test_crud_attr_chain(self):
        result = set(select(s for s in Student if Group[1].dept.number == s.group.dept.number))
        self.assertEqual(result, {Student[1], Student[2]})
    def test_composite_key1(self):
        result = set(select(t for t in Teacher if Grade[Student[1], Course['Physics', 2]] in t.grades))
        self.assertEqual(result, {Teacher.get(name='T1')})
    def test_composite_key2(self):
        result = set(select(s for s in Student if Course['Math', 1] in s.courses))
        self.assertEqual(result, {Student[1], Student[2]})
    def test_composite_key3(self):
        result = set(select(s for s in Student if Course['Math', 1] not in s.courses))
        self.assertEqual(result, {Student[3]})
    def test_composite_key4(self):
        result = set(select(s for s in Student if len(c for c in Course if c not in s.courses) == 2))
        self.assertEqual(result, {Student[1]})
    def test_composite_key5(self):
        result = set(select(s for s in Student if not (c for c in Course if c not in s.courses)))
        self.assertEqual(result, set())
    def test_composite_key6(self):
        result = set(select(c for c in Course if c not in (c2 for s in Student for c2 in s.courses)))
        self.assertEqual(result, {Course['Physics', 2]})
    def test_composite_key7(self):
        result = set(select(c for s in Student for c in s.courses))
        self.assertEqual(result, {Course['Math', 1], Course['Economics', 1]})
    def test_contains1(self):
        s1 = Student[1]
        result = set(select(g for g in Group if s1 in g.students))
        self.assertEqual(result, {Group[1]})
    def test_contains2(self):
        s1 = Student[1]
        result = set(select(g for g in Group if s1.name in g.students.name))
        self.assertEqual(result, {Group[1]})
    def test_contains3(self):
        s1 = Student[1]
        result = set(select(g for g in Group if s1 not in g.students))
        self.assertEqual(result, {Group[2]})
    def test_contains4(self):
        s1 = Student[1]
        result = set(select(g for g in Group if s1.name not in g.students.name))
        self.assertEqual(result, {Group[2]})
    def test_buffer_monad1(self):
        try: select(s for s in Student if s.picture == buffer('abc'))
        except TypeError as e: self.assertTrue(not PY2 and str(e) == 'string argument without an encoding')
        else: self.assertTrue(PY2)
    def test_buffer_monad2(self):
        select(s for s in Student if s.picture == buffer('abc', 'ascii'))
    def test_database_monad(self):
        result = set(select(s for s in db.Student if db.Student[1] == s))
        self.assertEqual(result, {Student[1]})
    def test_duplicate_name(self):
        result = set(select(x for x in Student if x.group in (x for x in Group)))
        self.assertEqual(result, {Student[1], Student[2], Student[3]})
    def test_hint_join1(self):
        result = set(select(s for s in Student if JOIN(max(s.courses.credits) == 3)))
        self.assertEqual(result, {Student[2]})
    def test_hint_join2(self):
        result = set(select(c for c in Course if JOIN(len(c.students) == 1)))
        self.assertEqual(result, {Course['Economics', 1]})
    def test_tuple_param(self):
        x = Student[1], Student[2]
        result = set(select(s for s in Student if s not in x))
        self.assertEqual(result, {Student[3]})
    @raises_exception(TypeError, "Expression `x` should not contain None values")
    def test_tuple_param_2(self):
        x = Student[1], None
        result = set(select(s for s in Student if s not in x))
        self.assertEqual(result, {Student[3]})
    def test_method_monad(self):
        result = set(select(s for s in Student if s not in Student.select(lambda s: s.scholarship > 0)))
        self.assertEqual(result, {Student[1]})
    def test_lambda_1(self):
        q = select(s for s in Student)
        q = q.filter(lambda s: s.name == 'S1')
        self.assertEqual(list(q), [Student[1]])
    def test_lambda_2(self):
        q = select(s for s in Student)
        q = q.filter(lambda stud: stud.name == 'S1')
        self.assertEqual(list(q), [Student[1]])
    def test_lambda_3(self):
        q = select(s for s in Student)
        q = q.filter(lambda stud: exists(x for x in Student if stud.name < x.name))
        self.assertEqual(set(q), {Student[1], Student[2]})
    def test_lambda_4(self):
        q = select(s for s in Student)
        q = q.filter(lambda stud: exists(s for s in Student if stud.name < s.name))
        self.assertEqual(set(q), {Student[1], Student[2]})
    def test_optimized_1(self):
        q = select((g, count(g.students)) for g in Group if count(g.students) > 1)
        self.assertEqual(set(q), {(Group[1], 2)})
    def test_optimized_2(self):
        q = select((s, count(s.courses)) for s in Student if count(s.courses) > 1)
        self.assertEqual(set(q), {(Student[2], 2)})
    def test_delete(self):
        q = select(g for g in Grade if g.teacher.id == 101).delete()
        q2 = select(g for g in Grade)[:]
        self.assertEqual([g.value for g in q2], ['C'])
    def test_delete_2(self):
        delete(g for g in Grade if g.teacher.id == 101)
        q2 = select(g for g in Grade)[:]
        self.assertEqual([g.value for g in q2], ['C'])
    def test_delete_3(self):
        select(g for g in Grade if g.teacher.id == 101).delete(bulk=True)
        q2 = select(g for g in Grade)[:]
        self.assertEqual([g.value for g in q2], ['C'])
    def test_delete_4(self):
        select(g for g in Grade if exists(g2 for g2 in Grade if g2.value > g.value)).delete(bulk=True)
        q2 = select(g for g in Grade)[:]
        self.assertEqual([g.value for g in q2], ['C'])
    def test_select_2(self):
        result = select(s for s in Student)[:]
        self.assertEqual(result, [Student[1], Student[2], Student[3]])
    def test_select_add(self):
        result = [None] + select(s for s in Student)[:]
        self.assertEqual(result, [None, Student[1], Student[2], Student[3]])
    def test_query_result_radd(self):
        result = select(s for s in Student)[:] + [None]
        self.assertEqual(result, [Student[1], Student[2], Student[3], None])
    def test_query_result_sort(self):
        result = select(s for s in Student)[:]
        result.sort()
        self.assertEqual(result, [Student[1], Student[2], Student[3]])
    def test_query_result_reverse(self):
        result = select(s for s in Student)[:]
        items = list(result)
        result.reverse()
        self.assertEqual(items, list(reversed(result)))
    def test_query_result_shuffle(self):
        result = select(s for s in Student)[:]
        items = set(result)
        result.shuffle()
        self.assertEqual(items, set(result))
    def test_query_result_to_list(self):
        result = select(s for s in Student)[:]
        items = result.to_list()
        self.assertTrue(type(items) is list)
    @raises_exception(TypeError, 'In order to do item assignment, cast QueryResult to list first')
    def test_query_result_setitem(self):
        result = select(s for s in Student)[:]
        result[0] = None
    @raises_exception(TypeError, 'In order to do item deletion, cast QueryResult to list first')
    def test_query_result_delitem(self):
        result = select(s for s in Student)[:]
        del result[0]
    @raises_exception(TypeError, 'In order to do +=, cast QueryResult to list first')
    def test_query_result_iadd(self):
        result = select(s for s in Student)[:]
        result += None
    @raises_exception(TypeError, 'In order to do append, cast QueryResult to list first')
    def test_query_result_append(self):
        result = select(s for s in Student)[:]
        result.append(None)
    @raises_exception(TypeError, 'In order to do clear, cast QueryResult to list first')
    def test_query_result_clear(self):
        result = select(s for s in Student)[:]
        result.clear()
    @raises_exception(TypeError, 'In order to do extend, cast QueryResult to list first')
    def test_query_result_extend(self):
        result = select(s for s in Student)[:]
        result.extend([])
    @raises_exception(TypeError, 'In order to do insert, cast QueryResult to list first')
    def test_query_result_insert(self):
        result = select(s for s in Student)[:]
        result.insert(0, None)
    @raises_exception(TypeError, 'In order to do pop, cast QueryResult to list first')
    def test_query_result_pop(self):
        result = select(s for s in Student)[:]
        result.pop()
    @raises_exception(TypeError, 'In order to do remove, cast QueryResult to list first')
    def test_query_result_remove(self):
        result = select(s for s in Student)[:]
        result.remove(None)


if __name__ == "__main__":
    unittest.main()
