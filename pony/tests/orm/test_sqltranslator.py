import unittest
from pony.orm import *
from testutils import *

db = TestDatabase('sqlite', ':memory:')

class Student(db.Entity):
    name = Required(unicode)
    group = Required('Group')
    scholarship = Required(int, default=0)
    grades = Set('Grade')

class Group(db.Entity):
    id = PrimaryKey(int)
    students = Set(Student)
    dep = Required(unicode)
    rooms = Set('Room')

class Course(db.Entity):
    name = Required(unicode)
    grades = Set('Grade')

class Grade(db.Entity):
    student = Required(Student)
    course = Required(Course)
    PrimaryKey(student, course)
    value = Required(str)

class Room(db.Entity):
    name = PrimaryKey(unicode)
    groups = Set(Group)

db.generate_mapping(create_tables=True)

@with_transaction
def populate_db():
    g1 = Group(id=1, dep='dep1')
    g2 = Group(id=2, dep='dep2')
    s1 = Student(id=1, name='S1', group=g1, scholarship=0)
    s2 = Student(id=2, name='S2', group=g1, scholarship=100)
    s3 = Student(id=3, name='S3', group=g2, scholarship=500)
    c1 = Course(id=10, name='Math')
    c2 = Course(id=11, name='Economics')
    c3 = Course(id=12, name='Physics')
    Grade(student=s1, course=c1, value='C')
    Grade(student=s1, course=c3, value='A')
    Grade(student=s2, course=c2, value='B')
    r1 = Room(name='Room1')
    r2 = Room(name='Room2')
    r3 = Room(name='Room3')
    g1.rooms = [ r1, r2 ]
    g2.rooms = [ r2, r3 ]
populate_db()

db2 = Database('sqlite', ':memory:')

class Room2(db2.Entity):
    name = PrimaryKey(unicode)


db2.generate_mapping(create_tables=True)

name1 = 'S1'

class TestSQLTranslator(unittest.TestCase):
    def setUp(self):
        rollback()
    def tearDown(self):
        rollback()
    def test_select1(self):
        result = set(select(s for s in Student))
        self.assertEquals(result, set([Student[1], Student[2], Student[3]]))
    def test_select_param(self):        
        result = select(s for s in Student if s.name == name1).all()
        self.assertEquals(result, [Student[1]])
    def test_select_object_param(self):
        stud1 = Student[1]
        result = set(select(s for s in Student if s != stud1))
        self.assertEquals(result, set([Student[2], Student[3]]))
    def test_select_deref(self):
        x = 'S1'
        result = select(s for s in Student if s.name == x).all()
        self.assertEquals(result, [Student[1]])
    def test_select_composite_key(self):
        grade1 = Grade[Student[1], Course[12]]
        result = select(g for g in Grade if g != grade1)
        grades = [ grade.value for grade in result ]
        grades.sort()
        self.assertEquals(grades, ['B', 'C'])
    def test_function_max1(self):
        result = select(s for s in Student if max(s.grades.value) == 'C').all()
        self.assertEquals(result, [Student[1]])
    @raises_exception(TypeError)
    def test_function_max2(self):
        grade1 = Grade[Student[1], Course[12]]
        select(s for s in Student if max(s.grades) == grade1)
    def test_function_min(self):
        result = select(s for s in Student if min(s.grades.value) == 'B').all()
        self.assertEquals(result, [Student[2]])
    @raises_exception(TypeError)
    def test_function_min2(self):
        grade1 = Grade[Student[1], Course[12]]
        select(s for s in Student if min(s.grades) == grade1).all()
    def test_function_len1(self):
        result = select(s for s in Student if len(s.grades) == 1).all()
        self.assertEquals(result, [Student[2]])
    def test_function_len2(self):
        result = select(s for s in Student if max(s.grades.value) == 'C').all()
        self.assertEquals(result, [Student[1]])
    def test_function_sum1(self):
        result = select(g for g in Group if sum(g.students.scholarship) == 100).all()
        self.assertEquals(result, [Group[1]])
    @raises_exception(TypeError)
    def test_function_sum2(self):
        select(g for g in Group if sum(g.students) == 100).all()
    @raises_exception(TypeError)
    def test_function_sum3(self):
        select(g for g in Group if sum(g.students.name) == 100).all()
    def test_function_abs(self):
        result = select(s for s in Student if abs(s.scholarship) == 100).all()
        self.assertEquals(result, [Student[2]])
    def test_builtin_in_locals(self):
        x = max
        gen = (s for s in Student if x(s.grades.value) == 'C')
        result = select(gen).all()
        self.assertEquals(result, [Student[1]])
        x = min
        result = select(gen).all()
        self.assertEquals(result, [])
    @raises_exception(TranslationError, "Name 'g' must be defined in query")
    def test_name(self):
        select(s for s in Student for g in g.subjects).all()
    def test_chain1(self):
        result = set(select(g for g in Group for s in g.students if s.name.endswith('3')))
        self.assertEquals(result, set([Group[2]]))
    def test_chain_m2m(self):
        result = set(select(g for g in Group for r in g.rooms if r.name == 'Room2'))
        self.assertEquals(result, set([Group[1], Group[2]]))
    @raises_exception(TranslationError, 'All entities in a query must belong to the same database')
    def test_two_diagrams(self):
        select(g for g in Group for r in Room2 if r.name == 'Room2').all()
    def test_add_sub_mul_etc(self):
        result = select(s for s in Student if ((-s.scholarship + 200) * 10 / 5 - 100) ** 2 == 10000 or 5 == 2).all()
        self.assertEquals(result, [Student[2]])
    def test_subscript(self):
        result = set(select(s for s in Student if s.name[1] == '2'))
        self.assertEquals(result, set([Student[2]]))
    def test_slice(self):
        result = set(select(s for s in Student if s.name[:1] == 'S'))
        self.assertEquals(result, set([Student[3], Student[2], Student[1]]))        
    def test_attr_chain(self):
        s1 = Student[1]
        result = select(s for s in Student if s == s1).all()
        self.assertEquals(result, [Student[1]])
        result = select(s for s in Student if not s == s1).all()
        self.assertEquals(result, [Student[2], Student[3]])        
        result = select(s for s in Student if s.group == s1.group).all()
        self.assertEquals(result, [Student[1], Student[2]])
        result = select(s for s in Student if s.group.dep == s1.group.dep).all()
        self.assertEquals(result, [Student[1], Student[2]])
    def test_list_monad1(self):
        result = select(s for s in Student if s.name in ['S1']).all()
        self.assertEquals(result, [Student[1]])
    def test_list_monad2(self):
        result = select(s for s in Student if s.name not in ['S1', 'S2']).all()
        self.assertEquals(result, [Student[3]])
    def test_list_monad3(self):
        grade1 = Grade[Student[1], Course[12]]
        grade2 = Grade[Student[1], Course[10]]
        result = set(select(g for g in Grade if g in [grade1, grade2]))
        self.assertEquals(result, set([grade1, grade2]))
        result = set(select(g for g in Grade if g not in [grade1, grade2]))
        self.assertEquals(result, set([Grade[Student[2], Course[11]]]))
    def test_tuple_monad1(self):
        n1 = 'S1'
        n2 = 'S2'
        result = select(s for s in Student if s.name in (n1, n2)).all()
        self.assertEquals(result, [Student[1], Student[2]])
    def test_None_value(self):
        result = select(s for s in Student if s.name is None).all()
        self.assertEquals(result, [])
    def test_None_value2(self):
        result = select(s for s in Student if None == s.name).all()
        self.assertEquals(result, [])
    def test_None_value3(self):
        n = None
        result = select(s for s in Student if s.name == n).all()
        self.assertEquals(result, [])        
    def test_None_value4(self):
        n = None
        result = select(s for s in Student if n == s.name).all()
        self.assertEquals(result, [])   
    @raises_exception(NotImplementedError)        
    def test_expr1(self):
        a = 100
        result = select(a for s in Student).all()
    def test_expr2(self):
        result = set(select(s.group for s in Student))
        self.assertEquals(result, set([Group[1], Group[2]]))
    def test_numeric_binop(self):
        i = 100
        f = 2.0
        result = select(s for s in Student if s.scholarship > i + f).all()
        self.assertEquals(result, [Student[3]])
    def test_string_const_monad(self):
        result = select(s for s in Student if len(s.name) > len('ABC')).all()
        self.assertEquals(result, [])
    def test_numeric_to_bool1(self):
        result = set(select(s for s in Student if s.name != 'John' or s.scholarship))
        self.assertEquals(result, set([Student[1], Student[2], Student[3]]))
    def test_numeric_to_bool2(self):
        result = set(select(s for s in Student if not s.scholarship))
        self.assertEquals(result, set([Student[1]]))
    def test_not_monad1(self):
        result = set(select(s for s in Student if not (s.scholarship > 0 and s.name != 'S1')))
        self.assertEquals(result, set([Student[1]]))
    def test_not_monad2(self):
        result = set(select(s for s in Student if not not (s.scholarship > 0 and s.name != 'S1')))
        self.assertEquals(result, set([Student[2], Student[3]]))
    def test_subquery_with_attr(self):
        result = select(s for s in Student if max(g.value for g in s.grades) == 'A').all()
        self.assertEquals(result, set([Student[1]]))
    def test_query_reuse(self):
        q = select(s for s in Student if s.scholarship > 0)
        q.count()
        self.assert_("ORDER BY" not in db.last_sql.upper())
        q.all() # should not throw exception, query can be reused
        self.assert_(True)
        
       
if __name__ == "__main__":
    unittest.main()
