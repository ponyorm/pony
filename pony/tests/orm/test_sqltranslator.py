import unittest
from pony.orm import *
from testutils import *

class Student(Entity):
    name = Required(unicode)
    group = Required('Group')
    scholarship = Required(int, default=0)
    grades = Set('Grade')

class Group(Entity):
    id = PrimaryKey(int)
    students = Set(Student)
    dep = Required(unicode)
    rooms = Set('Room')

class Course(Entity):
    name = Required(unicode)
    grades = Set('Grade')

class Grade(Entity):
    student = Required(Student)
    course = Required(Course)
    PrimaryKey(student, course)
    value = Required(str)

class Room(Entity):
    name = PrimaryKey(unicode)
    groups = Set(Group)

db = Database('sqlite', ':memory:')
db.generate_mapping(create_tables=True)

@with_transaction
def populate_db():
    g1 = Group.create(1, dep='dep1')
    g2 = Group.create(2, dep='dep2')
    s1 = Student.create(1, name='S1', group=g1, scholarship=0)
    s2 = Student.create(2, name='S2', group=g1, scholarship=100)
    s3 = Student.create(3, name='S3', group=g2, scholarship=500)
    c1 = Course.create(10, name='Math')
    c2 = Course.create(11, name='Economics')
    c3 = Course.create(12, name='Physics')
    Grade.create(s1, c1, value='C')
    Grade.create(s1, c3, value='A')
    Grade.create(s2, c2, value='B')
    r1 = Room.create('Room1')
    r2 = Room.create('Room2')
    r3 = Room.create('Room3')
    g1.rooms = [ r1, r2 ]
    g2.rooms = [ r2, r3 ]
populate_db()


_diagram_ = Diagram()

class Room2(Entity):
    name = PrimaryKey(unicode)

db2 = Database('sqlite', ':memory:')
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
        result = select(s for s in Student if s.name == name1).fetch()
        self.assertEquals(result, [Student[1]])
    def test_select_object_param(self):
        stud1 = Student[1]
        result = set(select(s for s in Student if s != stud1))
        self.assertEquals(result, set([Student[2], Student[3]]))
    def test_select_deref(self):
        x = 'S1'
        result = select(s for s in Student if s.name == x).fetch()
        self.assertEquals(result, [Student[1]])
    def test_select_composite_key(self):
        grade1 = Grade[Student[1], Course[12]]
        result = select(g for g in Grade if g != grade1)
        grades = [ grade.value for grade in result ]
        grades.sort()
        self.assertEquals(grades, ['B', 'C'])
    def test_function_max1(self):
        result = select(s for s in Student if max(s.grades.value) == 'C').fetch()
        self.assertEquals(result, [Student[1]])
    @raises_exception(TypeError)
    def test_function_max2(self):
        grade1 = Grade[Student[1], Course[12]]
        select(s for s in Student if max(s.grades) == grade1)
    def test_function_min(self):
        result = select(s for s in Student if min(s.grades.value) == 'B').fetch()
        self.assertEquals(result, [Student[2]])
    @raises_exception(TypeError)
    def test_function_min2(self):
        grade1 = Grade[Student[1], Course[12]]
        select(s for s in Student if min(s.grades) == grade1).fetch()
    def test_function_len1(self):
        result = select(s for s in Student if len(s.grades) == 1).fetch()
        self.assertEquals(result, [Student[2]])
    def test_function_len2(self):
        result = select(s for s in Student if max(s.grades.value) == 'C').fetch()
        self.assertEquals(result, [Student[1]])
    def test_function_sum1(self):
        result = select(g for g in Group if sum(g.students.scholarship) == 100).fetch()
        self.assertEquals(result, [Group[1]])
    @raises_exception(TypeError)
    def test_function_sum2(self):
        select(g for g in Group if sum(g.students) == 100).fetch()
    @raises_exception(TypeError)
    def test_function_sum3(self):
        select(g for g in Group if sum(g.students.name) == 100).fetch()
    def test_function_abs(self):
        result = select(s for s in Student if abs(s.scholarship) == 100).fetch()
        self.assertEquals(result, [Student[2]])
    def test_builtin_in_locals(self):
        x = max
        gen = (s for s in Student if x(s.grades.value) == 'C')
        result = select(gen).fetch()
        self.assertEquals(result, [Student[1]])
        x = min
        result = select(gen).fetch()
        self.assertEquals(result, [])
    @raises_exception(TranslationError, "Name 'g' must be defined in query")
    def test_name(self):
        select(s for s in Student for g in g.subjects).fetch()
    def test_chain1(self):
        result = set(select(g for g in Group for s in g.students if s.name.endswith('3')))
        self.assertEquals(result, set([Group[2]]))
    def test_chain_m2m(self):
        result = set(select(g for g in Group for r in g.rooms if r.name == 'Room2'))
        self.assertEquals(result, set([Group[1], Group[2]]))
    @raises_exception(TranslationError, 'All entities in a query must belong to the same diagram')
    def test_two_diagrams(self):
        select(g for g in Group for r in Room2 if r.name == 'Room2').fetch()
    def test_add_sub_mul_etc(self):
        result = select(s for s in Student if ((-s.scholarship + 200) * 10 / 5 - 100) ** 2 == 10000 or 5 == 2).fetch()
        self.assertEquals(result, [Student[2]])
    def test_subscript(self):
        result = set(select(s for s in Student if s.name[1] == '2'))
        self.assertEquals(result, set([Student[2]]))
    def test_slice(self):
        result = set(select(s for s in Student if s.name[:1] == 'S'))
        self.assertEquals(result, set([Student[3], Student[2], Student[1]]))        
    def test_attr_chain(self):
        s1 = Student[1]
        result = select(s for s in Student if s == s1).fetch()
        self.assertEquals(result, [Student[1]])
        result = select(s for s in Student if not s == s1).fetch()
        self.assertEquals(result, [Student[2], Student[3]])        
        result = select(s for s in Student if s.group == s1.group).fetch()
        self.assertEquals(result, [Student[1], Student[2]])
        result = select(s for s in Student if s.group.dep == s1.group.dep).fetch()
        self.assertEquals(result, [Student[1], Student[2]])
    def test_list_monad1(self):
        result = select(s for s in Student if s.name in ['S1']).fetch()
        self.assertEquals(result, [Student[1]])
    def test_list_monad2(self):
        result = select(s for s in Student if s.name not in ['S1', 'S2']).fetch()
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
        result = select(s for s in Student if s.name in (n1, n2)).fetch()
        self.assertEquals(result, [Student[1], Student[2]])
    def test_None_value(self):
        result = select(s for s in Student if s.name is None).fetch()
        self.assertEquals(result, [])
    def test_None_value2(self):
        result = select(s for s in Student if None == s.name).fetch()
        self.assertEquals(result, [])
    def test_None_value3(self):
        n = None
        result = select(s for s in Student if s.name == n).fetch()
        self.assertEquals(result, [])        
    def test_None_value4(self):
        n = None
        result = select(s for s in Student if n == s.name).fetch()
        self.assertEquals(result, [])   
    @raises_exception(NotImplementedError)        
    def test_expr1(self):
        a = 100
        result = select(a for s in Student).fetch()
    def test_expr2(self):
        result = set(select(s.group for s in Student))
        self.assertEquals(result, set([Group[1], Group[2]]))
    def test_numeric_binop(self):
        i = 100
        f = 2.0
        result = select(s for s in Student if s.scholarship > i + f).fetch()
        self.assertEquals(result, [Student[3]])
    def test_string_const_monad(self):
        result = select(s for s in Student if len(s.name) > len('ABC')).fetch()
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
       
if __name__ == "__main__":
    unittest.main()
