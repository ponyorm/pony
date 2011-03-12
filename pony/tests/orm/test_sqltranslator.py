import unittest
from pony.orm import *
from pony.sqltranslator import select, TranslationError
from pony.db import Database
from testutils import *

name1 = 'S1'

class TestSQLTranslator(unittest.TestCase):
    def setUp(self):
        _diagram_ = Diagram()
        self.diagram = _diagram_
        self.db = Database('sqlite', ':memory:')
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
        self.Student = Student
        self.Group = Group
        self.Course = Course
        self.Grade = Grade
        self.Room = Room
        conn = self.db.get_connection()
        conn.executescript("""
            drop table if exists Student;
            create table Student(
              id integer primary key,
              name varchar(50) not null,
              "group" integer not null,
              scholarship integer not null default 0
            );
            drop table if exists "Group";
            create table "Group"(
              id integer primary key,
              dep varchar(20)
            );
            drop table if exists Course;
            create table Course(
              id integer primary key,
              name varchar(50) not null
            );
            drop table if exists Grade;
            create table Grade(
              student integer not null references Students(id),
              course integer not null references Courses(id),
              value varchar(2) not null,
              primary key(student, course)
            );
            drop table if exists Room;
            create table Room(
              name varchar(20) primary key
            );
            drop table if exists Group_Room;
            create table Group_Room (
              "group" integer,
              room varchar(20),
              primary key("group", room)
            );
            insert into Student values (1, 'S1', 1, 0);
            insert into Student values (2, 'S2', 1, 100);
            insert into Student values (3, 'S3', 2, 500);
            insert into "Group" values (1, 'dep1');
            insert into "Group" values (2, 'dep2');
            insert into Course values (10, 'Math');
            insert into Course values (11, 'Economics');
            insert into Course values (12, 'Physics');
            insert into Grade values (1, 10, 'C');
            insert into Grade values (1, 12, 'A');
            insert into Grade values (2, 11, 'B');
            insert into Room values ('Room1');
            insert into Room values ('Room2');
            insert into Room values ('Room3');
            insert into Group_Room values (1, 'Room1');
            insert into Group_Room values (1, 'Room2');
            insert into Group_Room values (2, 'Room2');
            insert into Group_Room values (2, 'Room3');
        """)
        generate_mapping(self.db, check_tables=True)
    def test_select1(self):
        Student = self.Student
        result = set(select(s for s in Student))
        self.assertEquals(result, set([Student(1), Student(2), Student(3)]))
    def test_select_param(self):        
        Student = self.Student
        result = select(s for s in Student if s.name == name1).fetch()
        self.assertEquals(result, [Student(1)])
    def test_select_object_param(self):
        Student = self.Student
        stud1 = Student(1)
        result = set(select(s for s in Student if s != stud1))
        self.assertEquals(result, set([Student(2), Student(3)]))
    def test_select_deref(self):
        Student = self.Student
        x = 'S1'
        result = select(s for s in Student if s.name == x).fetch()
        self.assertEquals(result, [Student(1)])
    def test_select_composite_key(self):
        Grade = self.Grade
        Student = self.Student
        Course = self.Course
        grade1 = Grade(Student(1), Course(12))
        result = select(g for g in Grade if g != grade1)
        grades = [ grade.value for grade in result ]
        grades.sort()
        self.assertEquals(grades, ['B', 'C'])
    def test_function_max1(self):
        Student = self.Student
        result = select(s for s in Student if max(s.grades.value) == 'C').fetch()
        self.assertEquals(result, [Student(1)])
    @raises_exception(TypeError)
    def test_function_max2(self):
        Grade = self.Grade
        Student = self.Student
        Course = self.Course
        grade1 = Grade(Student(1), Course(12))
        select(s for s in Student if max(s.grades) == grade1)
    def test_function_min(self):
        Student = self.Student
        result = select(s for s in Student if min(s.grades.value) == 'B').fetch()
        self.assertEquals(result, [Student(2)])
    @raises_exception(TypeError)
    def test_function_min2(self):
        Grade = self.Grade
        Student = self.Student
        Course = self.Course
        grade1 = Grade(Student(1), Course(12))
        select(s for s in Student if min(s.grades) == grade1).fetch()
    def test_function_len1(self):
        Student = self.Student
        result = select(s for s in Student if len(s.grades) == 1).fetch()
        self.assertEquals(result, [Student(2)])
    def test_function_len2(self):
        Student = self.Student
        result = select(s for s in Student if max(s.grades.value) == 'C').fetch()
        self.assertEquals(result, [Student(1)])
    def test_function_sum1(self):
        Group = self.Group
        result = select(g for g in Group if sum(g.students.scholarship) == 100).fetch()
        self.assertEquals(result, [Group(1)])
    @raises_exception(TypeError)
    def test_function_sum2(self):
        Group = self.Group
        select(g for g in Group if sum(g.students) == 100).fetch()
    @raises_exception(TypeError)
    def test_function_sum3(self):
        Group = self.Group
        select(g for g in Group if sum(g.students.name) == 100).fetch()
    def test_function_abs(self):
        Student = self.Student
        result = select(s for s in Student if abs(s.scholarship) == 100).fetch()
        self.assertEquals(result, [Student(2)])
    def test_builtin_in_locals(self):
        x = max
        Student = self.Student
        gen = (s for s in Student if x(s.grades.value) == 'C')
        result = select(gen).fetch()
        self.assertEquals(result, [Student(1)])
        x = min
        result = select(gen).fetch()
        self.assertEquals(result, [])
    @raises_exception(TranslationError, "Name 'g' must be defined in query")
    def test_name(self):
        Student = self.Student
        select(s for s in Student for g in g.subjects).fetch()
    def test_chain1(self):
        Group = self.Group
        result = set(select(g for g in Group for s in g.students if s.name.endswith('3')))
        self.assertEquals(result, set([Group(2)]))
    def test_chain_m2m(self):
        Group = self.Group
        result = set(select(g for g in Group for r in g.rooms if r.name == 'Room2'))
        self.assertEquals(result, set([Group(1), Group(2)]))
    @raises_exception(TranslationError, 'All entities in a query must belong to the same diagram')
    def test_two_diagrams(self):
        Group = self.Group
        _diagram_ = Diagram()
        class Room(Entity):
            name = PrimaryKey(unicode)
        generate_mapping(self.db, check_tables=True)
        select(g for g in Group for r in Room if r.name == 'Room2').fetch()
    def test_add_sub_mul_etc(self):
        Student = self.Student
        result = select(s for s in Student if ((-s.scholarship + 200) * 10 / 5 - 100) ** 2 == 10000 or 5 == 2).fetch()
        self.assertEquals(result, [Student(2)])
    def test_subscript(self):
        Student = self.Student
        result = set(select(s for s in Student if s.name[1] == '2'))
        self.assertEquals(result, set([Student(2)]))
    def test_slice(self):
        Student = self.Student
        result = set(select(s for s in Student if s.name[:1] == 'S'))
        self.assertEquals(result, set([Student(3), Student(2), Student(1)]))        
    def test_attr_chain(self):
        Student = self.Student
        s1 = Student(1)
        result = select(s for s in Student if s == s1).fetch()
        self.assertEquals(result, [Student(1)])
        result = select(s for s in Student if not s == s1).fetch()
        self.assertEquals(result, [Student(2), Student(3)])        
        result = select(s for s in Student if s.group == s1.group).fetch()
        self.assertEquals(result, [Student(1), Student(2)])
        result = select(s for s in Student if s.group.dep == s1.group.dep).fetch()
        self.assertEquals(result, [Student(1), Student(2)])
    def test_list_monad1(self):
        Student = self.Student
        result = select(s for s in Student if s.name in ['S1']).fetch()
        self.assertEquals(result, [Student(1)])
    def test_list_monad2(self):
        Student = self.Student
        result = select(s for s in Student if s.name not in ['S1', 'S2']).fetch()
        self.assertEquals(result, [Student(3)])
    def test_list_monad3(self):
        Grade = self.Grade
        Student = self.Student
        Course = self.Course
        Group = self.Group
        grade1 = Grade(Student(1), Course(12))
        grade2 = Grade(Student(1), Course(10))
        result = set(select(g for g in Grade if g in [grade1, grade2]))
        self.assertEquals(result, set([grade1, grade2]))
        result = set(select(g for g in Grade if g not in [grade1, grade2]))
        self.assertEquals(result, set([Grade(Student(2), Course(11))]))
    def test_tuple_monad1(self):
        Student = self.Student
        n1 = 'S1'
        n2 = 'S2'
        result = select(s for s in Student if s.name in (n1, n2)).fetch()
        self.assertEquals(result, [Student(1), Student(2)])
    def test_None_value(self):
        Student = self.Student
        result = select(s for s in Student if s.name is None).fetch()
        self.assertEquals(result, [])
    def test_None_value2(self):
        Student = self.Student
        result = select(s for s in Student if None == s.name).fetch()
        self.assertEquals(result, [])
    def test_None_value3(self):
        Student = self.Student
        n = None
        result = select(s for s in Student if s.name == n).fetch()
        self.assertEquals(result, [])        
    def test_None_value4(self):
        Student = self.Student
        n = None
        result = select(s for s in Student if n == s.name).fetch()
        self.assertEquals(result, [])   
    @raises_exception(NotImplementedError)        
    def test_expr1(self):
        Student = self.Student
        a = 100
        result = select(a for s in Student).fetch()
    def test_expr2(self):
        Student = self.Student
        Group = self.Group
        result = set(select(s.group for s in Student))
        self.assertEquals(result, set([Group(1), Group(2)]))
    def test_numeric_binop(self):
        Student = self.Student
        i = 100
        f = 2.0
        result = select(s for s in Student if s.scholarship > i + f).fetch()
        self.assertEquals(result, [Student(3)])
    def test_string_const_monad(self):
        Student = self.Student
        result = select(s for s in Student if len(s.name) > len('ABC')).fetch()
        self.assertEquals(result, [])
    def test_numeric_to_bool1(self):
        Student = self.Student
        result = set(select(s for s in Student if s.name != 'John' or s.scholarship))
        self.assertEquals(result, set([Student(1), Student(2), Student(3)]))
    def test_numeric_to_bool2(self):
        Student = self.Student
        result = set(select(s for s in Student if not s.scholarship))
        self.assertEquals(result, set([Student(1)]))
    def test_not_monad1(self):
        Student = self.Student
        result = set(select(s for s in Student if not (s.scholarship > 0 and s.name != 'S1')))
        self.assertEquals(result, set([Student(1)]))
    def test_not_monad2(self):
        Student = self.Student
        result = set(select(s for s in Student if not not (s.scholarship > 0 and s.name != 'S1')))
        self.assertEquals(result, set([Student(2), Student(3)]))
       
if __name__ == "__main__":
    unittest.main()