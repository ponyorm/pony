import unittest
from pony.orm import *
from pony.sqltranslator import select
from pony.db import Database

name1 = 'S1'
#TODO move inside test after LOAD_DEREF is implemented
stud1 = None
grade1 = None

class TestSQLTranslator(unittest.TestCase):
    def setUp(self):
        _diagram_ = Diagram()
        self.diagram = _diagram_
        class Student(Entity):
            _table_ = 'Students'
            name = Required(unicode)
            group = Required('Group')
            scholarship = Required(int, default=0)
            grades = Set('Grade')
        class Group(Entity):
            _table_ = 'Groups'
            id = PrimaryKey(int)
            students = Set(Student)
        class Course(Entity):
            _table_ = 'Courses'
            name = Required(unicode)
            grades = Set('Grade')
        class Grade(Entity):
            _table_ = 'Grades'
            student = Required(Student)
            course = Required(Course)
            PrimaryKey(student, course)
            value = Required(str)        
        self.db = Database('sqlite', ':memory:')
        conn = self.db.get_connection()
        conn.executescript("""
            drop table if exists Students;
            create table Students(
              id integer primary key,
              name varchar(50) not null,
              "group" integer not null,
              scholarship integer not null default 0
            );
            drop table if exists Groups;
            create table Groups(
              id integer primary key
            );
            drop table if exists Courses;
            create table Courses(
              id integer primary key,
              name varchar(50) not null
            );
            drop table if exists Grades;
            create table Grades(
              student integer not null references Students(id),
              course integer not null references Courses(id),
              value varchar(2) not null,
              primary key(student, course)
            );
            insert into Students values (1, 'S1', 1, 0);
            insert into Students values (2, 'S2', 1, 100);
            insert into Students values (3, 'S3', 2, 500);
            insert into Groups values (1);
            insert into Groups values (2);
            insert into Courses values (10, 'Math');
            insert into Courses values (11, 'Economics');
            insert into Courses values (12, 'Physics');
            insert into Grades values (1, 10, 'C');
            insert into Grades values (1, 12, 'A');
            insert into Grades values (2, 11, 'B');            
        """)
        generate_mapping(self.db, check_tables=True)
        local.trans = Transaction()
    def test_select1(self):
        Student = self.diagram.entities["Student"]
        result = select(s for s in Student)
        names = [ stud.name for stud in result ]
        names.sort()
        self.assertEquals(names, ['S1', 'S2', 'S3'])
    def test_select_numeric_param(self):
        Student = self.diagram.entities["Student"]
        result = select(s for s in Student if s.name == name1)
        names = [ stud.name for stud in result ]        
        self.assertEquals(names, ['S1'])
    def test_select_object_param(self):
        global stud1
        Student = self.diagram.entities["Student"]
        stud1 = Student(1)
        result = select(s for s in Student if s != stud1)
        names = [ stud.name for stud in result ]
        names.sort()
        self.assertEquals(names, ['S2', 'S3'])
    def test_select_deref(self):
        Student = self.diagram.entities["Student"]
        x = 'S1'
        result = select(s for s in Student if s.name == x)
        names = [ stud.name for stud in result ]        
        self.assertEquals(names, ['S1'])
    def test_select_composite_key(self):
        global grade1
        Grade = self.diagram.entities["Grade"]
        Student = self.diagram.entities["Student"]
        Course = self.diagram.entities["Course"]
        grade1 = Grade(Student(1), Course(12))
        result = select(g for g in Grade if g != grade1)
        grades = [ grade.value for grade in result ]
        grades.sort()
        self.assertEquals(grades, ['B', 'C'])
    def test_function_max1(self):
        Student = self.diagram.entities["Student"]
        result = select(s for s in Student if max(s.grades.value) == 'C')
        names = [ stud.name for stud in result ]
        self.assertEquals(names, ['S1'])
    def test_function_max2(self):
        Grade = self.diagram.entities["Grade"]
        Student = self.diagram.entities["Student"]
        Course = self.diagram.entities["Course"]
        grade1 = Grade(Student(1), Course(12))
        try:
            result = select(s for s in Student if max(s.grades) == grade1)
            self.assert_(False)
        except TypeError:
            self.assert_(True)
    def test_function_min(self):
        Student = self.diagram.entities["Student"]
        result = select(s for s in Student if min(s.grades.value) == 'B')
        names = [ stud.name for stud in result ]
        self.assertEquals(names, ['S2'])
    def test_function_min2(self):
        Grade = self.diagram.entities["Grade"]
        Student = self.diagram.entities["Student"]
        Course = self.diagram.entities["Course"]
        grade1 = Grade(Student(1), Course(12))
        try:
            result = select(s for s in Student if min(s.grades) == grade1)
            self.assert_(False)
        except TypeError:
            self.assert_(True)
    def test_function_len1(self):
        Student = self.diagram.entities["Student"]
        result = select(s for s in Student if len(s.grades) == 1)
        names = [ stud.name for stud in result ]
        self.assertEquals(names, ['S2'])
    def test_function_len2(self):
        Student = self.diagram.entities["Student"]
        result = select(s for s in Student if max(s.grades.value) == 'C')
        names = [ stud.name for stud in result ]
        self.assertEquals(names, ['S1'])
    def test_function_sum1(self):
        Group = self.diagram.entities["Group"]
        result = select(g for g in Group if sum(g.students.scholarship) == 100)
        group_ids = [ group.id for group in result ]
        self.assertEquals(group_ids, [1])
    def test_function_sum2(self):
        Group = self.diagram.entities["Group"]
        try:
            result = select(g for g in Group if sum(g.students) == 100)
            self.assert_(False)
        except TypeError:
            self.assert_(True)
    def test_function_sum3(self):
        Group = self.diagram.entities["Group"]
        try:
            result = select(g for g in Group if sum(g.students.name) == 100)
            self.assert_(False)
        except TypeError:
            self.assert_(True)
    def test_function_abs(self):
        Student = self.diagram.entities["Student"]
        result = select(s for s in Student if abs(s.scholarship) == 100)
        names = [ stud.name for stud in result ]
        self.assertEquals(names, ['S2'])
    def test_builtin_in_locals(self):
        x = max
        Student = self.diagram.entities["Student"]
        gen = (s for s in Student if x(s.grades.value) == 'C')
        result = select(gen)
        names = [ stud.name for stud in result ]
        self.assertEquals(names, ['S1'])
        x = min
        result = select(gen)
        names = [ stud.name for stud in result ]
        self.assertEquals(names, [])
       
if __name__ == "__main__":
    unittest.main()