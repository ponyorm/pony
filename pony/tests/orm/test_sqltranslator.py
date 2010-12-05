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
            scholarship = Required(int, default=0)
            grades = Set('Grade')
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
              scholarship integer not null default 0
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
            insert into Students values (1, 'S1', 0);
            insert into Students values (2, 'S2', 100);
            insert into Students values (3, 'S3', 500);
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
##    def test_select_deref(self):
##        Student = self.diagram.entities["Student"]
##        x = 'S1'
##        result = select(s for s in Student if s.name == x)
##        names = [ stud.name for stud in result ]        
##        self.assertEquals(names, ['S1'])
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
        

if __name__ == "__main__":
    unittest.main()