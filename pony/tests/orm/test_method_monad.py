import unittest
from pony.orm import *
from pony.db import Database
from pony.sqltranslator import select, exists
from testutils import *

class TestMethodMonad(unittest.TestCase):
    def setUp(self):
        _diagram_ = Diagram()
        self.diagram = _diagram_
        class Student(Entity):
            name = Required(unicode)
            scholarship = Optional(int)
        self.db = Database('sqlite', ':memory:')
        con = self.db.get_connection()
        con.executescript("""
        drop table if exists Student;
        create table Student(
            id integer primary key,
            name varchar(20),
            scholarship integer
        );
        insert into Student values (1, "Joe", null);
        insert into Student values (2, " Bob ", 100);
        insert into Student values (3, " Beth ", 500);
        insert into Student values (4, "Jon", 500);
        insert into Student values (5, "Pete", 700);
        """)
        generate_mapping(self.db,  check_tables=True)

    def test1(self):
        Student = self.diagram.entities.get("Student")
        students = set(select(s for s in Student if not s.name.startswith('J')))
        self.assertEqual(students, set([Student(2), Student(3), Student(5)]))
    def test1a(self):
        Student = self.diagram.entities.get("Student")
        x = "Pe"
        students = set(select(s for s in Student if s.name.startswith(x)))
        self.assertEqual(students, set([Student(5)]))            
    def test2(self):
        Student = self.diagram.entities.get("Student")
        students = set(select(s for s in Student if s.name.endswith('e')))
        self.assertEqual(students, set([Student(1), Student(5)]))
    def test2a(self):
        Student = self.diagram.entities.get("Student")
        x = "te"
        students = set(select(s for s in Student if s.name.endswith(x)))
        self.assertEqual(students, set([Student(5)]))
    def test3(self):
        Student = self.diagram.entities.get("Student")
        students = set(select(s for s in Student if s.name.strip() == 'Beth'))
        self.assertEqual(students, set([Student(3)]))
    @raises_exception(TypeError, "'chars' argument must be a unicode")        
    def test3a(self):
        Student = self.diagram.entities.get("Student")
        students = set(select(s for s in Student if s.name.strip(5) == 'Beth'))
    def test4(self):
        Student = self.diagram.entities.get("Student")
        students = set(select(s for s in Student if s.name.rstrip('n') == 'Jo'))
        self.assertEqual(students, set([Student(4)]))        
    def test5(self):
        Student = self.diagram.entities.get("Student")
        students = set(select(s for s in Student if s.name.lstrip('P') == 'ete'))
        self.assertEqual(students, set([Student(5)]))
    @raises_exception(TypeError, "Argument of 'startswith' method must be a string")        
    def test6(self):
        Student = self.diagram.entities.get("Student")
        students = set(select(s for s in Student if not s.name.startswith(5)))      
    @raises_exception(TypeError, "Argument of 'endswith' method must be a string")        
    def test7(self):
        Student = self.diagram.entities.get("Student")
        students = set(select(s for s in Student if not s.name.endswith(5)))
    

if __name__ == "__main__":
    unittest.main()