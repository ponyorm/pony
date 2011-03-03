import unittest
from pony.orm import *
from pony.db import Database
from pony.sqltranslator import select
import pony.db

class TestStringMixin(unittest.TestCase):
    def setUp(self):
        _diagram_ = Diagram()
        self.diagram = _diagram_        
        class Student(Entity):
            name = Required(unicode)
        self.Student = Student
        self.db = Database('sqlite', ':memory:')
        con = self.db.get_connection()
        con.executescript("""
        drop table if exists Student;
        create table Student(
            id integer primary key,
            name varchar(20)
        );
        insert into Student values (1, "ABCDEF");
        insert into Student values (2, "Bob");
        insert into Student values (3, "Beth");
        insert into Student values (4, "Jon");
        insert into Student values (5, "Pete");
        """)
        generate_mapping(self.db,  check_tables=True)
    def test1(self):
        Student = self.Student
        name = "ABCDEF5"
        result = set(select(s for s in Student if s.name + "5" == name))
        self.assertEquals(result, set([Student(1)]))
    def test2(self):
        Student = self.Student
        result = set(select(s for s in Student if s.name[0:2] == "ABCDEF"[0:2]))
        self.assertEquals(result, set([Student(1)]))
    def test3(self):
        Student = self.Student
        result = set(select(s for s in Student if s.name[1:100] == "ABCDEF"[1:100]))
        self.assertEquals(result, set([Student(1)]))
    def test4(self):
        Student = self.Student
        result = set(select(s for s in Student if s.name[:] == "ABCDEF"))
        self.assertEquals(result, set([Student(1)]))
    def test5(self):
        Student = self.Student
        result = set(select(s for s in Student if s.name[:3] == "ABCDEF"[0:3]))
        self.assertEquals(result, set([Student(1)]))
    def test6(self):
        Student = self.Student
        x = 4
        result = set(select(s for s in Student if s.name[:x] == "ABCDEF"[:x]))
    def test7(self):
        Student = self.Student
        result = set(select(s for s in Student if s.name[0:] == "ABCDEF"[0:]))
        self.assertEquals(result, set([Student(1)]))
    def test8(self):
        Student = self.Student
        x = 2
        result = set(select(s for s in Student if s.name[x:] == "ABCDEF"[x:]))
        self.assertEquals(result, set([Student(1)]))        
    def test9(self):
        Student = self.Student
        x = 4
        result = set(select(s for s in Student if s.name[0:x] == "ABCDEF"[0:x]))
        self.assertEquals(result, set([Student(1)]))
    def test10(self):
        Student = self.Student
        x = 0
        result = set(select(s for s in Student if s.name[x:3] == "ABCDEF"[x:3]))
        self.assertEquals(result, set([Student(1)]))
    def test11(self):
        Student = self.Student
        x = 1
        y = 4
        result = set(select(s for s in Student if s.name[x:y] == "ABCDEF"[x:y]))
        self.assertEquals(result, set([Student(1)]))
    def test12(self):
        Student = self.Student
        x = 10
        y = 20
        result = set(select(s for s in Student if s.name[x:y] == "ABCDEF"[x:y]))
        self.assertEquals(result, set([Student(1), Student(2), Student(3), Student(4), Student(5)]))        
    def test13(self):
        Student = self.Student
        result = set(select(s for s in Student if s.name[1] == "ABCDEF"[1]))
        self.assertEquals(result, set([Student(1)]))         
    def test14(self):
        Student = self.Student
        x = 1
        result = set(select(s for s in Student if s.name[x] == "ABCDEF"[x]))
        self.assertEquals(result, set([Student(1)]))
    def test15(self):
        Student = self.Student
        x = -1
        result = set(select(s for s in Student if s.name[x] == "ABCDEF"[x]))
        self.assertEquals(result, set([Student(1)]))        
    def test16(self):
        Student = self.Student
        result = set(select(s for s in Student if 'o' in s.name))
        self.assertEquals(result, set([Student(2), Student(4)]))         
    def test17(self):
        Student = self.Student
        x = 'o'
        result = set(select(s for s in Student if x in s.name))
        self.assertEquals(result, set([Student(2), Student(4)]))
     
        
if __name__ == '__main__':
    pony.db.debug = False
    unittest.main()
        