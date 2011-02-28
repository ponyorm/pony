import unittest
from pony.orm import *
from pony.db import Database
from pony.sqltranslator import select
import pony.db

class TestFuncMonad(unittest.TestCase):
    def setUp(self):
        _diagram_ = Diagram()
        self.diagram = _diagram_        
        class Student(Entity):
            name = Required(unicode)
            age = Required(int)
        self.Student = Student
        self.db = Database('sqlite', ':memory:')
        con = self.db.get_connection()
        con.executescript("""
        drop table if exists Student;
        create table Student(
            id integer primary key,
            name varchar(20),
            age integer
        );
        insert into Student values (1, "Joe", 18);
        insert into Student values (2, "Bob", 19);
        insert into Student values (3, "Beth", 20);
        insert into Student values (4, "Jon", 20);
        insert into Student values (5, "Pete", 18);
        """)
        generate_mapping(self.db,  check_tables=True)
    def test1(self):
        Student = self.Student
        result = set(select(s for s in Student if max(s.age, 18) == 18 ))
        self.assertEquals(result, set([Student(1), Student(5)]))
    
        
if __name__ == '__main__':
    unittest.main()
        