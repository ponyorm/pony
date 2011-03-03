import unittest
from pony.orm import *
from pony.sqltranslator import select, TranslationError
from pony.db import Database
from testutils import *

class TestQuerySetMonad(unittest.TestCase):
    def setUp(self):
        _diagram_ = Diagram()
        self.diagram = _diagram_
        self.db = Database('sqlite', ':memory:')
        class Student(Entity):
            name = Required(unicode)
            group = Required('Group')
            scholarship = Required(int, default=0)
        class Group(Entity):
            id = PrimaryKey(int)
            students = Set(Student)
        conn = self.db.get_connection()
        conn.executescript("""
            drop table if exists Student;
            create table Student(
              id integer primary key,
              name varchar(50) not null,
              "group" integer not null,
              scholarship integer default 0 
            );
            drop table if exists "Group";
            create table "Group"(
              id integer primary key
            );            
            insert into Student values (1, 'S1', 1, 0);
            insert into Student values (2, 'S2', 1, 100);
            insert into Student values (3, 'S3', 2, 500);
            insert into "Group" values (1);
            insert into "Group" values (2);
        """)
        generate_mapping(self.db, check_tables=True)
    def test_len(self):
        Group = self.diagram.entities["Group"]
        result = set(select(g for g in Group if len(g.students) > 1))
        self.assertEquals(result, set([Group(1)]))
    def test_len2(self):
        Group = self.diagram.entities["Group"]
        Student = self.diagram.entities["Student"]
        result = set(select(g for g in Group if len(s for s in Student if s.group == g) > 1))
        self.assertEquals(result, set([Group(1)]))
    def test_len3(self):
        Group = self.diagram.entities["Group"]
        Student = self.diagram.entities["Student"]
        result = set(select(g for g in Group if len(s.name for s in Student if s.group == g) > 1))
        self.assertEquals(result, set([Group(1)]))
    @raises_exception(TypeError)
    def test_sum1(self):
        Group = self.diagram.entities["Group"]
        Student = self.diagram.entities["Student"]
        result = set(select(g for g in Group if sum(s for s in Student if s.group == g) > 1))
        self.assertEquals(result, set([]))
    @raises_exception(TypeError)
    def test_sum2(self):
        Group = self.diagram.entities["Group"]
        Student = self.diagram.entities["Student"]
        select(g for g in Group if sum(s.name for s in Student if s.group == g) > 1)
    def test_sum3(self):
        Group = self.diagram.entities["Group"]
        Student = self.diagram.entities["Student"]
        result = set(select(g for g in Group if sum(s.scholarship for s in Student if s.group == g) > 500))
        self.assertEquals(result, set([]))
    def test_min1(self):
        Group = self.diagram.entities["Group"]
        Student = self.diagram.entities["Student"]
        result = set(select(g for g in Group if min(s.name for s in Student if s.group == g) == 'S1'))
        self.assertEquals(result, set([Group(1)]))
    @raises_exception(TypeError)
    def test_min2(self):
        Group = self.diagram.entities["Group"]
        Student = self.diagram.entities["Student"]
        select(g for g in Group if min(s for s in Student if s.group == g) == None)
    def test_max1(self):
        Group = self.diagram.entities["Group"]
        Student = self.diagram.entities["Student"]
        result = set(select(g for g in Group if max(s.scholarship for s in Student if s.group == g) > 100))
        self.assertEquals(result, set([Group(2)]))
    @raises_exception(TypeError)    
    def test_max2(self):
        Group = self.diagram.entities["Group"]
        Student = self.diagram.entities["Student"]
        select(g for g in Group if max(s for s in Student if s.group == g) == None)
    def test_negate(self):
        Group = self.diagram.entities["Group"]
        Student = self.diagram.entities["Student"]
        result = set(select(g for g in Group if not(s.scholarship for s in Student if s.group == g)))
        self.assertEquals(result, set([]))
    def test_no_conditions(self):
        Group = self.diagram.entities["Group"]
        Student = self.diagram.entities["Student"]
        students = set(select(s for s in Student if s.group in (g for g in Group)))
        self.assertEqual(students, set([Student(1), Student(2), Student(3)]))    
if __name__ == "__main__":
    unittest.main()
        