import unittest
from pony.orm import *
from pony.db import Database
from pony.sqltranslator import select, exists
from testutils import *

class TestOrderbyLimit(unittest.TestCase):
    def setUp(self):
        _diagram_ = Diagram()
        self.diagram = _diagram_
        class Student(Entity):
            name = Required(unicode)
            scholarship = Optional(int)
            group = Required(int)
        self.db = Database('sqlite', ':memory:')
        con = self.db.get_connection()
        con.executescript("""
        drop table if exists Student;
        create table Student(
            id integer primary key,
            name varchar(20),
            scholarship integer,
            [group] integer
        );
        insert into Student values (1, "B", null, 41);
        insert into Student values (2, "C", 700, 41);
        insert into Student values (3, "A", 500, 42);
        insert into Student values (4, "D", 500, 43);
        insert into Student values (5, "E", 700, 42);
        """)
        generate_mapping(self.db,  check_tables=True)

    def test1(self):
        Student = self.diagram.entities.get("Student")
        students = select(s for s in Student).orderby(Student.name).fetch()
        self.assertEqual(students, [Student(3), Student(1), Student(2), Student(4), Student(5)])
    def test2(self):
        Student = self.diagram.entities.get("Student")
        students = select(s for s in Student).orderby(Student.name.asc).fetch()
        self.assertEqual(students, [Student(3), Student(1), Student(2), Student(4), Student(5)])
    def test3(self):
        Student = self.diagram.entities.get("Student")
        students = select(s for s in Student).orderby(Student.id.desc).fetch()
        self.assertEqual(students, [Student(5), Student(4), Student(3), Student(2), Student(1)])
    def test4(self):
        Student = self.diagram.entities.get("Student")
        students = select(s for s in Student).orderby(Student.scholarship.asc, Student.group.desc).fetch()
        self.assertEqual(students, [Student(1), Student(4), Student(3), Student(5), Student(2)])
    def test5(self):
        Student = self.diagram.entities.get("Student")
        students = select(s for s in Student).orderby(Student.name).limit(3).fetch()
        self.assertEqual(students, [Student(3), Student(1), Student(2)])
    def test6(self):
        Student = self.diagram.entities.get("Student")
        students = select(s for s in Student).orderby(Student.name).limit(3, 1).fetch()
        self.assertEqual(students, [Student(1), Student(2), Student(4)])
    def test7(self):
        Student = self.diagram.entities.get("Student")
        query = select(s for s in Student).orderby(Student.name).limit(3, 1)
        students = query.fetch()
        self.assertEqual(students, [Student(1), Student(2), Student(4)])
        students = query.fetch()
        self.assertEqual(students, [Student(1), Student(2), Student(4)])
    @raises_exception(TypeError, "query.orderby() arguments must be attributes. Got: 'name'")
    def test8(self):
        Student = self.diagram.entities.get("Student")
        students = select(s for s in Student).orderby("name").fetch()     
    def test9(self):
        Student = self.diagram.entities.get("Student")
        students = select(s for s in Student).orderby(Student.id)[1:4].fetch()
        self.assertEqual(students, [Student(2), Student(3), Student(4)])
    def test10(self):
        Student = self.diagram.entities.get("Student")
        students = select(s for s in Student).orderby(Student.id)[:4].fetch()
        self.assertEqual(students, [Student(1), Student(2), Student(3), Student(4)])
    @raises_exception(TypeError, "Parameter 'stop' of slice object should be specified")
    def test11(self):
        Student = self.diagram.entities.get("Student")
        students = select(s for s in Student).orderby(Student.id)[4:].fetch()
    @raises_exception(TypeError, "Parameter 'start' of slice object cannot be negative")
    def test12(self):
        Student = self.diagram.entities.get("Student")
        students = select(s for s in Student).orderby(Student.id)[-3:2].fetch()
    def test13(self):
        Student = self.diagram.entities.get("Student")
        students = select(s for s in Student).orderby(Student.id)[3].fetch()
        self.assertEqual(students, [Student(4)])
    @raises_exception(TypeError, "Incorrect argument type: 'a'")
    def test14(self):
        Student = self.diagram.entities.get("Student")
        students = select(s for s in Student).orderby(Student.id)["a"].fetch()        
    def test15(self):
        Student = self.diagram.entities.get("Student")
        students = select(s for s in Student).orderby(Student.id)[0:4][1:3].fetch()
        self.assertEqual(students, [Student(2), Student(3)])       
if __name__ == "__main__":
    unittest.main()