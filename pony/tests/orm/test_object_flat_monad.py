import unittest
from pony.orm import *

class TestObjectFlatMonad(unittest.TestCase):
    def setUp(self):
        _diagram_ = Diagram()
        self.diagram = _diagram_        
        class Student(Entity):
            name = Required(unicode)
            scholarship = Optional(int)
            group = Required("Group")
            marks = Set("Mark")
        class Group(Entity):
            number = PrimaryKey(int)
            department = Required(int)
            students = Set(Student)
            subjects = Set("Subject")
        class Subject(Entity):
            name = PrimaryKey(unicode)
            groups = Set(Group)
            marks = Set("Mark")
        class Mark(Entity):
            value = Required(int)
            student = Required(Student)
            subject = Required(Subject)
            PrimaryKey(student, subject)
        self.Student = Student
        self.Group = Group
        self.Subject = Subject
        self.Mark = Mark
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
        drop table if exists [Group];
        create table [Group](
            number integer primary key,
            department integer
        );
        drop table if exists Subject;
        create table Subject(
            name varchar(20) primary key
        );
        drop table if exists Group_Subject;
        create table Group_Subject(
            [group] integer,
            subject varchar(20),
            primary key ([group], subject)
        );
        drop table if exists Mark;
        create table Mark(
            value integer,
            student integer,
            subject varchar(20),
            primary key (student, subject)
        );
        insert into Student values (1, "Joe", null, 41);
        insert into Student values (2, "Bob", 100, 41);
        insert into Student values (3, "Beth", 500, 41);
        insert into Student values (4, "Jon", 500, 42);
        insert into Student values (5, "Pete", 700, 42);
        insert into [Group] values (41, 101);
        insert into [Group] values (42, 102);
        insert into [Group] values (43, 102);
        insert into Group_Subject values (41, "Math");
        insert into Group_Subject values (41, "Physics");
        insert into Group_Subject values (41, "History");
        insert into Group_Subject values (42, "Math");
        insert into Group_Subject values (42, "Physics");
        insert into Group_Subject values (43, "Physics");
        insert into Subject values ("Math");
        insert into Subject values ("Physics");
        insert into Subject values ("History");
        insert into Mark values (5, 1, "Math");
        insert into Mark values (4, 2, "Physics");
        insert into Mark values (3, 2, "Math");
        insert into Mark values (2, 2, "History");
        insert into Mark values (1, 3, "History");
        insert into Mark values (2, 3, "Math");
        insert into Mark values (2, 4, "Math");
        """)
        self.db.generate_mapping(check_tables=True)
    def test1(self):
        Group = self.Group
        Subject = self.Subject
        result = set(select(s.groups for s in Subject if len(s.name) == 4))
        self.assertEquals(result, set([Group(41), Group(42)]))
    def test2(self):
        Group = self.Group
        Student = self.Student
        result = set(select(g.students for g in Group if g.department == 102))
        self.assertEquals(result, set([Student(5), Student(4)]))

if __name__ == '__main__':
    unittest.main()
        