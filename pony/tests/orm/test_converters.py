import unittest
from pony.orm import *
from decimal import Decimal
from datetime import date

class TestConverters(unittest.TestCase):
    def setUp(self):
        _diagram_ = Diagram()
        self.diagram = _diagram_        
        class Student(Entity):
            name = Required(unicode)
            scholarship = Required(Decimal, 5, 2)
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
            date = Required(date)
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
            date datetime,
            primary key (student, subject)
        );
        insert into Student values (1, "Joe", 99.9, 41);
        insert into Student values (2, "Bob", 100.0, 41);
        insert into Student values (3, "Beth", 500.5, 41);
        insert into Student values (4, "Jon", 500.6, 42);
        insert into Student values (5, "Pete", 700.1, 42);
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
        insert into Mark values (5, 1, "Math", "2010-10-01");
        insert into Mark values (4, 2, "Physics", "2010-10-02");
        insert into Mark values (3, 2, "Math", "2010-10-03");
        insert into Mark values (2, 2, "History", "2010-10-04");
        insert into Mark values (1, 3, "History", "2010-10-05");
        insert into Mark values (2, 3, "Math", "2010-10-06");
        insert into Mark values (2, 4, "Math", "2010-10-07");
        """)
        self.db.generate_mapping(check_tables=True)
    def test1(self):
        Student = self.Student
        result = set(select(s.scholarship for s in Student if min(s.marks.value) < 2))
        self.assertEquals(result, set([Decimal("500.5")]))
##    def test2(self):
##        Group = self.Group
##        Student = self.Student
##        #result = set(select(s.scholarship for s in Student if min(s.scholarship) < Decimal("100")))
##        #result = set(select(s.scholarship for s in Student if s.marks.date == date(2010, 10, 2)))
##        self.assertEquals(result, set([]))
        
if __name__ == '__main__':
    unittest.main()
        