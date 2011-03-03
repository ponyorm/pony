import unittest
from pony.orm import *
from pony.db import Database
from pony.sqltranslator import select, exists
from testutils import *

class TestAttrSetMonad(unittest.TestCase):
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
        drop table if exists OneToOne;
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
        generate_mapping(self.db,  check_tables=True)

    def test1(self):
        Group = self.Group
        groups = select(g for g in Group if len(g.students) > 2).fetch()
        self.assertEqual(groups, [Group(41)])
    def test2(self):
        Group = self.Group
        groups = set(select(g for g in Group if len(g.students.name) >= 2))
        self.assertEqual(groups, set([Group(41), Group(42)]))
    def test3(self):
        Group = self.Group
        groups = select(g for g in Group if len(g.students.marks) > 2).fetch()
        self.assertEqual(groups, [Group(41)])
    def test4(self):
        Group = self.Group
        groups = select(g for g in Group if max(g.students.marks.value) <= 2).fetch()
        self.assertEqual(groups, [Group(42)])
    def test5(self):
        Student = self.Student
        students= select(s for s in Student if len(s.marks.subject.name) > 5).fetch()
        self.assertEqual(students, [])
    def test6(self):
        Student = self.Student
        students = set(select(s for s in Student if len(s.marks.subject) >= 2))
        self.assertEqual(students, set([Student(2), Student(3)]))
    def test8(self):
        Student = self.Student
        Group = self.Group
        students = set(select(s for s in Student if s.group in select(g for g in Group if g.department == 101)))
        self.assertEqual(students, set([Student(1), Student(2), Student(3)]))
    def test9(self):
        Student = self.Student
        Group = self.Group
        students = set(select(s for s in Student if s.group not in select(g for g in Group if g.department == 101)))
        self.assertEqual(students, set([Student(4), Student(5)]))
    def test10(self):
        Student = self.Student
        Group = self.Group
        students = set(select(s for s in Student if s.group in (g for g in Group if g.department == 101)))
        self.assertEqual(students, set([Student(1), Student(2), Student(3)]))
    def test11(self):
        Group = self.Group
        students = set(select(g for g in Group if len(g.subjects.groups.subjects) > 1))
        self.assertEqual(students, set([Group(41), Group(42), Group(43)]))
    def test12(self):
        Group = self.Group
        groups = set(select(g for g in Group if len(g.subjects) >= 2))
        self.assertEqual(groups, set([Group(41), Group(42)]))
    def test13(self):
        Group = self.Group
        groups = set(select(g for g in Group if g.students))
        self.assertEqual(groups, set([Group(41), Group(42)]))
    def test14(self):
        Group = self.Group
        groups = set(select(g for g in Group if not g.students))
        self.assertEqual(groups, set([Group(43)]))
    def test15(self):
        Group = self.Group
        groups = set(select(g for g in Group if exists(g.students)))
        self.assertEqual(groups, set([Group(41), Group(42)]))
    def test15a(self):
        Group = self.Group
        groups = set(select(g for g in Group if not not exists(g.students)))
        self.assertEqual(groups, set([Group(41), Group(42)]))
    def test16(self):
        Group = self.Group
        groups = select(g for g in Group if not exists(g.students)).fetch()
        self.assertEqual(groups, [Group(43)])
    def test17(self):
        Group = self.Group
        groups = set(select(g for g in Group if 100 in g.students.scholarship))
        self.assertEqual(groups, set([Group(41)]))        
    def test18(self):
        Group = self.Group
        groups = set(select(g for g in Group if 100 not in g.students.scholarship))
        self.assertEqual(groups, set([Group(42), Group(43)]))
    def test19(self):
        Group = self.Group
        groups = set(select(g for g in Group if not not not 100 not in g.students.scholarship))
        self.assertEqual(groups, set([Group(41)]))
    def test20(self):
        Group = self.Group
        Student = self.Student
        groups = set(select(g for g in Group if exists(s for s in Student if s.group == g and s.scholarship == 500)))
        self.assertEqual(groups, set([Group(41), Group(42)]))
    def test21(self):
        Group = self.Group
        groups = set(select(g for g in Group if g.department is not None))
        self.assertEqual(groups, set([Group(41), Group(42), Group(43)]))
    def test21a(self):
        Group = self.Group
        groups = set(select(g for g in Group if not g.department is not None))
        self.assertEqual(groups, set([]))
    def test21b(self):
        Group = self.Group
        groups = set(select(g for g in Group if not not not g.department is None))
        self.assertEqual(groups, set([Group(41), Group(42), Group(43)]))   
    def test22(self):
        Group = self.Group
        Student = self.Student
        groups = set(select(g for g in Group if 700 in select(s.scholarship for s in Student if s.group == g)))
        self.assertEqual(groups, set([Group(42)]))
    def test23(self):
        Group = self.Group
        Student = self.Student
        groups = set(select(g for g in Group if 700 not in select(s.scholarship for s in Student if s.group == g)))
        self.assertEqual(groups, set([Group(43)]))
    @raises_exception(NotImplementedError)
    def test24(self):
        Group = self.Group
        groups = set(select(g for g in Group for g2 in Group if g.students == g2.students))
    @raises_exception(NotImplementedError)
    def test25(self):
        Student = self.Student
        Mark = self.Mark
        Subject = self.Subject
        m1 = Mark(Student(1), Subject("Math"))
        marks = select(s for s in Student if m1 in s.marks)
    def test26(self):
        Group = self.Group
        Student = self.Student
        s1 = Student(1)
        groups = set(select(g for g in Group if s1 in g.students))
        self.assertEqual(groups, set([Group(41)]))            
    

if __name__ == "__main__":
    unittest.main()

