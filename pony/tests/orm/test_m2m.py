import unittest
from pony.orm3 import *
from pony.db import Database
from pony import db

class TestManyToMany(unittest.TestCase):
    def setUp(self):
        _diagram_ = Diagram()
        self.diagram = _diagram_
        class Department(Entity):
            number = PrimaryKey(int)
            name = Required(str)
            groups = Set("Group")
        class Group(Entity):
            department = Required(Department)
            number = Required(int)
            subjects = Set("Subject")
            PrimaryKey(department, number)
        class Subject(Entity):
            name = PrimaryKey(str)
            groups = Set(Group)

        self.db = Database('sqlite', ':memory:')
        conn = self.db.get_connection()
        conn.executescript("""
        create table Department(
            number integer primary key,
            name varchar(20) not null
            );
        create table [Group](
            department_number integer,
            number integer,
            primary key (department_number, number)
            );
        create table Subject(
            name varchar(20) primary key
            );
        create table Group_Subject(
            group_department_number integer,
            group_number integer,
            subject_name varchar(20),
            primary key (group_department_number, group_number, subject_name)
            );
        insert into Department values (1, 'Dep1');
        insert into [Group] values (1, 101);
        insert into [Group] values (1, 102);
        insert into Subject values ('Subj1');
        insert into Subject values ('Subj2');
        insert into Group_Subject values (1, 101, 'Subj1');
        """)
        generate_mapping(self.db, check_tables=True)
        local.trans = Transaction()
    def tearDown(self):
        self.db.release()
    def test_add(self):
        db.debug = True
        Group = self.diagram.entities.get("Group")
        Subject = self.diagram.entities.get("Subject")
        g = Group.find_one(number=101)
        s2 = Subject.find_one('Subj2')
        g.subjects.add(s2)
        get_trans().commit()
    def test_remove(self):
        g = Group.find_one(number=101)
        s1 = Subject.find_one('Subj1')
        g.subjects.add(s1)
        get_trans().commit()

if __name__ == "main":
    unittest.main()
