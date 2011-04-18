import unittest
from pony.orm import *

class TestManyToManyNonComposite(unittest.TestCase):
    def setUp(self):
        _diagram_ = Diagram()
        self.diagram = _diagram_
        class Group(Entity):
            number = PrimaryKey(int)
            subjects = Set("Subject")
        class Subject(Entity):
            name = PrimaryKey(str)
            groups = Set(Group)

        self.db = Database('sqlite', ':memory:')
        conn = self.db.get_connection()
        conn.executescript("""
        drop table if exists [Group];
        create table [Group](
            number integer primary key
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
        insert into [Group] values (101);
        insert into [Group] values (102);
        insert into Subject values ('Subj1');
        insert into Subject values ('Subj2');
        insert into Subject values ('Subj3');
        insert into Subject values ('Subj4');
        insert into Group_Subject values (101, 'Subj1');
        insert into Group_Subject values (101, 'Subj2');
        """)
        self.db.generate_mapping(check_tables=True)
        # local.session = DBSession()
    def test_add_remove(self):
        Group = self.diagram.entities.get("Group")
        Subject = self.diagram.entities.get("Subject")
        g = Group.find_one(101)
        subjects = Subject.find_all()
        g.subjects.remove(subjects[:2])
        g.subjects.add(subjects[-2:])
        commit()
        self.assertEqual(Group(101).subjects, set([Subject('Subj3'), Subject('Subj4')]))
        db_subjects = self.db.select("subject from Group_Subject where [group] = 101")
        self.assertEqual(db_subjects , ['Subj3', 'Subj4'])
#    def test_set_load(self):
#        # TODO
#        pass


#class TestManyToManyComposite(unittest.TestCase):
#    def setUp(self):
#        _diagram_ = Diagram()
#        self.diagram = _diagram_
#        class Department(Entity):
#            number = PrimaryKey(int)
#            name = Required(str)
#            groups = Set("Group")
#        class Group(Entity):
#            department = Required(Department)
#            number = Required(int)
#            subjects = Set("Subject")
#            PrimaryKey(department, number)
#        class Subject(Entity):
#            name = PrimaryKey(str)
#            groups = Set(Group)
#
#        self.db = Database('sqlite', ':memory:')
#        conn = self.db.get_connection()
#        conn.executescript("""
#        drop table if exists Department;
#        create table Department(
#            number integer primary key,
#            name varchar(20) not null
#            );
#        drop table if exists [Group];
#        create table [Group](
#            department_number integer,
#            number integer,
#            primary key (department_number, number)
#            );
#        drop table if exists Subject;
#        create table Subject(
#            name varchar(20) primary key
#            );
#        drop table if exists Group_Subject;
#        create table Group_Subject(
#            group_department_number integer,
#            group_number integer,
#            subject_name varchar(20),
#            primary key (group_department_number, group_number, subject_name)
#            );
#        insert into Department values (1, 'Dep1');
#        insert into [Group] values (1, 101);
#        insert into [Group] values (1, 102);
#        insert into Subject values ('Subj1');
#        insert into Subject values ('Subj2');
#        insert into Group_Subject values (1, 101, 'Subj1');
#        """)
#        self.db.generate_mapping(check_tables=True)
#        local.session = DBSession()
#    def tearDown(self):
#        self.db.release()
#    def test_add(self):
#        Group = self.diagram.entities.get("Group")
#        Subject = self.diagram.entities.get("Subject")
#        g = Group.find_one(number=101)
#        s2 = Subject.find_one('Subj2')
#        g.subjects.add(s2)
#        #commit()
#    def test_remove(self):
#        Group = self.diagram.entities.get("Group")
#        Subject = self.diagram.entities.get("Subject")
#        g = Group.find_one(number=101)
#        s1 = Subject.find_one('Subj1')
#        g.subjects.add(s1)
#        #commit()

if __name__ == "__main__":
    unittest.main()
