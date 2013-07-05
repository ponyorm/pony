from __future__ import with_statement

import unittest
from pony.orm.core import *

db = Database('sqlite', ':memory:')

class Group(db.Entity):
    number = PrimaryKey(int)
    subjects = Set("Subject")

class Subject(db.Entity):
    name = PrimaryKey(str)
    groups = Set(Group)

db.generate_mapping(create_tables=True)

with db_session:
   g1 = Group(number=101)
   g2 = Group(number=102)
   s1 = Subject(name='Subj1')
   s2 = Subject(name='Subj2')
   s3 = Subject(name='Subj3')
   s4 = Subject(name='Subj4')
   g1.subjects = [ s1, s2 ]

class TestManyToManyNonComposite(unittest.TestCase):
    def setUp(self):
        rollback()
        db_session.__enter__()
    def tearDown(self):
        rollback()
        db_session.__exit__()
    def test_add_remove(self):
        g = Group.get(number=101)
        subjects = Subject.select()[:]
        g.subjects.remove(subjects[:2])
        g.subjects.add(subjects[-2:])
        commit()
        rollback()
        self.assertEqual(Group[101].subjects, set([Subject['Subj3'], Subject['Subj4']]))
        db_subjects = db.select('subject from Group_Subject where "group" = 101')
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
#        self.db.generate_mapping()
#        local.session = DBSession()
#    def tearDown(self):
#        self.db.release()
#    def test_add(self):
#        Group = self.diagram.entities.get("Group")
#        Subject = self.diagram.entities.get("Subject")
#        g = Group.get(number=101)
#        s2 = Subject.get('Subj2')
#        g.subjects.add(s2)
#        #commit()
#    def test_remove(self):
#        Group = self.diagram.entities.get("Group")
#        Subject = self.diagram.entities.get("Subject")
#        g = Group.get(number=101)
#        s1 = Subject.get('Subj1')
#        g.subjects.add(s1)
#        #commit()

if __name__ == "__main__":
    unittest.main()
