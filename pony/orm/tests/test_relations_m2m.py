from __future__ import with_statement

import unittest
from pony.orm.core import *

class TestManyToManyNonComposite(unittest.TestCase):

    def setUp(self):
        db = Database('sqlite', ':memory:')

        class Group(db.Entity):
            number = PrimaryKey(int)
            subjects = Set("Subject")

        class Subject(db.Entity):
            name = PrimaryKey(str)
            groups = Set(Group)

        self.db = db
        self.Group = Group
        self.Subject = Subject
        
        self.db.generate_mapping(create_tables=True)

        with db_session:
           g1 = Group(number=101)
           g2 = Group(number=102)
           s1 = Subject(name='Subj1')
           s2 = Subject(name='Subj2')
           s3 = Subject(name='Subj3')
           s4 = Subject(name='Subj4')
           g1.subjects = [ s1, s2 ]

    def test_1(self):
        db, Group, Subject = self.db, self.Group, self.Subject

        with db_session:
            g = Group.get(number=101)
            s = Subject.get(name='Subj1')
            g.subjects.add(s)

        with db_session:
            db_subjects = db.select('subject from Group_Subject where "group" = 101')
            self.assertEqual(db_subjects , ['Subj1', 'Subj2'])

    def test_2(self):
        db, Group, Subject = self.db, self.Group, self.Subject

        with db_session:
            g = Group.get(number=101)
            s = Subject.get(name='Subj3')
            g.subjects.add(s)

        with db_session:
            db_subjects = db.select('subject from Group_Subject where "group" = 101')
            self.assertEqual(db_subjects , ['Subj1', 'Subj2', 'Subj3'])

    def test_3(self):
        db, Group, Subject = self.db, self.Group, self.Subject

        with db_session:
            g = Group.get(number=101)
            s = Subject.get(name='Subj3')
            g.subjects.remove(s)

        with db_session:
            db_subjects = db.select('subject from Group_Subject where "group" = 101')
            self.assertEqual(db_subjects , ['Subj1', 'Subj2'])

    def test_4(self):
        db, Group, Subject = self.db, self.Group, self.Subject

        with db_session:
            g = Group.get(number=101)
            s = Subject.get(name='Subj2')
            g.subjects.remove(s)

        with db_session:
            db_subjects = db.select('subject from Group_Subject where "group" = 101')
            self.assertEqual(db_subjects , ['Subj1'])

    def test_5(self):
        db, Group, Subject = self.db, self.Group, self.Subject

        with db_session:
            g = Group.get(number=101)
            s1, s2, s3, s4 = Subject.select()[:]
            g.subjects.remove([s1, s2])
            g.subjects.add([s3, s4])

        with db_session:
            db_subjects = db.select('subject from Group_Subject where "group" = 101')
            self.assertEqual(db_subjects , ['Subj3', 'Subj4'])
            self.assertEqual(Group[101].subjects, set([Subject['Subj3'], Subject['Subj4']]))

    def test_6(self):
        db, Group, Subject = self.db, self.Group, self.Subject

        with db_session:
            g = Group.get(number=101)
            s = Subject.get(name='Subj3')
            g.subjects.add(s)
            g.subjects.remove(s)
            last_sql = db.last_sql

        with db_session:
            self.assertEqual(db.last_sql, last_sql)  # assert no DELETE statement on commit
            db_subjects = db.select('subject from Group_Subject where "group" = 101')
            self.assertEqual(db_subjects , ['Subj1', 'Subj2'])

    def test_7(self):
        db, Group, Subject = self.db, self.Group, self.Subject

        with db_session:
            g = Group.get(number=101)
            s = Subject.get(name='Subj1')
            g.subjects.remove(s)
            g.subjects.add(s)
            last_sql = db.last_sql

        with db_session:
            self.assertEqual(db.last_sql, last_sql)  # assert no INSERT statement on commit
            db_subjects = db.select('subject from Group_Subject where "group" = 101')
            self.assertEqual(db_subjects , ['Subj1', 'Subj2'])

    def test_8(self):
        db, Group, Subject = self.db, self.Group, self.Subject

        with db_session:
            g = Group.get(number=101)
            s1 = Subject.get(name='Subj1')
            s2 = Subject.get(name='Subj2')
            g.subjects.clear()
            g.subjects.add([s1, s2])
            last_sql = db.last_sql

        with db_session:
            self.assertEqual(db.last_sql, last_sql)  # assert no INSERT statement on commit
            db_subjects = db.select('subject from Group_Subject where "group" = 101')
            self.assertEqual(db_subjects , ['Subj1', 'Subj2'])

    def test_9(self):
        db, Group, Subject = self.db, self.Group, self.Subject

        with db_session:
            g2 = Group.get(number=102)
            s1 = Subject.get(name='Subj1')
            g2.subjects.add(s1)
            g2.subjects.clear()
            last_sql = db.last_sql

        with db_session:
            self.assertEqual(db.last_sql, last_sql)  # assert no DELETE statement on commit
            db_subjects = db.select('subject from Group_Subject where "group" = 102')
            self.assertEqual(db_subjects , [])

    def test_10(self):
        db, Group, Subject = self.db, self.Group, self.Subject

        with db_session:
            g = Group.get(number=101)
            s1, s2, s3, s4 = Subject.select()[:]
            g.subjects = [ s2, s3 ]

        with db_session:
            db_subjects = db.select('subject from Group_Subject where "group" = 101')
            self.assertEqual(db_subjects , ['Subj2', 'Subj3'])

    def test_11(self):
        db, Group, Subject = self.db, self.Group, self.Subject

        with db_session:
            g = Group.get(number=101)
            s1, s2, s3, s4 = Subject.select()[:]
            g.subjects.remove(s2)
            g.subjects = [ s1, s2 ]
            last_sql = db.last_sql

        with db_session:
            self.assertEqual(db.last_sql, last_sql)  # assert no INSERT statement on commit
            db_subjects = db.select('subject from Group_Subject where "group" = 101')
            self.assertEqual(db_subjects , ['Subj1', 'Subj2'])

    def test_12(self):
        db, Group, Subject = self.db, self.Group, self.Subject

        with db_session:
            g = Group.get(number=101)
            s1, s2, s3, s4 = Subject.select()[:]
            g.subjects.add(s3)
            g.subjects = [ s1, s2 ]
            last_sql = db.last_sql

        with db_session:
            self.assertEqual(db.last_sql, last_sql)  # assert no DELETE statement on commit
            db_subjects = db.select('subject from Group_Subject where "group" = 101')
            self.assertEqual(db_subjects , ['Subj1', 'Subj2'])

if __name__ == "__main__":
    unittest.main()
