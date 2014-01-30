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

    @db_session
    def test_13(self):
        db, Group, Subject = self.db, self.Group, self.Subject

        g1 = Group[101]
        s1 = Subject['Subj1']
        self.assertTrue(s1 in g1.subjects)

        group_setdata = g1._vals_[Group.subjects]
        self.assertTrue(s1 in group_setdata)
        self.assertEqual(group_setdata.added, None)
        self.assertEqual(group_setdata.removed, None)
        
        subj_setdata = s1._vals_[Subject.groups]
        self.assertTrue(g1 in subj_setdata)
        self.assertEqual(subj_setdata.added, None)
        self.assertEqual(subj_setdata.removed, None)

        g1.subjects.remove(s1)
        self.assertTrue(s1 not in group_setdata)
        self.assertEqual(group_setdata.added, None)
        self.assertEqual(group_setdata.removed, set([ s1 ]))
        self.assertTrue(g1 not in subj_setdata)
        self.assertEqual(subj_setdata.added, None)
        self.assertEqual(subj_setdata.removed, set([ g1 ]))
        
        g1.subjects.add(s1)
        self.assertTrue(s1 in group_setdata)
        self.assertEqual(group_setdata.added, set())
        self.assertEqual(group_setdata.removed, set())
        self.assertTrue(g1 in subj_setdata)
        self.assertEqual(subj_setdata.added, set())
        self.assertEqual(subj_setdata.removed, set())

    @db_session
    def test_14(self):
        db, Group, Subject = self.db, self.Group, self.Subject

        g = Group[101]
        e = g.subjects.is_empty()
        self.assertEquals(e, False)

        db._dblocal.last_sql = None
        e = g.subjects.is_empty()  # should take result from the cache
        self.assertEquals(e, False)
        self.assertEquals(db.last_sql, None)

        g = Group[102]
        e = g.subjects.is_empty()  # should take SQL from the SQL cache
        self.assertEquals(e, True)

        db._dblocal.last_sql = None
        e = g.subjects.is_empty()  # should take result from the cache
        self.assertEquals(e, True)
        self.assertEquals(db.last_sql, None)

    @db_session
    def test_15(self):
        db, Group = self.db, self.Group

        g = Group[101]
        c = len(g.subjects)
        self.assertEquals(c, 2)
        db._dblocal.last_sql = None
        e = g.subjects.is_empty()  # should take result from the cache
        self.assertEquals(e, False)
        self.assertEquals(db.last_sql, None)
        
        g = Group[102]
        c = len(g.subjects)
        self.assertEquals(c, 0)
        db._dblocal.last_sql = None
        e = g.subjects.is_empty()  # should take result from the cache
        self.assertEquals(e, True)
        self.assertEquals(db.last_sql, None)

    @db_session
    def test_16(self):
        db, Group, Subject = self.db, self.Group, self.Subject

        g = Group[101]
        s1 = Subject['Subj1']
        s3 = Subject['Subj3']
        c = g.subjects.count()
        self.assertEquals(c, 2)

        db._dblocal.last_sql = None
        c = g.subjects.count()  # should take count from the cache
        self.assertEquals(c, 2)
        self.assertEquals(db.last_sql, None)

        g.subjects.add(s3)
        db._dblocal.last_sql = None
        c = g.subjects.count()  # should take modified count from the cache
        self.assertEquals(c, 3)
        self.assertEquals(db.last_sql, None)

        g.subjects.remove(s1)
        db._dblocal.last_sql = None
        c = g.subjects.count()  # should take modified count from the cache
        self.assertEquals(c, 2)
        self.assertEquals(db.last_sql, None)


if __name__ == "__main__":
    unittest.main()
