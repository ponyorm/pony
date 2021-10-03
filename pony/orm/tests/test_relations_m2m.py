from __future__ import absolute_import, print_function, division

import unittest
from pony.orm.core import *
from pony.orm.tests import db_params, teardown_database

db = Database()


class Group(db.Entity):
    number = PrimaryKey(int)
    subjects = Set("Subject")


class Subject(db.Entity):
    name = PrimaryKey(str)
    groups = Set(Group)


class TestManyToManyNonComposite(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        db.bind(**db_params)
        db.generate_mapping(check_tables=False)
        db.drop_all_tables(with_all_data=True)

    def setUp(self):
        db.create_tables()
        with db_session:
           g1 = Group(number=101)
           g2 = Group(number=102)
           s1 = Subject(name='Subj1')
           s2 = Subject(name='Subj2')
           s3 = Subject(name='Subj3')
           s4 = Subject(name='Subj4')
           g1.subjects = [ s1, s2 ]

    def tearDown(self):
        teardown_database(db)

    def test_1(self):
        schema = db.schema
        m2m_table_name = 'Group_Subject'
        if not (db.provider.dialect == 'SQLite' and pony.__version__ < '0.9'):
            m2m_table_name = m2m_table_name.lower()
        self.assertIn(m2m_table_name, schema.tables)
        m2m_table = schema.tables[m2m_table_name]
        if pony.__version__ >= '0.9':
            fkeys = m2m_table.foreign_keys
        else:
            fkeys = set(m2m_table.foreign_keys.values())
        self.assertEqual(len(fkeys), 2)
        for fk in fkeys:
            self.assertEqual(fk.on_delete, 'CASCADE')

    def test_2(self):
        with db_session:
            g = Group.get(number=101)
            s = Subject.get(name='Subj1')
            g.subjects.add(s)

        with db_session:
            db_subjects = db.select('subject from Group_Subject where "group" = 101')
            self.assertEqual(set(db_subjects), {'Subj1', 'Subj2'})

    def test_3(self):
        with db_session:
            g = Group.get(number=101)
            s = Subject.get(name='Subj3')
            g.subjects.add(s)

        with db_session:
            db_subjects = db.select('subject from Group_Subject where "group" = 101')
            self.assertEqual(set(db_subjects), {'Subj1', 'Subj2', 'Subj3'})

    def test_4(self):
        with db_session:
            g = Group.get(number=101)
            s = Subject.get(name='Subj3')
            g.subjects.remove(s)

        with db_session:
            db_subjects = db.select('subject from Group_Subject where "group" = 101')
            self.assertEqual(set(db_subjects), {'Subj1', 'Subj2'})

    def test_5(self):
        with db_session:
            g = Group.get(number=101)
            s = Subject.get(name='Subj2')
            g.subjects.remove(s)

        with db_session:
            db_subjects = db.select('subject from Group_Subject where "group" = 101')
            self.assertEqual(set(db_subjects), {'Subj1'})

    def test_6(self):
        with db_session:
            g = Group.get(number=101)
            s1, s2, s3, s4 = Subject.select()[:]
            g.subjects.remove([s1, s2])
            g.subjects.add([s3, s4])

        with db_session:
            db_subjects = db.select('subject from Group_Subject where "group" = 101')
            self.assertEqual(set(db_subjects), {'Subj3', 'Subj4'})
            self.assertEqual(Group[101].subjects, {Subject['Subj3'], Subject['Subj4']})

    def test_7(self):
        with db_session:
            g = Group.get(number=101)
            s = Subject.get(name='Subj3')
            g.subjects.add(s)
            g.subjects.remove(s)
            last_sql = db.last_sql

        with db_session:
            self.assertEqual(db.last_sql, last_sql)  # assert no DELETE statement on commit
            db_subjects = db.select('subject from Group_Subject where "group" = 101')
            self.assertEqual(set(db_subjects), {'Subj1', 'Subj2'})

    def test_8(self):
        with db_session:
            g = Group.get(number=101)
            s = Subject.get(name='Subj1')
            g.subjects.remove(s)
            g.subjects.add(s)
            last_sql = db.last_sql

        with db_session:
            self.assertEqual(db.last_sql, last_sql)  # assert no INSERT statement on commit
            db_subjects = db.select('subject from Group_Subject where "group" = 101')
            self.assertEqual(set(db_subjects), {'Subj1', 'Subj2'})

    def test_9(self):
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
            self.assertEqual(set(db_subjects), {'Subj1', 'Subj2'})

    def test_10(self):
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

    def test_11(self):
        with db_session:
            g = Group.get(number=101)
            s1, s2, s3, s4 = Subject.select()[:]
            g.subjects = [ s2, s3 ]

        with db_session:
            db_subjects = db.select('subject from Group_Subject where "group" = 101')
            self.assertEqual(set(db_subjects), {'Subj2', 'Subj3'})

    def test_12(self):
        with db_session:
            g = Group.get(number=101)
            s1, s2, s3, s4 = Subject.select()[:]
            g.subjects.remove(s2)
            g.subjects = [ s1, s2 ]
            last_sql = db.last_sql

        with db_session:
            self.assertEqual(db.last_sql, last_sql)  # assert no INSERT statement on commit
            db_subjects = db.select('subject from Group_Subject where "group" = 101')
            self.assertEqual(set(db_subjects), {'Subj1', 'Subj2'})

    def test_13(self):
        with db_session:
            g = Group.get(number=101)
            s1, s2, s3, s4 = Subject.select()[:]
            g.subjects.add(s3)
            g.subjects = [ s1, s2 ]
            last_sql = db.last_sql

        with db_session:
            self.assertEqual(db.last_sql, last_sql)  # assert no DELETE statement on commit
            db_subjects = db.select('subject from Group_Subject where "group" = 101')
            self.assertEqual(set(db_subjects), {'Subj1', 'Subj2'})

    @db_session
    def test_14(self):
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
        self.assertEqual(group_setdata.removed, {s1})
        self.assertTrue(g1 not in subj_setdata)
        self.assertEqual(subj_setdata.added, None)
        self.assertEqual(subj_setdata.removed, {g1})

        g1.subjects.add(s1)
        self.assertTrue(s1 in group_setdata)
        self.assertEqual(group_setdata.added, set())
        self.assertEqual(group_setdata.removed, set())
        self.assertTrue(g1 in subj_setdata)
        self.assertEqual(subj_setdata.added, set())
        self.assertEqual(subj_setdata.removed, set())

    @db_session
    def test_15(self):
        g = Group[101]
        e = g.subjects.is_empty()
        self.assertEqual(e, False)

        db._dblocal.last_sql = None
        e = g.subjects.is_empty()  # should take result from the cache
        self.assertEqual(e, False)
        self.assertEqual(db.last_sql, None)

        g = Group[102]
        e = g.subjects.is_empty()  # should take SQL from the SQL cache
        self.assertEqual(e, True)

        db._dblocal.last_sql = None
        e = g.subjects.is_empty()  # should take result from the cache
        self.assertEqual(e, True)
        self.assertEqual(db.last_sql, None)

    @db_session
    def test_16(self):
        g = Group[101]
        c = len(g.subjects)
        self.assertEqual(c, 2)
        db._dblocal.last_sql = None
        e = g.subjects.is_empty()  # should take result from the cache
        self.assertEqual(e, False)
        self.assertEqual(db.last_sql, None)

        g = Group[102]
        c = len(g.subjects)
        self.assertEqual(c, 0)
        db._dblocal.last_sql = None
        e = g.subjects.is_empty()  # should take result from the cache
        self.assertEqual(e, True)
        self.assertEqual(db.last_sql, None)

    @db_session
    def test_17(self):
        g = Group[101]
        s1 = Subject['Subj1']
        s3 = Subject['Subj3']
        c = g.subjects.count()
        self.assertEqual(c, 2)

        db._dblocal.last_sql = None
        c = g.subjects.count()  # should take count from the cache
        self.assertEqual(c, 2)
        self.assertEqual(db.last_sql, None)

        g.subjects.add(s3)
        db._dblocal.last_sql = None
        c = g.subjects.count()  # should take modified count from the cache
        self.assertEqual(c, 3)
        self.assertEqual(db.last_sql, None)

        g.subjects.remove(s1)
        db._dblocal.last_sql = None
        c = g.subjects.count()  # should take modified count from the cache
        self.assertEqual(c, 2)
        self.assertEqual(db.last_sql, None)


if __name__ == "__main__":
    unittest.main()
