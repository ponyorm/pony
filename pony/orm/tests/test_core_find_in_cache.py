from __future__ import absolute_import, print_function, division

import unittest
from pony.orm.tests.testutils import raises_exception
from pony.orm import *
from pony.orm.tests import setup_database, teardown_database

db = Database()

class AbstractUser(db.Entity):
    username = PrimaryKey(unicode)

class User(AbstractUser):
    diagrams = Set('Diagram')
    email = Optional(unicode)

class SubUser1(User):
    attr1 = Optional(unicode)

class SubUser2(User):
    attr2 = Optional(unicode)

class Organization(AbstractUser):
    address = Optional(unicode)

class SubOrg1(Organization):
    attr3 = Optional(unicode)

class SubOrg2(Organization):
    attr4 = Optional(unicode)

class Diagram(db.Entity):
    name = Required(unicode)
    owner = Required(User)


def is_seed(entity, pk):
    cache = entity._database_._get_cache()
    return pk in [ obj._pk_ for obj in cache.seeds[entity._pk_attrs_] ]


class TestFindInCache(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_database(db)
        with db_session:
            u1 = User(username='user1')
            u2 = SubUser1(username='subuser1', attr1='some attr')
            u3 = SubUser2(username='subuser2', attr2='some attr')
            o1 = Organization(username='org1')
            o2 = SubOrg1(username='suborg1', attr3='some attr')
            o3 = SubOrg2(username='suborg2', attr4='some attr')
            au = AbstractUser(username='abstractUser')
            Diagram(name='diagram1', owner=u1)
            Diagram(name='diagram2', owner=u2)
            Diagram(name='diagram3', owner=u3)

    @classmethod
    def tearDownClass(cls):
        teardown_database(db)

    def setUp(self):
        rollback()
        db_session.__enter__()

    def tearDown(self):
        rollback()
        db_session.__exit__()

    def test1(self):
        u = User.get(username='org1')
        org = Organization.get(username='org1')
        u1 = User.get(username='org1')
        self.assertEqual(u, None)
        self.assertEqual(org, Organization['org1'])
        self.assertEqual(u1, None)

    def test_user_1(self):
        Diagram.get(lambda d: d.name == 'diagram1')
        last_sql = db.last_sql
        self.assertTrue(is_seed(User, 'user1'))
        u = AbstractUser['user1']
        self.assertNotEqual(last_sql, db.last_sql)
        self.assertEqual(u.__class__, User)

    def test_user_2(self):
        Diagram.get(lambda d: d.name == 'diagram1')
        last_sql = db.last_sql
        self.assertTrue(is_seed(User, 'user1'))
        u = User['user1']
        self.assertNotEqual(last_sql, db.last_sql)
        self.assertEqual(u.__class__, User)

    @raises_exception(ObjectNotFound)
    def test_user_3(self):
        Diagram.get(lambda d: d.name == 'diagram1')
        last_sql = db.last_sql
        self.assertTrue(is_seed(User, 'user1'))
        try:
            SubUser1['user1']
        finally:
            self.assertNotEqual(last_sql, db.last_sql)

    @raises_exception(ObjectNotFound)
    def test_user_4(self):
        Diagram.get(lambda d: d.name == 'diagram1')
        last_sql = db.last_sql
        self.assertTrue(is_seed(User, 'user1'))
        try:
            Organization['user1']
        finally:
            self.assertEqual(last_sql, db.last_sql)

    @raises_exception(ObjectNotFound)
    def test_user_5(self):
        Diagram.get(lambda d: d.name == 'diagram1')
        last_sql = db.last_sql
        self.assertTrue(is_seed(User, 'user1'))
        try:
            SubOrg1['user1']
        finally:
            self.assertEqual(last_sql, db.last_sql)

    def test_subuser_1(self):
        Diagram.get(lambda d: d.name == 'diagram2')
        last_sql = db.last_sql
        self.assertTrue(is_seed(User, 'subuser1'))
        u = AbstractUser['subuser1']
        self.assertNotEqual(last_sql, db.last_sql)
        self.assertEqual(u.__class__, SubUser1)

    def test_subuser_2(self):
        Diagram.get(lambda d: d.name == 'diagram2')
        last_sql = db.last_sql
        self.assertTrue(is_seed(User, 'subuser1'))
        u = User['subuser1']
        self.assertNotEqual(last_sql, db.last_sql)
        self.assertEqual(u.__class__, SubUser1)

    def test_subuser_3(self):
        Diagram.get(lambda d: d.name == 'diagram2')
        last_sql = db.last_sql
        self.assertTrue(is_seed(User, 'subuser1'))
        u = SubUser1['subuser1']
        self.assertNotEqual(last_sql, db.last_sql)
        self.assertEqual(u.__class__, SubUser1)

    @raises_exception(ObjectNotFound)
    def test_subuser_4(self):
        Diagram.get(lambda d: d.name == 'diagram2')
        last_sql = db.last_sql
        self.assertTrue(is_seed(User, 'subuser1'))
        try:
            Organization['subuser1']
        finally:
            self.assertEqual(last_sql, db.last_sql)

    @raises_exception(ObjectNotFound)
    def test_subuser_5(self):
        Diagram.get(lambda d: d.name == 'diagram2')
        last_sql = db.last_sql
        self.assertTrue(is_seed(User, 'subuser1'))
        try:
            SubUser2['subuser1']
        finally:
            self.assertNotEqual(last_sql, db.last_sql)

    @raises_exception(ObjectNotFound)
    def test_subuser_6(self):
        Diagram.get(lambda d: d.name == 'diagram2')
        last_sql = db.last_sql
        self.assertTrue(is_seed(User, 'subuser1'))
        try:
            SubOrg2['subuser1']
        finally:
            self.assertEqual(last_sql, db.last_sql)

    def test_user_6(self):
        u1 = SubUser1['subuser1']
        last_sql = db.last_sql
        u2 = SubUser1['subuser1']
        self.assertEqual(last_sql, db.last_sql)
        self.assertEqual(u1, u2)

    def test_user_7(self):
        u1 = SubUser1['subuser1']
        u1.delete()
        last_sql = db.last_sql
        u2 = SubUser1.get(username='subuser1')
        self.assertEqual(last_sql, db.last_sql)
        self.assertEqual(u2, None)

    def test_user_8(self):
        u1 = SubUser1['subuser1']
        last_sql = db.last_sql
        u2 = SubUser1.get(username='subuser1', attr1='wrong val')
        self.assertEqual(last_sql, db.last_sql)
        self.assertEqual(u2, None)

if __name__ == '__main__':
    unittest.main()