import unittest


from pony.orm import *
from pony.orm.tests import setup_database, teardown_database

db = Database()


class TestPost(db.Entity):
    category = Optional('TestCategory')
    name = Optional(str, default='Noname')


class TestCategory(db.Entity):
    posts = Set(TestPost)


class TransactionLockTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_database(db)
        with db_session:
            cls.post = TestPost(id=1)

    @classmethod
    def tearDownClass(cls):
        teardown_database(db)

    __call__ = db_session(unittest.TestCase.__call__)

    def tearDown(self):
        rollback()

    def test_create(self):
        p = TestPost(id=2)
        p.flush()
        cache = db._get_cache()
        self.assertEqual(cache.immediate, True)
        self.assertEqual(cache.in_transaction, True)

    def test_update(self):
        p = TestPost[self.post.id]
        p.name = 'Trash'
        p.flush()
        cache = db._get_cache()
        self.assertEqual(cache.immediate, True)
        self.assertEqual(cache.in_transaction, True)

    def test_delete(self):
        p = TestPost[self.post.id]
        p.delete()
        flush()
        cache = db._get_cache()
        self.assertEqual(cache.immediate, True)
        self.assertEqual(cache.in_transaction, True)
