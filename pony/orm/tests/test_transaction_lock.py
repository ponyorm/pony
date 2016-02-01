

import unittest


from pony.orm import *

db = Database()

class TestPost(db.Entity):
    category = Optional('TestCategory')
    name = Optional(str, default='Noname')

class TestCategory(db.Entity):
    posts = Set(TestPost)

db.bind('sqlite', ':memory:')
db.generate_mapping(create_tables=True)

with db_session:
    post = TestPost()


class TransactionLockTestCase(unittest.TestCase):

    __call__ = db_session(unittest.TestCase.__call__)

    def tearDown(self):
        rollback()

    def test_create(self):
        p = TestPost()
        p.flush()
        cache = db._get_cache()
        self.assertEqual(cache.immediate, True)
        self.assertEqual(cache.in_transaction, True)

    def test_update(self):
        p = TestPost[post.id]
        p.name = 'Trash'
        p.flush()
        cache = db._get_cache()
        self.assertEqual(cache.immediate, True)
        self.assertEqual(cache.in_transaction, True)

    def test_delete(self):
        p = TestPost[post.id]
        p.delete()
        flush()
        cache = db._get_cache()
        self.assertEqual(cache.immediate, True)
        self.assertEqual(cache.in_transaction, True)
