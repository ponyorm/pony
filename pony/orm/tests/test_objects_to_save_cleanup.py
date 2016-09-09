

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


class EntityStatusTestCase(object):

    def make_flush(self, obj=None):
        raise NotImplementedError

    @db_session
    def test_delete_updated(self):
        p = TestPost()
        self.make_flush(p)
        p.name = 'Pony'
        self.assertEqual(p._status_, 'modified')
        self.make_flush(p)
        self.assertEqual(p._status_, 'updated')
        p.delete()
        self.assertEqual(p._status_, 'marked_to_delete')
        self.make_flush(p)
        self.assertEqual(p._status_, 'deleted')

    @db_session
    def test_delete_inserted(self):
        p = TestPost()
        self.assertEqual(p._status_, 'created')
        self.make_flush(p)
        self.assertEqual(p._status_, 'inserted')
        p.delete()

    @db_session
    def test_cancelled(self):
        p = TestPost()
        self.assertEqual(p._status_, 'created')
        p.delete()
        self.assertEqual(p._status_, 'cancelled')
        self.make_flush(p)
        self.assertEqual(p._status_, 'cancelled')


class EntityStatusTestCase_ObjectFlush(EntityStatusTestCase,
                                       unittest.TestCase):

    def make_flush(self, obj=None):
        obj.flush()


class EntityStatusTestCase_FullFlush(EntityStatusTestCase,
                                     unittest.TestCase):

    def make_flush(self, obj=None):
        flush() # full flush
