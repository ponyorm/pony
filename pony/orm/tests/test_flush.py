from __future__ import absolute_import, print_function, division

import unittest

from pony.orm.core import *
from pony.orm.tests.testutils import *
from pony.orm.tests import setup_database, teardown_database

db = Database()


class Person(db.Entity):
    name = Required(unicode)


class TestFlush(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_database(db)

    @classmethod
    def tearDownClass(self):
        teardown_database(db)

    def test1(self):
        with db_session:
            a = Person(name='A')
            b = Person(name='B')
            c = Person(name='C')
            self.assertEqual(a.id, None)
            self.assertEqual(b.id, None)
            self.assertEqual(c.id, None)

            b.flush()
            self.assertEqual(a.id, None)
            self.assertIsNotNone(b.id)
            b_id = b.id
            self.assertEqual(c.id, None)

            flush()
            self.assertIsNotNone(a.id)
            self.assertEqual(b.id, b_id)
            self.assertIsNotNone(c.id)
            self.assertEqual(len({a.id, b.id, c.id}), 3)
