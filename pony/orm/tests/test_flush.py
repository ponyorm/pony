from __future__ import absolute_import, print_function, division

import unittest

from pony.orm.core import *
from pony.orm.tests.testutils import *

class TestFlush(unittest.TestCase):
    def setUp(self):
        self.db = Database('sqlite', ':memory:')

        class Person(self.db.Entity):
            name = Required(unicode)

        self.db.generate_mapping(create_tables=True)

    def tearDown(self):
        self.db = None

    def test1(self):
        Person = self.db.Person
        with db_session:
            a = Person(name='A')
            b = Person(name='B')
            c = Person(name='C')
            self.assertEqual(a.id, None)
            self.assertEqual(b.id, None)
            self.assertEqual(c.id, None)

            b.flush()
            self.assertEqual(a.id, None)
            self.assertEqual(b.id, 1)
            self.assertEqual(c.id, None)

            flush()
            self.assertEqual(a.id, 2)
            self.assertEqual(b.id, 1)
            self.assertEqual(c.id, 3)
