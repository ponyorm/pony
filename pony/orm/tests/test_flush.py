import unittest
from typing import TYPE_CHECKING

from pony.orm.core import *
from pony.orm.tests import setup_database, teardown_database
from pony.orm.tests.testutils import *

db = Database()

if TYPE_CHECKING:
    from pony.orm.core import Entity
else:
    Entity = db.Entity


class Person(Entity):
    name = Required(str)


class TestFlush(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_database(db)

    @classmethod
    def tearDownClass(cls):
        teardown_database(db)

    def test1(self):
        with db_session:
            a = Person(name="A")
            b = Person(name="B")
            c = Person(name="C")
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
