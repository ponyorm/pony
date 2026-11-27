import unittest
from typing import TYPE_CHECKING

from pony.orm import *
from pony.orm.tests import setup_database, teardown_database
from pony.orm.tests.testutils import raises_exception

db = Database()

if TYPE_CHECKING:
    from pony.orm.core import Entity
else:
    Entity = db.Entity


class Person(Entity):
    name = Required(str)
    tel = Optional(str)


class TestAutostrip(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_database(db)

    @classmethod
    def tearDownClass(cls):
        teardown_database(db)

    @db_session
    def test_1(self):
        p = Person(name=" John  ", tel=" ")
        p.flush()
        self.assertEqual(p.name, "John")
        self.assertEqual(p.tel, "")

    @raises_exception(ValueError, "Attribute Person.name is required")
    @db_session
    def test_2(self):
        p = Person(name=" ")


if __name__ == "__main__":
    unittest.main()
