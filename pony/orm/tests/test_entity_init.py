import unittest
from datetime import datetime
from hashlib import md5
from typing import TYPE_CHECKING

from pony.orm import *
from pony.orm.tests import setup_database, teardown_database

db = Database()

if TYPE_CHECKING:
    from pony.orm.core import Entity
else:
    Entity = db.Entity


class User(Entity):
    name = Required(str)
    password = Required(str)
    created_at = Required(datetime)

    def __init__(self, name, password):
        password = md5(password.encode("utf8")).hexdigest()
        super().__init__(name=name, password=password, created_at=datetime.now())
        self.uppercase_name = name.upper()


class TestCustomInit(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_database(db)

    @classmethod
    def tearDownClass(cls):
        teardown_database(db)

    def test1(self):
        with db_session:
            u1 = User("John", "123")
            u2 = User("Mike", "456")
            commit()
            self.assertEqual(u1.name, "John")
            self.assertEqual(u1.uppercase_name, "JOHN")
            self.assertEqual(u1.password, md5(b"123").hexdigest())
            self.assertEqual(u2.name, "Mike")
            self.assertEqual(u2.uppercase_name, "MIKE")
            self.assertEqual(u2.password, md5(b"456").hexdigest())

        with db_session:
            users = select(u for u in User).order_by(User.id)
            self.assertEqual(len(users), 2)
            u1, u2 = users
            self.assertEqual(u1.name, "John")
            self.assertTrue(not hasattr(u1, "uppercase_name"))
            self.assertEqual(u1.password, md5(b"123").hexdigest())
            self.assertEqual(u2.name, "Mike")
            self.assertTrue(not hasattr(u2, "uppercase_name"))
            self.assertEqual(u2.password, md5(b"456").hexdigest())


if __name__ == "__main__":
    unittest.main()
