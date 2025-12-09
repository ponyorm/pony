from __future__ import absolute_import, print_function, division

import unittest
from datetime import date, datetime
from hashlib import md5

from pony.orm.tests.testutils import raises_exception
from pony.orm import *
from pony.orm.tests import setup_database, teardown_database

db = Database()


class User(db.Entity):
    name = Required(str)
    password = Required(str)
    created_at = Required(datetime)

    def __init__(self, name, password):
        password = md5(password.encode('utf8')).hexdigest()
        super(User, self).__init__(name=name, password=password, created_at=datetime.now())
        self.uppercase_name = name.upper()


class TestCustomInit(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_database(db)

    @classmethod
    def tearDownClass(self):
        teardown_database(db)

    def test1(self):
        with db_session:
            u1 = User('John', '123')
            u2 = User('Mike', '456')
            commit()
            self.assertEqual(u1.name, 'John')
            self.assertEqual(u1.uppercase_name, 'JOHN')
            self.assertEqual(u1.password, md5(b'123').hexdigest())
            self.assertEqual(u2.name, 'Mike')
            self.assertEqual(u2.uppercase_name, 'MIKE')
            self.assertEqual(u2.password, md5(b'456').hexdigest())

        with db_session:
            users = select(u for u in User).order_by(User.id)
            self.assertEqual(len(users), 2)
            u1, u2 = users
            self.assertEqual(u1.name, 'John')
            self.assertTrue(not hasattr(u1, 'uppercase_name'))
            self.assertEqual(u1.password, md5(b'123').hexdigest())
            self.assertEqual(u2.name, 'Mike')
            self.assertTrue(not hasattr(u2, 'uppercase_name'))
            self.assertEqual(u2.password, md5(b'456').hexdigest())

if __name__ == '__main__':
    unittest.main()