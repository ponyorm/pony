from __future__ import absolute_import, print_function, division

import unittest

from pony import orm
from pony.orm.tests.testutils import *
from pony.orm.tests import setup_database, teardown_database

class TestIntConverter1(unittest.TestCase):
    def setUp(self):
        self.db = db = orm.Database()

        class Foo(db.Entity):
            id = orm.PrimaryKey(int)
            x = orm.Required(int, size=8, unsigned=True)

        setup_database(db)

        with orm.db_session:
            foo = Foo(id=123, x=1)

    def tearDown(self):
        teardown_database(self.db)

    def test_1(self):
        with orm.db_session:
            foo = self.db.Foo[123]
            foo.x -= 1
        with orm.db_session:
            foo = self.db.Foo[123]
            self.assertEqual(foo.x, 0)

    @raises_exception(ValueError, "Value -1 of attr Foo.x is less than the minimum allowed value 0")
    @orm.db_session
    def test_2(self):
        foo = self.db.Foo[123]
        foo.x -= 2

    @orm.db_session
    def test_3(self):
        with orm.db_session:
            foo = self.db.Foo[123]
            foo.x += 254
        with orm.db_session:
            foo = self.db.Foo[123]
            self.assertEqual(foo.x, 255)

    @raises_exception(ValueError, "Value 256 of attr Foo.x is greater than the maximum allowed value 255")
    @orm.db_session
    def test_4(self):
        foo = self.db.Foo[123]
        foo.x += 255
