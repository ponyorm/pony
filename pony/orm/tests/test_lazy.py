from __future__ import absolute_import, print_function, division

import unittest

from pony.orm.core import *
from pony.orm.tests import setup_database, teardown_database


class TestLazy(unittest.TestCase):
    def setUp(self):
        db = self.db = Database()
        class X(self.db.Entity):
            a = Required(int)
            b = Required(unicode, lazy=True)
        self.X = X
        setup_database(db)
        with db_session:
            x1 = X(id=1, a=1, b='first')
            x2 = X(id=2, a=2, b='second')
            x3 = X(id=3, a=3, b='third')

    def tearDown(self):
        teardown_database(self.db)

    @db_session
    def test_lazy_1(self):
        X = self.X
        x1 = X[1]
        self.assertTrue(X.a in x1._vals_)
        self.assertTrue(X.b not in x1._vals_)
        b = x1.b
        self.assertEqual(b, 'first')

    @db_session
    def test_lazy_2(self):
        X = self.X
        x1 = X[1]
        x2 = X[2]
        x3 = X[3]
        self.assertTrue(X.b not in x1._vals_)
        self.assertTrue(X.b not in x2._vals_)
        self.assertTrue(X.b not in x3._vals_)
        b = x1.b
        self.assertTrue(X.b in x1._vals_)
        self.assertTrue(X.b not in x2._vals_)
        self.assertTrue(X.b not in x3._vals_)

    @db_session
    def test_lazy_3(self):  # coverage of https://github.com/ponyorm/pony/issues/49
        X = self.X
        x1 = X.get(b='first')
        self.assertTrue(X._bits_[X.b] & x1._rbits_)
        self.assertTrue(X.b, x1._vals_)

    @db_session
    def test_lazy_4(self):  # coverage of https://github.com/ponyorm/pony/issues/49
        X = self.X
        result = select(x for x in X if x.b == 'first')[:]
        for x in result:
            self.assertTrue(X._bits_[X.b] & x._rbits_)
            self.assertTrue(X.b in x._vals_)

    @db_session
    def test_lazy_5(self):  # coverage of https://github.com/ponyorm/pony/issues/49
        X = self.X
        result = select(x for x in X if x.b == 'first' if count() > 0)[:]
        for x in result:
            self.assertFalse(X._bits_[X.b] & x._rbits_)
            self.assertTrue(X.b not in x._vals_)
