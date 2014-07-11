import unittest

from pony.orm.core import *

class TestLazy(unittest.TestCase):
    def setUp(self):
        self.db = Database('sqlite', ':memory:')
        class X(self.db.Entity):
            a = Required(int)
            b = Required(unicode, lazy=True)
        self.X = X
        self.db.generate_mapping(create_tables=True)
        with db_session:
            x1 = X(a=1, b='first')
            x2 = X(a=2, b='second')
            x3 = X(a=3, b='third')

    @db_session
    def test_lazy_1(self):
        X = self.X
        x1 = X[1]
        self.assertIn(X.a, x1._vals_)
        self.assertNotIn(X.b, x1._vals_)
        b = x1.b
        self.assertEquals(b, 'first')

    @db_session
    def test_lazy_2(self):
        X = self.X
        x1 = X[1]
        x2 = X[2]
        x3 = X[3]
        self.assertNotIn(X.b, x1._vals_)
        self.assertNotIn(X.b, x2._vals_)
        self.assertNotIn(X.b, x3._vals_)
        b = x1.b
        self.assertIn(X.b, x1._vals_)
        self.assertNotIn(X.b, x2._vals_)
        self.assertNotIn(X.b, x3._vals_)

    @db_session
    def test_lazy_3(self):  # coverage of https://github.com/ponyorm/pony/issues/49
        X = self.X
        x1 = X.get(b='first')
        self.assertTrue(X._bits_[X.b] & x1._rbits_)
        self.assertIn(X.b, x1._vals_)

    @db_session
    def test_lazy_4(self):  # coverage of https://github.com/ponyorm/pony/issues/49
        X = self.X
        result = select(x for x in X if x.b == 'first')[:]
        for x in result:
            self.assertTrue(X._bits_[X.b] & x._rbits_)
            self.assertIn(X.b, x._vals_)

    @db_session
    def test_lazy_5(self):  # coverage of https://github.com/ponyorm/pony/issues/49
        X = self.X
        result = select(x for x in X if x.b == 'first' if count() > 0)[:]
        for x in result:
            self.assertFalse(X._bits_[X.b] & x._rbits_)
            self.assertNotIn(X.b, x._vals_)
