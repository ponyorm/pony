from __future__ import absolute_import, print_function, division

import unittest

from pony.orm.core import *
from pony.orm.core import local
from pony.orm.tests.testutils import *
from pony.orm.tests import setup_database, teardown_database


class TestGeneratorDbSession(unittest.TestCase):
    def setUp(self):
        db = Database()
        class Account(db.Entity):
            id = PrimaryKey(int)
            amount = Required(int)
        setup_database(db)

        self.db = db
        self.Account = Account

        with db_session:
            a1 = Account(id=1, amount=1000)
            a2 = Account(id=2, amount=2000)
            a3 = Account(id=3, amount=3000)

    def tearDown(self):
        teardown_database(self.db)
        assert local.db_session is None
        self.db = self.Account = None

    @raises_exception(TypeError, 'db_session with `retry` option cannot be applied to generator function')
    def test1(self):
        @db_session(retry=3)
        def f(): yield

    @raises_exception(TypeError, 'db_session with `ddl` option cannot be applied to generator function')
    def test2(self):
        @db_session(ddl=True)
        def f(): yield

    @raises_exception(TypeError, 'db_session with `serializable` option cannot be applied to generator function')
    def test3(self):
        @db_session(serializable=True)
        def f(): yield

    def test4(self):
        @db_session(immediate=True)
        def f(): yield

    @raises_exception(TransactionError, '@db_session-wrapped generator cannot be used inside another db_session')
    def test5(self):
        @db_session
        def f(): yield
        with db_session:
            next(f())

    def test6(self):
        @db_session
        def f():
            x = local.db_session
            self.assertTrue(x is not None)

            yield self.db._get_cache()
            self.assertEqual(local.db_session, x)

            a1 = self.Account[1]
            yield a1.amount
            self.assertEqual(local.db_session, x)

            a2 = self.Account[2]
            yield a2.amount

        gen = f()
        cache = next(gen)
        self.assertTrue(cache.is_alive)
        self.assertEqual(local.db_session, None)

        amount = next(gen)
        self.assertEqual(amount, 1000)
        self.assertEqual(local.db_session, None)

        amount = next(gen)
        self.assertEqual(amount, 2000)
        self.assertEqual(local.db_session, None)

        try: next(gen)
        except StopIteration:
            self.assertFalse(cache.is_alive)
        else:
            self.fail()

    def test7(self):
        @db_session
        def f(id1):
            a1 = self.Account[id1]
            id2 = yield a1.amount
            a2 = self.Account[id2]
            amount = yield a2.amount
            a1.amount -= amount
            a2.amount += amount
            commit()

        gen = f(1)

        amount1 = next(gen)
        self.assertEqual(amount1, 1000)

        amount2 = gen.send(2)
        self.assertEqual(amount2, 2000)

        try:
            gen.send(100)
        except StopIteration:
            pass
        else:
            self.fail()

        with db_session:
            a1 = self.Account[1]
            self.assertEqual(a1.amount, 900)
            a2 = self.Account[2]
            self.assertEqual(a2.amount, 2100)

    @raises_exception(TransactionError, 'You need to manually commit() changes before suspending the generator')
    def test8(self):
        @db_session
        def f(id1):
            a1 = self.Account[id1]
            a1.amount += 100
            yield a1.amount

        for amount in f(1):
            pass

    def test9(self):
        @db_session
        def f(id1):
            a1 = self.Account[id1]
            a1.amount += 100
            commit()
            yield a1.amount

        for amount in f(1):
            pass

    def test10(self):
        @db_session
        def f(id1):
            a1 = self.Account[id1]
            yield a1.amount
            a1.amount += 100

        with db_session:
            a = self.Account[1].amount
        for amount in f(1):
            pass
        with db_session:
            b = self.Account[1].amount

        self.assertEqual(b, a + 100)

    def test12(self):
        @db_session
        def f(id1):
            a1 = self.Account[id1]
            yield a1.amount

        gen = f(1)
        next(gen)
        gen.close()

    @raises_exception(TypeError, 'error message')
    def test13(self):
        @db_session
        def f(id1):
            a1 = self.Account[id1]
            yield a1.amount

        gen = f(1)
        next(gen)
        gen.throw(TypeError('error message'))

if __name__ == '__main__':
    unittest.main()
