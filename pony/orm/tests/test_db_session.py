from __future__ import with_statement

import unittest
from datetime import date
from decimal import Decimal
from itertools import count

from pony.orm.core import *
from testutils import *

class TestQuery(unittest.TestCase):
    def setUp(self):
        self.db = Database('sqlite', ':memory:')
        class X(self.db.Entity):
            a = Required(int)
            b = Optional(int)
        self.X = X
        self.db.generate_mapping(create_tables=True)
        with db_session:
            x1 = X(a=1, b=1)
            x2 = X(a=2, b=2)

    @raises_exception(TypeError, "Pass only keyword arguments to db_session or use db_session as decorator")
    def test_db_session_1(self):
        db_session(1, 2, 3)

    @raises_exception(TypeError, "Pass only keyword arguments to db_session or use db_session as decorator")
    def test_db_session_2(self):
        db_session(1, 2, 3, a=10, b=20)

    def test_db_session_3(self):
        self.assertIs(db_session, db_session())

    def test_db_session_4(self):
        with db_session:
            with db_session:
                self.X(a=3, b=3)
        with db_session:
            self.assertEqual(count(x for x in self.X), 3)

    def test_db_session_decorator_1(self):
        @db_session
        def test():
            self.X(a=3, b=3)
        test()
        with db_session:
            self.assertEqual(count(x for x in self.X), 3)

    def test_db_session_decorator_2(self):
        @db_session
        def test():
            self.X(a=3, b=3)
            1/0
        try:
            test()
        except ZeroDivisionError:
            with db_session:
                self.assertEqual(count(x for x in self.X), 2)
        else:
            self.fail()

    def test_db_session_decorator_3(self):
        @db_session(allowed_exceptions=[TypeError])
        def test():
            self.X(a=3, b=3)
            1/0
        try:
            test()
        except ZeroDivisionError:
            with db_session:
                self.assertEqual(count(x for x in self.X), 2)
        else:
            self.fail()

    def test_db_session_decorator_4(self):
        @db_session(allowed_exceptions=[ZeroDivisionError])
        def test():
            self.X(a=3, b=3)
            1/0
        try:
            test()
        except ZeroDivisionError:
            with db_session:
                self.assertEqual(count(x for x in self.X), 3)
        else:
            self.fail()

    @raises_exception(TypeError, "'retry' parameter of db_session must be of integer type. Got: <type 'str'>")
    def test_db_session_decorator_5(self):
        @db_session(retry='foobar')
        def test():
            pass

    @raises_exception(TypeError, "'retry' parameter of db_session must not be negative. Got: -1")
    def test_db_session_decorator_6(self):
        @db_session(retry=-1)
        def test():
            pass

    def test_db_session_decorator_7(self):
        counter = count().next
        @db_session(retry_exceptions=[ZeroDivisionError])
        def test():
            counter()
            self.X(a=3, b=3)
            1/0
        try:
            test()
        except ZeroDivisionError:
            self.assertEqual(counter(), 1)
            with db_session:
                self.assertEqual(count(x for x in self.X), 2)
        else:
            self.fail()

    def test_db_session_decorator_8(self):
        counter = count().next
        @db_session(retry=1, retry_exceptions=[ZeroDivisionError])
        def test():
            counter()
            self.X(a=3, b=3)
            1/0
        try:
            test()
        except ZeroDivisionError:
            self.assertEqual(counter(), 2)
            with db_session:
                self.assertEqual(count(x for x in self.X), 2)
        else:
            self.fail()

    def test_db_session_decorator_9(self):
        counter = count().next
        @db_session(retry=5, retry_exceptions=[ZeroDivisionError])
        def test():
            counter()
            self.X(a=3, b=3)
            1/0
        try:
            test()
        except ZeroDivisionError:
            self.assertEqual(counter(), 6)
            with db_session:
                self.assertEqual(count(x for x in self.X), 2)
        else:
            self.fail()

    def test_db_session_decorator_10(self):
        counter = count().next
        @db_session(retry=3, retry_exceptions=[TypeError])
        def test():
            counter()
            self.X(a=3, b=3)
            1/0
        try:
            test()
        except ZeroDivisionError:
            self.assertEqual(counter(), 1)
            with db_session:
                self.assertEqual(count(x for x in self.X), 2)
        else:
            self.fail()

    def test_db_session_decorator_11(self):
        counter = count().next
        @db_session(retry=5, retry_exceptions=[ZeroDivisionError])
        def test():
            i = counter()
            self.X(a=3, b=3)
            if i < 2: 1/0
        try:
            test()
        except ZeroDivisionError:
            self.fail()
        else:
            self.assertEqual(counter(), 3)
            with db_session:
                self.assertEqual(count(x for x in self.X), 3)

    @raises_exception(TypeError, "The same exception ZeroDivisionError cannot be specified "
                                 "in both allowed and retry exception lists simultaneously")
    def test_db_session_decorator_12(self):
        @db_session(retry=3, retry_exceptions=[ZeroDivisionError],
                             allowed_exceptions=[ZeroDivisionError])
        def test():
            pass

    def test_db_session_decorator_13(self):
        @db_session(allowed_exceptions=lambda e: isinstance(e, ZeroDivisionError))
        def test():
            self.X(a=3, b=3)
            1/0
        try:
            test()
        except ZeroDivisionError:
            with db_session:
                self.assertEqual(count(x for x in self.X), 3)
        else:
            self.fail()
        
    def test_db_session_decorator_14(self):
        @db_session(allowed_exceptions=lambda e: isinstance(e, TypeError))
        def test():
            self.X(a=3, b=3)
            1/0
        try:
            test()
        except ZeroDivisionError:
            with db_session:
                self.assertEqual(count(x for x in self.X), 2)
        else:
            self.fail()

    def test_db_session_decorator_15(self):
        counter = count().next
        @db_session(retry=3, retry_exceptions=lambda e: isinstance(e, ZeroDivisionError))
        def test():
            i = counter()
            self.X(a=3, b=3)
            1/0
        try:
            test()
        except ZeroDivisionError:
            self.assertEqual(counter(), 4)
            with db_session:
                self.assertEqual(count(x for x in self.X), 2)
        else:
            self.fail()

    def test_db_session_manager_1(self):
        with db_session:
            self.X(a=3, b=3)
        with db_session:
            self.assertEqual(count(x for x in self.X), 3)

    @raises_exception(TypeError, "@db_session can accept 'retry' parameter "
                      "only when used as decorator and not as context manager")
    def test_db_session_manager_2(self):
        with db_session(retry=3):
            self.X(a=3, b=3)

    def test_db_session_manager_3(self):
        try:
            with db_session(allowed_exceptions=[TypeError]):
                self.X(a=3, b=3)
                1/0
        except ZeroDivisionError:
            with db_session:
                self.assertEqual(count(x for x in self.X), 2)
        else:
            self.fail()

    def test_db_session_manager_4(self):
        try:
            with db_session(allowed_exceptions=[ZeroDivisionError]):
                self.X(a=3, b=3)
                1/0
        except ZeroDivisionError:
            with db_session:
                self.assertEqual(count(x for x in self.X), 3)
        else:
            self.fail()

    @raises_exception(TypeError, "@db_session can accept 'ddl' parameter "
                      "only when used as decorator and not as context manager")
    def test_db_session_ddl_1(self):
        with db_session(ddl=True):
            pass

    @raises_exception(TransactionError, "test() cannot be called inside of db_session")
    def test_db_session_ddl_2(self):
        @db_session(ddl=True)
        def test():
            pass
        with db_session:
            test()

    def test_db_session_ddl_3(self):
        @db_session(ddl=True)
        def test():
            pass
        test()

if __name__ == '__main__':
    unittest.main()
