from __future__ import absolute_import, print_function, division

import unittest, warnings
from datetime import date
from decimal import Decimal
from itertools import count

from pony.orm.core import *
from pony.orm.tests.testutils import *
from pony.orm.tests import setup_database, teardown_database


class TestDBSession(unittest.TestCase):
    def setUp(self):
        self.db = Database()
        class X(self.db.Entity):
            a = PrimaryKey(int)
            b = Optional(int)
        self.X = X
        setup_database(self.db)
        with db_session:
            x1 = X(a=1, b=1)
            x2 = X(a=2, b=2)

    def tearDown(self):
        if self.db.provider.dialect != 'SQLite':
            teardown_database(self.db)

    @raises_exception(TypeError, "Pass only keyword arguments to db_session or use db_session as decorator")
    def test_db_session_1(self):
        db_session(1, 2, 3)

    @raises_exception(TypeError, "Pass only keyword arguments to db_session or use db_session as decorator")
    def test_db_session_2(self):
        db_session(1, 2, 3, a=10, b=20)

    def test_db_session_3(self):
        self.assertTrue(db_session is db_session())

    def test_db_session_4(self):
        # Nested db_sessions are ignored
        with db_session:
            with db_session:
                self.X(a=3, b=3)
        with db_session:
            self.assertEqual(count(x for x in self.X), 3)

    def test_db_session_decorator_1(self):
        # Should commit changes on exit from db_session
        @db_session
        def test():
            self.X(a=3, b=3)
        test()
        with db_session:
            self.assertEqual(count(x for x in self.X), 3)

    def test_db_session_decorator_2(self):
        # Should rollback changes if an exception is occurred
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
        # Should rollback changes if the exception is not in the list of allowed exceptions
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
        # Should commit changes if the exception is in the list of allowed exceptions
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

    def test_allowed_exceptions_1(self):
        # allowed_exceptions may be callable, should commit if nonzero
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

    def test_allowed_exceptions_2(self):
        # allowed_exceptions may be callable, should rollback if not nonzero
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

    @raises_exception(TypeError, "'retry' parameter of db_session must be of integer type. Got: %r" % str)
    def test_retry_1(self):
        @db_session(retry='foobar')
        def test():
            pass

    @raises_exception(TypeError, "'retry' parameter of db_session must not be negative. Got: -1")
    def test_retry_2(self):
        @db_session(retry=-1)
        def test():
            pass

    def test_retry_3(self):
        # Should not to do retry until retry count is specified
        counter = count()
        @db_session(retry_exceptions=[ZeroDivisionError])
        def test():
            next(counter)
            self.X(a=3, b=3)
            1/0
        try:
            test()
        except ZeroDivisionError:
            self.assertEqual(next(counter), 1)
            with db_session:
                self.assertEqual(count(x for x in self.X), 2)
        else:
            self.fail()

    def test_retry_4(self):
        # Should rollback & retry 1 time if retry=1
        counter = count()
        @db_session(retry=1, retry_exceptions=[ZeroDivisionError])
        def test():
            next(counter)
            self.X(a=3, b=3)
            1/0
        try:
            test()
        except ZeroDivisionError:
            self.assertEqual(next(counter), 2)
            with db_session:
                self.assertEqual(count(x for x in self.X), 2)
        else:
            self.fail()

    def test_retry_5(self):
        # Should rollback & retry N time if retry=N
        counter = count()
        @db_session(retry=5, retry_exceptions=[ZeroDivisionError])
        def test():
            next(counter)
            self.X(a=3, b=3)
            1/0
        try:
            test()
        except ZeroDivisionError:
            self.assertEqual(next(counter), 6)
            with db_session:
                self.assertEqual(count(x for x in self.X), 2)
        else:
            self.fail()

    def test_retry_6(self):
        # Should not retry if the exception not in the list of retry_exceptions
        counter = count()
        @db_session(retry=3, retry_exceptions=[TypeError])
        def test():
            next(counter)
            self.X(a=3, b=3)
            1/0
        try:
            test()
        except ZeroDivisionError:
            self.assertEqual(next(counter), 1)
            with db_session:
                self.assertEqual(count(x for x in self.X), 2)
        else:
            self.fail()

    def test_retry_7(self):
        # Should commit after successful retrying
        counter = count()
        @db_session(retry=5, retry_exceptions=[ZeroDivisionError])
        def test():
            i = next(counter)
            self.X(a=3, b=3)
            if i < 2: 1/0
        try:
            test()
        except ZeroDivisionError:
            self.fail()
        else:
            self.assertEqual(next(counter), 3)
            with db_session:
                self.assertEqual(count(x for x in self.X), 3)

    @raises_exception(TypeError, "The same exception ZeroDivisionError cannot be specified "
                                 "in both allowed and retry exception lists simultaneously")
    def test_retry_8(self):
        @db_session(retry=3, retry_exceptions=[ZeroDivisionError],
                             allowed_exceptions=[ZeroDivisionError])
        def test():
            pass

    def test_retry_9(self):
        # retry_exceptions may be callable, should retry if nonzero
        counter = count()
        @db_session(retry=3, retry_exceptions=lambda e: isinstance(e, ZeroDivisionError))
        def test():
            i = next(counter)
            self.X(a=3, b=3)
            1/0
        try:
            test()
        except ZeroDivisionError:
            self.assertEqual(next(counter), 4)
            with db_session:
                self.assertEqual(count(x for x in self.X), 2)
        else:
            self.fail()

    def test_retry_10(self):
        # Issue 313: retry on exception raised during db_session.__exit__
        retries = count()
        @db_session(retry=3)
        def test():
            next(retries)
            self.X(a=1, b=1)
        try:
            test()
        except TransactionIntegrityError:
            self.assertEqual(next(retries), 4)
        else:
            self.fail()

    @raises_exception(PonyRuntimeWarning, '@db_session decorator with `retry=3` option is ignored for test() function '
                                          'because it is called inside another db_session')
    def test_retry_11(self):
        @db_session(retry=3)
        def test():
            pass
        with warnings.catch_warnings():
            warnings.simplefilter('error', PonyRuntimeWarning)
            with db_session:
                test()

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
        # Should rollback if the exception is not in the list of allowed_exceptions
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
        # Should commit if the exception is in the list of allowed_exceptions
        try:
            with db_session(allowed_exceptions=[ZeroDivisionError]):
                self.X(a=3, b=3)
                1/0
        except ZeroDivisionError:
            with db_session:
                self.assertEqual(count(x for x in self.X), 3)
        else:
            self.fail()

    # restriction removed in 0.7.3:
    # @raises_exception(TypeError, "@db_session can accept 'ddl' parameter "
    #                   "only when used as decorator and not as context manager")
    def test_db_session_ddl_1(self):
        with db_session(ddl=True):
            pass

    def test_db_session_ddl_1a(self):
        with db_session(ddl=True):
              with db_session(ddl=True):
                  pass

    def test_db_session_ddl_1b(self):
        with db_session(ddl=True):
              with db_session:
                  pass

    @raises_exception(TransactionError, 'Cannot start ddl transaction inside non-ddl transaction')
    def test_db_session_ddl_1c(self):
        with db_session:
              with db_session(ddl=True):
                  pass

    @raises_exception(TransactionError, "@db_session-decorated test() function with `ddl` option "
                                        "cannot be called inside of another db_session")
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

    @raises_exception(ZeroDivisionError)
    def test_db_session_exceptions_1(self):
        def before_insert(self):
            1/0
        self.X.before_insert = before_insert
        with db_session:
            self.X(a=3, b=3)
            # Should raise ZeroDivisionError and not CommitException

    @raises_exception(ZeroDivisionError)
    def test_db_session_exceptions_2(self):
        def before_insert(self):
            1 / 0
        self.X.before_insert = before_insert
        with db_session:
            self.X(a=3, b=3)
            commit()
            # Should raise ZeroDivisionError and not CommitException

    @raises_exception(ZeroDivisionError)
    def test_db_session_exceptions_3(self):
        def before_insert(self):
            1 / 0
        self.X.before_insert = before_insert
        with db_session:
            self.X(a=3, b=3)
            db.commit()
            # Should raise ZeroDivisionError and not CommitException

    @raises_exception(ZeroDivisionError)
    def test_db_session_exceptions_4(self):
        with db_session:
            connection = self.db.get_connection()
            connection.close()
            1/0


db = Database()

class Group(db.Entity):
    id = PrimaryKey(int)
    major = Required(unicode)
    students = Set('Student')

class Student(db.Entity):
    name = Required(unicode)
    picture = Optional(buffer, lazy=True)
    group = Required('Group')

class TestDBSessionScope(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_database(db)
        with db_session:
            g1 = Group(id=1, major='Math')
            g2 = Group(id=2, major='Physics')
            s1 = Student(id=1, name='S1', group=g1)
            s2 = Student(id=2, name='S2', group=g1)
            s3 = Student(id=3, name='S3', group=g2)

    @classmethod
    def tearDownClass(cls):
        teardown_database(db)

    def setUp(self):
        rollback()

    def tearDown(self):
        rollback()

    def test1(self):
        with db_session:
            s1 = Student[1]
        name = s1.name

    @raises_exception(DatabaseSessionIsOver, 'Cannot load attribute Student[1].picture: the database session is over')
    def test2(self):
        with db_session:
            s1 = Student[1]
        picture = s1.picture

    @raises_exception(DatabaseSessionIsOver, 'Cannot load attribute Group[1].major: the database session is over')
    def test3(self):
        with db_session:
            s1 = Student[1]
        group_id = s1.group.id
        major = s1.group.major

    @raises_exception(DatabaseSessionIsOver, 'Cannot assign new value to Student[1].name: the database session is over')
    def test4(self):
        with db_session:
            s1 = Student[1]
        s1.name = 'New name'

    def test5(self):
        with db_session:
            g1 = Group[1]
        self.assertEqual(str(g1.students), 'StudentSet([...])')

    @raises_exception(DatabaseSessionIsOver, 'Cannot load collection Group[1].students: the database session is over')
    def test6(self):
        with db_session:
            g1 = Group[1]
        l = len(g1.students)

    @raises_exception(DatabaseSessionIsOver, 'Cannot change collection Group[1].students: the database session is over')
    def test7(self):
        with db_session:
            s1 = Student[1]
            g1 = Group[1]
        g1.students.remove(s1)

    @raises_exception(DatabaseSessionIsOver, 'Cannot change collection Group[1].students: the database session is over')
    def test8(self):
        with db_session:
            g2_students = Group[2].students
            g1 = Group[1]
        g1.students = g2_students

    @raises_exception(DatabaseSessionIsOver, 'Cannot change collection Group[1].students: the database session is over')
    def test9(self):
        with db_session:
            s3 = Student[3]
            g1 = Group[1]
        g1.students.add(s3)

    @raises_exception(DatabaseSessionIsOver, 'Cannot change collection Group[1].students: the database session is over')
    def test10(self):
        with db_session:
            g1 = Group[1]
        g1.students.clear()

    @raises_exception(DatabaseSessionIsOver, 'Cannot delete object Student[1]: the database session is over')
    def test11(self):
        with db_session:
            s1 = Student[1]
        s1.delete()

    @raises_exception(DatabaseSessionIsOver, 'Cannot change object Student[1]: the database session is over')
    def test12(self):
        with db_session:
            s1 = Student[1]
        s1.set(name='New name')

    def test_db_session_strict_1(self):
        with db_session(strict=True):
            s1 = Student[1]

    @raises_exception(DatabaseSessionIsOver, 'Cannot read value of Student[1].name: the database session is over')
    def test_db_session_strict_2(self):
        with db_session(strict=True):
            s1 = Student[1]
        name = s1.name

if __name__ == '__main__':
    unittest.main()
