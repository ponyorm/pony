from __future__ import absolute_import, print_function, division
from pony.py23compat import PY2, PYPY, PYPY2

import sys, unittest
from datetime import date, datetime
from decimal import Decimal

from pony.orm.core import *
from pony.orm.sqltranslation import IncomparableTypesError
from pony.orm.tests.testutils import *
from pony.orm.tests import setup_database, teardown_database

db = Database()

class Student(db.Entity):
    id = PrimaryKey(int)
    name = Required(unicode)
    dob = Required(date)
    last_visit = Required(datetime)
    scholarship = Required(Decimal, 6, 2)
    phd = Required(bool)
    group = Required('Group')

class Group(db.Entity):
    number = PrimaryKey(int)
    students = Set(Student)


class TestFuncMonad(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_database(db)
        with db_session:
            g1 = Group(number=1)
            g2 = Group(number=2)

            Student(id=1, name="AA", dob=date(1981, 1, 1), last_visit=datetime(2011, 1, 1, 11, 11, 11),
                    scholarship=Decimal("0"), phd=True, group=g1)

            Student(id=2, name="BB", dob=date(1982, 2, 2), last_visit=datetime(2011, 2, 2, 12, 12, 12),
                    scholarship=Decimal("202.2"), phd=True, group=g1)

            Student(id=3, name="CC", dob=date(1983, 3, 3), last_visit=datetime(2011, 3, 3, 13, 13, 13),
                    scholarship=Decimal("303.3"), phd=False, group=g1)

            Student(id=4, name="DD", dob=date(1984, 4, 4), last_visit=datetime(2011, 4, 4, 14, 14, 14),
                    scholarship=Decimal("404.4"), phd=False, group=g2)

            Student(id=5, name="EE", dob=date(1985, 5, 5), last_visit=datetime(2011, 5, 5, 15, 15, 15),
                    scholarship=Decimal("505.5"), phd=False, group=g2)
    @classmethod
    def tearDownClass(cls):
        teardown_database(db)
    def setUp(self):
        rollback()
        db_session.__enter__()
    def tearDown(self):
        rollback()
        db_session.__exit__()
    def test_minmax1(self):
        result = set(select(s for s in Student if max(s.id, 3) == 3 ))
        self.assertEqual(result, {Student[1], Student[2], Student[3]})
    def test_minmax2(self):
        result = set(select(s for s in Student if min(s.id, 3) == 3 ))
        self.assertEqual(result, {Student[4], Student[5], Student[3]})
    def test_minmax3(self):
        result = set(select(s for s in Student if max(s.name, "CC") == "CC" ))
        self.assertEqual(result, {Student[1], Student[2], Student[3]})
    def test_minmax4(self):
        result = set(select(s for s in Student if min(s.name, "CC") == "CC" ))
        self.assertEqual(result, {Student[4], Student[5], Student[3]})
    def test_minmax5(self):
        x = chr(128)
        try: result = set(select(s for s in Student if min(s.name, x) == "CC" ))
        except TypeError as e:
            self.assertTrue(PY2 and e.args[0] == "The bytestring '\\x80' contains non-ascii symbols. Try to pass unicode string instead")
        else: self.assertFalse(PY2)
    def test_minmax6(self):
        x = chr(128)
        try: result = set(select(s for s in Student if min(s.name, x, "CC") == "CC" ))
        except TypeError as e:
            self.assertTrue(PY2 and e.args[0] == "The bytestring '\\x80' contains non-ascii symbols. Try to pass unicode string instead")
        else: self.assertFalse(PY2)
    def test_minmax7(self):
        result = set(select(s for s in Student if min(s.phd, 2) == 2 ))
    def test_date_func1(self):
        result = set(select(s for s in Student if s.dob >= date(1983, 3, 3)))
        self.assertEqual(result, {Student[3], Student[4], Student[5]})
    # @raises_exception(ExprEvalError, "date(1983, 'three', 3) raises TypeError: an integer is required")
    @raises_exception(TypeError, "'month' argument of date(year, month, day) function must be of 'int' type. "
                                 "Got: '%s'" % unicode.__name__)
    def test_date_func2(self):
        result = set(select(s for s in Student if s.dob >= date(1983, 'three', 3)))
    # @raises_exception(NotImplementedError)
    # def test_date_func3(self):
    #     d = 3
    #     result = set(select(s for s in Student if s.dob >= date(1983, d, 3)))
    def test_datetime_func1(self):
        result = set(select(s for s in Student if s.last_visit >= date(2011, 3, 3)))
        self.assertEqual(result, {Student[3], Student[4], Student[5]})
    def test_datetime_func2(self):
        result = set(select(s for s in Student if s.last_visit >= datetime(2011, 3, 3)))
        self.assertEqual(result, {Student[3], Student[4], Student[5]})
    def test_datetime_func3(self):
        result = set(select(s for s in Student if s.last_visit >= datetime(2011, 3, 3, 13, 13, 13)))
        self.assertEqual(result, {Student[3], Student[4], Student[5]})
    # @raises_exception(ExprEvalError, "datetime(1983, 'three', 3) raises TypeError: an integer is required")
    @raises_exception(TypeError, "'month' argument of datetime(...) function must be of 'int' type. "
                                 "Got: '%s'" % unicode.__name__)
    def test_datetime_func4(self):
        result = set(select(s for s in Student if s.last_visit >= datetime(1983, 'three', 3)))
    # @raises_exception(NotImplementedError)
    # def test_datetime_func5(self):
    #     d = 3
    #     result = set(select(s for s in Student if s.last_visit >= date(1983, d, 3)))
    def test_datetime_now1(self):
        result = set(select(s for s in Student if s.dob < date.today()))
        self.assertEqual(result, {Student[1], Student[2], Student[3], Student[4], Student[5]})
    @raises_exception(ExprEvalError, "`1 < datetime.now()` raises TypeError: " + (
        "can't compare 'datetime' to 'int'" if PYPY2 else
        "'<' not supported between instances of 'int' and 'datetime'" if PYPY and sys.version_info >= (3, 6) else
        "unorderable types: int < datetime" if PYPY else
        "can't compare datetime.datetime to int" if PY2 else
        "unorderable types: int() < datetime.datetime()" if sys.version_info < (3, 6) else
        "'<' not supported between instances of 'int' and 'datetime.datetime'"))
    def test_datetime_now2(self):
        select(s for s in Student if 1 < datetime.now())
    def test_datetime_now3(self):
        result = set(select(s for s in Student if s.dob < datetime.today()))
        self.assertEqual(result, {Student[1], Student[2], Student[3], Student[4], Student[5]})
    def test_decimal_func(self):
        result = set(select(s for s in Student if s.scholarship >= Decimal("303.3")))
        self.assertEqual(result, {Student[3], Student[4], Student[5]})
    def test_concat_1(self):
        result = set(select(concat(s.name, ':', s.dob.year, ':', s.scholarship) for s in Student))
        if db.provider.dialect == 'PostgreSQL':
            self.assertEqual(result, {'AA:1981:0.00', 'BB:1982:202.20', 'CC:1983:303.30', 'DD:1984:404.40', 'EE:1985:505.50'})
        else:
            self.assertEqual(result, {'AA:1981:0', 'BB:1982:202.2', 'CC:1983:303.3', 'DD:1984:404.4', 'EE:1985:505.5'})
    @raises_exception(TranslationError, 'Invalid argument of concat() function: g.students')
    def test_concat_2(self):
        result = set(select(concat(g.number, g.students) for g in Group))

if __name__ == '__main__':
    unittest.main()
