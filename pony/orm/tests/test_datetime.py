from __future__ import absolute_import, print_function, division
from pony.py23compat import PY2

import unittest
from datetime import date, datetime, timedelta

from pony.orm.core import *
from pony.orm.tests.testutils import *
from pony.orm.tests import setup_database, teardown_database

db = Database()


class Entity1(db.Entity):
    id = PrimaryKey(int)
    d = Required(date)
    dt = Required(datetime)


class TestDate(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_database(db)
        with db_session:
            Entity1(id=1, d=date(2009, 10, 20), dt=datetime(2009, 10, 20, 10, 20, 30))
            Entity1(id=2, d=date(2010, 10, 21), dt=datetime(2010, 10, 21, 10, 21, 31))
            Entity1(id=3, d=date(2011, 11, 22), dt=datetime(2011, 11, 22, 10, 20, 32))

    @classmethod
    def tearDownClass(cls):
        teardown_database(db)

    def setUp(self):
        rollback()
        db_session.__enter__()

    def tearDown(self):
        rollback()
        db_session.__exit__()

    def test_create(self):
        e1 = Entity1(id=4, d=date(2011, 10, 20), dt=datetime(2009, 10, 20, 10, 20, 30))

    def test_date_year(self):
        result = select(e for e in Entity1 if e.d.year > 2009)
        self.assertEqual(len(result), 2)

    def test_date_month(self):
        result = select(e for e in Entity1 if e.d.month == 10)
        self.assertEqual(len(result), 2)

    def test_date_day(self):
        result = select(e for e in Entity1 if e.d.day == 22)
        self.assertEqual(len(result), 1)

    def test_datetime_year(self):
        result = select(e for e in Entity1 if e.dt.year > 2009)
        self.assertEqual(len(result), 2)

    def test_datetime_month(self):
        result = select(e for e in Entity1 if e.dt.month == 10)
        self.assertEqual(len(result), 2)

    def test_datetime_day(self):
        result = select(e for e in Entity1 if e.dt.day == 22)
        self.assertEqual(len(result), 1)

    def test_datetime_hour(self):
        result = select(e for e in Entity1 if e.dt.hour == 10)
        self.assertEqual(len(result), 3)

    def test_datetime_minute(self):
        result = select(e for e in Entity1 if e.dt.minute == 20)
        self.assertEqual(len(result), 2)

    def test_datetime_second(self):
        result = select(e for e in Entity1 if e.dt.second == 30)
        self.assertEqual(len(result), 1)

    def test_date_sub_date(self):
        dt = date(2012, 1, 1)
        result = select(e.id for e in Entity1 if dt - e.d > timedelta(days=500))
        self.assertEqual(set(result), {1})

    def test_datetime_sub_datetime(self):
        dt = datetime(2012, 1, 1, 10, 20, 30)
        result = select(e.id for e in Entity1 if dt - e.dt > timedelta(days=500))
        self.assertEqual(set(result), {1})

    def test_date_sub_timedelta_param(self):
        td = timedelta(days=500)
        result = select(e.id for e in Entity1 if e.d - td < date(2009, 1, 1))
        self.assertEqual(set(result), {1})

    def test_date_sub_const_timedelta(self):
        result = select(e.id for e in Entity1 if e.d - timedelta(days=500) < date(2009, 1, 1))
        self.assertEqual(set(result), {1})

    def test_datetime_sub_timedelta_param(self):
        td = timedelta(days=500)
        result = select(e.id for e in Entity1 if e.dt - td < datetime(2009, 1, 1, 10, 20, 30))
        self.assertEqual(set(result), {1})

    def test_datetime_sub_const_timedelta(self):
        result = select(e.id for e in Entity1 if e.dt - timedelta(days=500) < datetime(2009, 1, 1, 10, 20, 30))
        self.assertEqual(set(result), {1})

    def test_date_add_timedelta_param(self):
        td = timedelta(days=500)
        result = select(e.id for e in Entity1 if e.d + td > date(2013, 1, 1))
        self.assertEqual(set(result), {3})

    def test_date_add_const_timedelta(self):
        result = select(e.id for e in Entity1 if e.d + timedelta(days=500) > date(2013, 1, 1))
        self.assertEqual(set(result), {3})

    def test_datetime_add_timedelta_param(self):
        td = timedelta(days=500)
        result = select(e.id for e in Entity1 if e.dt + td > date(2013, 1, 1))
        self.assertEqual(set(result), {3})

    def test_datetime_add_const_timedelta(self):
        result = select(e.id for e in Entity1 if e.dt + timedelta(days=500) > date(2013, 1, 1))
        self.assertEqual(set(result), {3})

    @raises_exception(TypeError, "Unsupported operand types 'date' and '%s' "
                                 "for operation '-' in expression: e.d - s" % ('unicode' if PY2 else 'str'))
    def test_date_sub_error(self):
        s = 'hello'
        result = select(e.id for e in Entity1 if e.d - s > timedelta(days=500))
        self.assertEqual(set(result), {1})

    @raises_exception(TypeError, "Unsupported operand types 'datetime' and '%s' "
                                 "for operation '-' in expression: e.dt - s" % ('unicode' if PY2 else 'str'))
    def test_datetime_sub_error(self):
        s = 'hello'
        result = select(e.id for e in Entity1 if e.dt - s > timedelta(days=500))
        self.assertEqual(set(result), {1})


if __name__ == '__main__':
    unittest.main()
