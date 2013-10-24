from __future__ import with_statement

import unittest
from datetime import date, datetime
from pony.orm.core import *
from testutils import *

db = Database('sqlite', ':memory:')

class Entity1(db.Entity):
	a = PrimaryKey(int)
	b = Required(date)
	c = Required(datetime)

db.generate_mapping(create_tables=True)

with db_session:
    Entity1(a=1, b=date(2009, 10, 20), c=datetime(2009, 10, 20, 10, 20, 30))
    Entity1(a=2, b=date(2010, 10, 21), c=datetime(2010, 10, 21, 10, 21, 31))
    Entity1(a=3, b=date(2011, 11, 22), c=datetime(2011, 11, 22, 10, 20, 32))

class TestDate(unittest.TestCase):
    def setUp(self):
        rollback()
        db_session.__enter__()
    def tearDown(self):
        rollback()
        db_session.__exit__()
    def test_create(self):
        e1 = Entity1(a=4, b=date(2011, 10, 20), c=datetime(2009, 10, 20, 10, 20, 30))
        self.assert_(True)
    def test_date_year(self):
        result = select(e for e in Entity1 if e.b.year > 2009)
        self.assertEqual(len(result), 2)
    def test_date_month(self):
        result = select(e for e in Entity1 if e.b.month == 10)
        self.assertEqual(len(result), 2)
    def test_date_day(self):
        result = select(e for e in Entity1 if e.b.day == 22)
        self.assertEqual(len(result), 1)
    def test_datetime_year(self):
        result = select(e for e in Entity1 if e.c.year > 2009)
        self.assertEqual(len(result), 2)
    def test_datetime_month(self):
        result = select(e for e in Entity1 if e.c.month == 10)
        self.assertEqual(len(result), 2)
    def test_datetime_day(self):
        result = select(e for e in Entity1 if e.c.day == 22)
        self.assertEqual(len(result), 1)
    def test_datetime_hour(self):
        result = select(e for e in Entity1 if e.c.hour == 10)
        self.assertEqual(len(result), 3)
    def test_datetime_minute(self):
        result = select(e for e in Entity1 if e.c.minute == 20)
        self.assertEqual(len(result), 2)
    def test_datetime_second(self):
        result = select(e for e in Entity1 if e.c.second == 30)
        self.assertEqual(len(result), 1)

if __name__ == '__main__':
    unittest.main()
