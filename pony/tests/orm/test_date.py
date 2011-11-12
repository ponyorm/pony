import unittest
from datetime import date
from pony.orm import *
from testutils import *

db = TestDatabase('sqlite', ':memory:')

class Entity1(db.Entity):
	a = PrimaryKey(int)
	b = Required(date)

db.generate_mapping(create_tables=True)

Entity1(a=1, b=date(2009, 10, 20))
Entity1(a=2, b=date(2010, 10, 21))
Entity1(a=3, b=date(2011, 11, 22))
commit()

class TestDate(unittest.TestCase):
    def setUp(self):
        rollback()
    def testCreate(self):
        e1 = Entity1(a=4, b=date(2011, 10, 20))        
        self.assert_(True)
    def testYear(self):
        result = select(e for e in Entity1 if e.b.year > 2009).all()
        self.assertEquals(len(result), 2)
    def testMonth(self):
        result = select(e for e in Entity1 if e.b.month == 10).all()
        self.assertEquals(len(result), 2)
    def testDay(self):
        result = select(e for e in Entity1 if e.b.day == 22).all()
        self.assertEquals(len(result), 1)

if __name__ == '__main__':
    unittest.main()