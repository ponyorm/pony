import unittest
from datetime import date
from pony.orm import *
from testutils import *

db = TestDatabase('sqlite', ':memory:')

class Entity1(db.Entity):
	a = PrimaryKey(int)
	b = Required(date)

class TestDate(unittest.TestCase):
    def testCreate(self):
        e1 = Entity1(a=1, b=date(2011, 10, 20))
        print db.last_sql

if __name__ == '__main__':
    unittest.main()