import unittest

from enum import Enum
from pony.orm import Database, Required, db_session, select

db = Database('sqlite', ':memory:')


class Result(Enum):
    SUCCESS = 0
    FAILURE = 1
    UNKNOWN = 2


class Test(db.Entity):
    name = Required(str)
    result = Required(Result)

db.generate_mapping(create_tables=True)

with db_session:
    Test(name="one", result=Result.SUCCESS)
    Test(name="two", result=Result.FAILURE)
    Test(name="three", result=Result.UNKNOWN)


class TestEnum(unittest.TestCase):
    def test_enum_1(self):
        with db_session:
            query = select(test for test in Test if test.result == Result.SUCCESS)
            self.assertEqual(1, query.count())
            self.assertEqual(query.first().result, Result.SUCCESS)

    def test_enum_2(self):
        with db_session:
            query = select(test for test in Test)
            query = query.filter(lambda test: test.result == Result.FAILURE)
            self.assertEqual(1, query.count())
            self.assertEqual(query.first().result, Result.FAILURE)

    def test_enum_3(self):
        with db_session:
            query = select(test for test in Test)
            query = query.where('test.result == Result.UNKNOWN')
            self.assertEqual(1, query.count())
            self.assertEqual(query.first().result, Result.UNKNOWN)


if __name__ == '__main__':
    unittest.main()
