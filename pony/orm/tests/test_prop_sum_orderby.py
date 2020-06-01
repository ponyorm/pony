from __future__ import absolute_import, print_function, division

import unittest

from pony.orm.core import *
from pony.orm.tests.testutils import *
from pony.orm.tests import setup_database, teardown_database

db = Database()

db = Database('sqlite', ':memory:')

class Product(db.Entity):
    id = PrimaryKey(int)
    name = Required(str)
    comments = Set('Comment')

    @property
    def sum_01(self):
        return coalesce(select(c.points for c in self.comments).sum(), 0)

    @property
    def sum_02(self):
        return coalesce(select(c.points for c in self.comments).sum(), 0.0)

    @property
    def sum_03(self):
        return coalesce(select(sum(c.points) for c in self.comments), 0)

    @property
    def sum_04(self):
        return coalesce(select(sum(c.points) for c in self.comments), 0.0)

    @property
    def sum_05(self):
        return sum(c.points for c in self.comments)

    @property
    def sum_06(self):
        return coalesce(sum(c.points for c in self.comments), 0)

    @property
    def sum_07(self):
        return coalesce(sum(c.points for c in self.comments), 0.0)

    @property
    def sum_08(self):
        return select(sum(c.points) for c in self.comments)

    @property
    def sum_09(self):
        return select(coalesce(sum(c.points), 0) for c in self.comments)

    @property
    def sum_10(self):
        return select(coalesce(sum(c.points), 0.0) for c in self.comments)

    @property
    def sum_11(self):
        return select(sum(c.points) for c in self.comments)

    @property
    def sum_12(self):
        return sum(self.comments.points)

    @property
    def sum_13(self):
        return coalesce(sum(self.comments.points), 0)

    @property
    def sum_14(self):
        return coalesce(sum(self.comments.points), 0.0)


class Comment(db.Entity):
    id = PrimaryKey(int)
    points = Required(int)
    product = Optional('Product')


class TestQuerySetMonad(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_database(db)
        with db_session:
            p1 = Product(id=1, name='P1')
            p2 = Product(id=2, name='P1', comments=[
                Comment(id=201, points=5)
            ])
            p3 = Product(id=3, name='P1', comments=[
                Comment(id=301, points=1), Comment(id=302, points=2)
            ])
            p4 = Product(id=4, name='P1', comments=[
                Comment(id=401, points=1), Comment(id=402, points=5), Comment(id=403, points=1)
            ])

    @classmethod
    def tearDownClass(cls):
        teardown_database(db)

    def setUp(self):
        rollback()
        db_session.__enter__()

    def tearDown(self):
        rollback()
        db_session.__exit__()

    def test_sum_01(self):
        q = list(Product.select().sort_by(lambda p: p.sum_01))
        result = [p.id for p in q]
        self.assertEqual(result, [1, 3, 2, 4])

    def test_sum_02(self):
        q = list(Product.select().sort_by(lambda p: p.sum_02))
        result = [p.id for p in q]
        self.assertEqual(result, [1, 3, 2, 4])

    def test_sum_03(self):
        q = list(Product.select().sort_by(lambda p: p.sum_03))
        result = [p.id for p in q]
        self.assertEqual(result, [1, 3, 2, 4])

    def test_sum_04(self):
        q = list(Product.select().sort_by(lambda p: p.sum_04))
        result = [p.id for p in q]
        self.assertEqual(result, [1, 3, 2, 4])

    def test_sum_05(self):
        q = list(Product.select().sort_by(lambda p: p.sum_05))
        result = [p.id for p in q]
        self.assertEqual(result, [1, 3, 2, 4])

    def test_sum_06(self):
        q = list(Product.select().sort_by(lambda p: p.sum_06))
        result = [p.id for p in q]
        self.assertEqual(result, [1, 3, 2, 4])

    def test_sum_07(self):
        q = list(Product.select().sort_by(lambda p: p.sum_07))
        result = [p.id for p in q]
        self.assertEqual(result, [1, 3, 2, 4])

    def test_sum_08(self):
        q = list(Product.select().sort_by(lambda p: p.sum_08))
        result = [p.id for p in q]
        self.assertEqual(result, [1, 3, 2, 4])

    def test_sum_09(self):
        q = list(Product.select().sort_by(lambda p: p.sum_09))
        result = [p.id for p in q]
        self.assertEqual(result, [1, 3, 2, 4])

    def test_sum_10(self):
        q = list(Product.select().sort_by(lambda p: p.sum_10))
        result = [p.id for p in q]
        self.assertEqual(result, [1, 3, 2, 4])

    def test_sum_11(self):
        q = list(Product.select().sort_by(lambda p: p.sum_11))
        result = [p.id for p in q]
        self.assertEqual(result, [1, 3, 2, 4])

    def test_sum_12(self):
        q = list(Product.select().sort_by(lambda p: p.sum_12))
        result = [p.id for p in q]
        self.assertEqual(result, [1, 3, 2, 4])

    def test_sum_13(self):
        q = list(Product.select().sort_by(lambda p: p.sum_13))
        result = [p.id for p in q]
        self.assertEqual(result, [1, 3, 2, 4])

    def test_sum_14(self):
        q = list(Product.select().sort_by(lambda p: p.sum_14))
        result = [p.id for p in q]
        self.assertEqual(result, [1, 3, 2, 4])


if __name__ == "__main__":
    unittest.main()
