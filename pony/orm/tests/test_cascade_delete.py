import unittest

from pony.orm import *
from pony.orm.tests import setup_database, teardown_database, only_for

db = Database()

class X(db.Entity):
    id = PrimaryKey(int)
    parent = Optional('X', reverse='children')
    children = Set('X', reverse='parent', cascade_delete=True)


class Y(db.Entity):
    parent = Optional('Y', reverse='children')
    children = Set('Y', reverse='parent', cascade_delete=True, lazy=True)


class TestCascade(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_database(db)
        with db_session:
            x1 = X(id=1)
            x2 = X(id=2, parent=x1)
            x3 = X(id=3, parent=x1)
            x4 = X(id=4, parent=x3)
            x5 = X(id=5, parent=x3)
            x6 = X(id=6, parent=x5)
            x7 = X(id=7, parent=x3)
            x8 = X(id=8, parent=x7)
            x9 = X(id=9, parent=x7)
            x10 = X(id=10)
            x11 = X(id=11, parent=x10)
            x12 = X(id=12, parent=x10)

            y1 = Y(id=1)
            y2 = Y(id=2, parent=y1)
            y3 = Y(id=3, parent=y1)
            y4 = Y(id=4, parent=y3)
            y5 = Y(id=5, parent=y3)
            y6 = Y(id=6, parent=y5)
            y7 = Y(id=7, parent=y3)
            y8 = Y(id=8, parent=y7)
            y9 = Y(id=9, parent=y7)
            y10 = Y(id=10)
            y11 = Y(id=11, parent=y10)
            y12 = Y(id=12, parent=y10)

    @classmethod
    def tearDownClass(cls):
        teardown_database(db)

    def setUp(self):
        rollback()
        db_session.__enter__()

    def tearDown(self):
        rollback()
        db_session.__exit__()

    def test_1(self):
        db.merge_local_stats()
        X[1].delete()
        stats = db.local_stats[None]
        self.assertEqual(5, stats.db_count)

    def test_2(self):
        db.merge_local_stats()
        Y[1].delete()
        stats = db.local_stats[None]
        self.assertEqual(10, stats.db_count)
