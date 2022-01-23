import unittest
from datetime import datetime, timedelta

from pony.orm import *
from pony.orm.tests import setup_database, teardown_database

db = Database()


class User(db.Entity):
    id = PrimaryKey(int)
    login = Required(str)
    password = Required(str)
    created_at = Required(datetime, default=datetime.now())


class TestLambda(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        setup_database(db)

    @classmethod
    def setUp(self) -> None:
        with db_session:
            User(id=1, login='test', password='123456', created_at=datetime(2012, 12, 13, 5, 25, 30))
            User(id=2, login='test2', password='123456', created_at=datetime(2015, 12, 13, 5, 25, 30))
            User(id=3, login='test3', password='123456')

    @classmethod
    def tearDownClass(cls) -> None:
        teardown_database(db)

    @classmethod
    def tearDown(self) -> None:
        with db_session:
            User.select().delete()

    @db_session
    def test_select(self):
        result = User.select(lambda u: u.created_at < datetime.now() - timedelta(days=365))[:]
        self.assertEqual([u.id for u in result], [1, 2])

    @db_session
    def test_order_by_1(self):
        result = User.select().order_by(lambda u: u.id)
        self.assertEqual([u.id for u in result], [1, 2, 3])

    @db_session
    def test_order_by_2(self):
        result = User.select().order_by(lambda u: desc(u.id))
        self.assertEqual([u.id for u in result], [3, 2, 1])

    @db_session
    def test_order_by_3(self):
        result = User.select().order_by(lambda u: (u.login, u.id))
        self.assertEqual([u.id for u in result], [1, 2, 3])

    @db_session
    def test_order_by_4(self):
        result = User.select().order_by(lambda u: (desc(u.login), desc(u.id)))
        self.assertEqual([u.id for u in result], [3, 2, 1])

