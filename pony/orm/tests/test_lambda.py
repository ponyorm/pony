import unittest
from datetime import datetime, timedelta

from pony.orm import *
from pony.orm.tests import setup_database, teardown_database

db = Database()


class User(db.Entity):
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
            User(login='test', password='123456', created_at=datetime(2012, 12, 13, 5, 25, 30))
            User(login='test2', password='123456', created_at=datetime(2015, 12, 13, 5, 25, 30))
            User(login='test3', password='123456')

    @classmethod
    def tearDownClass(cls) -> None:
        teardown_database(db)

    @classmethod
    def tearDown(self) -> None:
        with db_session:
            User.select().delete()

    @db_session
    def test1(self):
        result = User.select(lambda u: u.created_at < datetime.now() - timedelta(days=365))[:]
        self.assertEqual([User[1], User[2]], result)
