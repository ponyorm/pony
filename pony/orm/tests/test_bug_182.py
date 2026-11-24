import unittest
from typing import TYPE_CHECKING

from pony import orm
from pony.orm import *
from pony.orm.tests import setup_database, teardown_database

db = Database()

if TYPE_CHECKING:
    from pony.orm.core import Entity
else:
    Entity = db.Entity


class User(Entity):
    name = Required(str)
    servers = Set("Server")


class Worker(User):
    pass


class Admin(Worker):
    pass


# And M:1 relationship with another entity
class Server(Entity):
    name = Required(str)
    user = Optional(User)


class Test(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_database(db)
        with orm.db_session:
            Server(name="s1.example.com", user=User(name="Alex"))
            Server(name="s2.example.com", user=Worker(name="John"))
            Server(name="free.example.com", user=None)

    @classmethod
    def tearDownClass(cls):
        teardown_database(db)

    @db_session
    def test(self):
        qu = left_join((s.name, s.user.name) for s in db.Server)[:]
        for _server, user in qu:
            if user is None:
                break
        else:
            self.fail()
