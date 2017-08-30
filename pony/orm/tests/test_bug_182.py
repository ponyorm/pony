
import unittest

from pony.orm import *
from pony import orm


class Test(unittest.TestCase):

    def setUp(self):
        db = self.db = Database('sqlite', ':memory:')

        class User(db.Entity):
            name = Required(str)
            servers = Set("Server")

        class Worker(db.User):
            pass

        class Admin(db.Worker):
            pass

        # And M:1 relationship with another entity
        class Server(db.Entity):
            name = Required(str)
            user = Optional(User)

        db.generate_mapping(check_tables=True, create_tables=True)

        with orm.db_session:
            Server(name='s1.example.com', user=User(name="Alex"))
            Server(name='s2.example.com', user=Worker(name="John"))
            Server(name='free.example.com', user=None)

    @db_session
    def test(self):
        qu = left_join((s.name, s.user.name) for s in self.db.Server)[:]
        for server, user in qu:
            if user is None:
                break
        else:
            self.fail()

