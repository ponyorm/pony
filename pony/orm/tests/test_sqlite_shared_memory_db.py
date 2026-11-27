import threading
import unittest
from typing import TYPE_CHECKING

from pony.orm.core import *

db = Database("sqlite", ":sharedmemory:")

if TYPE_CHECKING:
    from pony.orm.core import Entity
else:
    Entity = db.Entity


class Person(Entity):
    name = Required(str)


db.generate_mapping(create_tables=True)

with db_session:
    Person(name="John")
    Person(name="Mike")


class TestThread(threading.Thread):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, *kwargs)
        self.result = []

    def run(self):
        with db_session:
            persons = Person.select().fetch()
            self.result.extend(p.name for p in persons)


class TestFlush(unittest.TestCase):
    def test1(self):
        thread1 = TestThread()
        thread1.start()
        thread1.join()
        self.assertEqual(set(thread1.result), {"John", "Mike"})
