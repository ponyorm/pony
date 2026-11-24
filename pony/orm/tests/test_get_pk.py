import unittest
from datetime import date
from typing import TYPE_CHECKING

from pony.orm import *
from pony.orm.tests import setup_database, teardown_database

day = date.today()

db = Database()

if TYPE_CHECKING:
    from pony.orm.core import Entity
else:
    Entity = db.Entity


class A(Entity):
    b = Required("B")
    c = Required("C")
    PrimaryKey(b, c)


class B(Entity):
    id = PrimaryKey(date)
    a_set = Set(A)


class C(Entity):
    x = Required("X")
    y = Required("Y")
    a_set = Set(A)
    PrimaryKey(x, y)


class X(Entity):
    id = PrimaryKey(int)
    c_set = Set(C)


class Y(Entity):
    id = PrimaryKey(int)
    c_set = Set(C)


class Test(unittest.TestCase):
    def setUp(self):
        setup_database(db)
        with db_session:
            x1 = X(id=123)
            y1 = Y(id=456)
            b1 = B(id=day)
            c1 = C(x=x1, y=y1)
            A(b=b1, c=c1)

    def tearDown(self):
        teardown_database(db)

    @db_session
    def test_1(self):
        a1 = A.select().first()
        a2 = A[a1.get_pk()]
        self.assertEqual(a1, a2)

    @db_session
    def test2(self):
        a = A.select().first()
        b = B.select().first()
        c = C.select().first()
        pk = (b.get_pk(), c._get_raw_pkval_())
        self.assertTrue(a is A[pk])

    @db_session
    def test3(self):
        a = A.select().first()
        c = C.select().first()
        pk = (day, c.get_pk())
        self.assertTrue(a is A[pk])
