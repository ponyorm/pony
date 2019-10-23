from pony.py23compat import basestring

import unittest

from pony.orm import *
from pony import orm
from pony.utils import cached_property
from datetime import date


class Test(unittest.TestCase):

    @cached_property
    def db(self):
        return orm.Database('sqlite', ':memory:')

    def setUp(self):
        db = self.db
        self.day = date.today()

        class A(db.Entity):
            b = Required("B")
            c = Required("C")
            PrimaryKey(b, c)

        class B(db.Entity):
            id = PrimaryKey(date)
            a_set = Set(A)

        class C(db.Entity):
            x = Required("X")
            y = Required("Y")
            a_set = Set(A)
            PrimaryKey(x, y)

        class X(db.Entity):
            id = PrimaryKey(int)
            c_set = Set(C)

        class Y(db.Entity):
            id = PrimaryKey(int)
            c_set = Set(C)

        db.generate_mapping(check_tables=True, create_tables=True)

        with orm.db_session:
            x1 = X(id=123)
            y1 = Y(id=456)
            b1 = B(id=self.day)
            c1 = C(x=x1, y=y1)
            A(b=b1, c=c1)


    @db_session
    def test_1(self):
        a1 = self.db.A.select().first()
        a2 = self.db.A[a1.get_pk()]
        self.assertEqual(a1, a2)

    @db_session
    def test2(self):
        a = self.db.A.select().first()
        b = self.db.B.select().first()
        c = self.db.C.select().first()
        pk = (b.get_pk(), c._get_raw_pkval_())
        self.assertTrue(a is self.db.A[pk])

    @db_session
    def test3(self):
        a = self.db.A.select().first()
        c = self.db.C.select().first()
        pk = (self.day, c.get_pk())
        self.assertTrue(a is self.db.A[pk])