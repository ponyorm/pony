
from __future__ import absolute_import, print_function, division
from pony.py23compat import PYPY2, pickle

import unittest
from datetime import date
from decimal import Decimal

from pony.orm.core import *
from pony.orm.tests.testutils import *
from pony.orm.tests import teardown_database, setup_database

db = Database()


class Group(db.Entity):
    id = PrimaryKey(int)
    number = Required(str, unique=True)
    students = Set("Student")


class Student(db.Entity):
    id = PrimaryKey(int)
    name = Required(str)
    group = Required("Group")


class TestCRUD(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_database(db)
        with db_session:
            g1 = Group(id=1, number='g111')
            g2 = Group(id=2, number='g222')
            s1 = Student(id=1, name='S1', group=g1)
            s2 = Student(id=2, name='S2', group=g1)
            s3 = Student(id=3, name='S3', group=g2)

    @classmethod
    def tearDownClass(cls):
        teardown_database(db)

    def setUp(self):
        rollback()
        db_session.__enter__()

    def tearDown(self):
        rollback()
        db_session.__exit__()

    def test_unique_load(self):
        s1 = Student[1]
        g1 = s1.group
        g1.number = 'g123'
        self.assertEqual(g1.number, 'g123')
