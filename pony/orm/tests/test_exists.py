import unittest

from pony.orm.core import *
from pony.orm.tests.testutils import *
from pony.orm.tests import setup_database, teardown_database

db = Database()

class Group(db.Entity):
    students = Set('Student')

class Student(db.Entity):
    first_name = Required(str)
    last_name = Required(str)
    login = Optional(str, nullable=True)
    graduated = Optional(bool, default=False)
    group = Required(Group)
    passport = Optional('Passport', column='passport')

class Passport(db.Entity):
    student = Optional(Student)


class TestExists(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_database(db)
        with db_session:
            g1 = Group(id=1)
            g2 = Group(id=2)

            p = Passport(id=1)

            Student(id=1, first_name='Mashu', last_name='Kyrielight', login='Shielder', group=g1)
            Student(id=2, first_name='Okita', last_name='Souji', login='Sakura', group=g1)
            Student(id=3, first_name='Francis', last_name='Drake', group=g2, graduated=True)
            Student(id=4, first_name='Oda', last_name='Nobunaga', group=g2, graduated=True)
            Student(id=5, first_name='William', last_name='Shakespeare', group=g2, graduated=True, passport=p)

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
        q = select(g for g in Group if exists(s.login for s in g.students))[:]
        self.assertEqual(q[0], Group[1])

    def test_2(self):
        q = select(g for g in Group if exists(s.graduated for s in g.students))[:]
        self.assertEqual(q[0], Group[2])

    def test_3(self):
        q = select(s for s in Student if
                   exists(len(s2.first_name) == len(s.first_name) and s != s2 for s2 in Student))[:]
        self.assertEqual(set(q), {Student[1], Student[2], Student[3], Student[5]})

    def test_4(self):
        q = select(g for g in Group if not exists(not s.graduated for s in g.students))[:]
        self.assertEqual(q[0], Group[2])

    def test_5(self):
        q = select(g for g in Group if exists(s for s in g.students))[:]
        self.assertEqual(set(q), {Group[1], Group[2]})

    def test_6(self):
        q = select(g for g in Group if exists(s.login for s in g.students if s.first_name != 'Okita') and g.id != 10)[:]
        self.assertEqual(q[0], Group[1])

    def test_7(self):
        q = select(g for g in Group if exists(s.passport for s in g.students))[:]
        self.assertEqual(q[0], Group[2])