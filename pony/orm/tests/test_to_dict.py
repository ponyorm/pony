import unittest
from decimal import Decimal
from datetime import date

from pony.orm import *
from pony.orm.serialization import to_dict
from pony.orm.tests.testutils import *
from pony.orm.tests import setup_database, teardown_database


class TestObjectToDict(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        db = cls.db = Database()

        class Student(db.Entity):
            name = Required(unicode)
            scholarship = Optional(int)
            gpa = Optional(Decimal, 3, 1)
            dob = Optional(date)
            group = Optional('Group')
            courses = Set('Course')
            biography = Optional(LongUnicode)

        class Group(db.Entity):
            number = PrimaryKey(int)
            students = Set(Student)

        class Course(db.Entity):
            name = Required(unicode, unique=True)
            students = Set(Student)

        setup_database(db)

        with db_session:
            g1 = Group(number=1)
            g2 = Group(number=2)
            c1 = Course(id=1, name='Math')
            c2 = Course(id=2, name='Physics')
            c3 = Course(id=3, name='Computer Science')
            Student(id=1, name='S1', group=g1, gpa=3.1, courses=[c1, c2], biography='some text')
            Student(id=2, name='S2', group=g1, gpa=3.2, scholarship=100, dob=date(2000, 1, 1))
            Student(id=3, name='S3', group=g1, gpa=3.3, scholarship=200, dob=date(2001, 1, 2), courses=[c2, c3])
            Student(id=4, name='S4')

    @classmethod
    def tearDownClass(cls):
        teardown_database(cls.db)

    def setUp(self):
        rollback()
        db_session.__enter__()

    def tearDown(self):
        rollback()
        db_session.__exit__()

    def test1(self):
        s1 = self.db.Student[1]
        d = s1.to_dict()
        self.assertEqual(d, dict(id=1, name='S1', scholarship=None, gpa=Decimal('3.1'), dob=None,
                                 group=1))

    def test2(self):
        s1 = self.db.Student[1]
        d = s1.to_dict(related_objects=True)
        self.assertEqual(d, dict(id=1, name='S1', scholarship=None, gpa=Decimal('3.1'), dob=None,
                                 group=self.db.Group[1]))

    def test3(self):
        s1 = self.db.Student[1]
        d = s1.to_dict(with_collections=True)
        self.assertEqual(d, dict(id=1, name='S1', scholarship=None, gpa=Decimal('3.1'), dob=None,
                                 group=1, courses=[1, 2]))

    def test4(self):
        s1 = self.db.Student[1]
        d = s1.to_dict(with_collections=True, related_objects=True)
        self.assertEqual(d, dict(id=1, name='S1', scholarship=None, gpa=Decimal('3.1'), dob=None,
                                 group=self.db.Group[1], courses=[self.db.Course[1], self.db.Course[2]]))

    def test5(self):
        s1 = self.db.Student[1]
        d = s1.to_dict(with_lazy=True)
        self.assertEqual(d, dict(id=1, name='S1', scholarship=None, gpa=Decimal('3.1'), dob=None,
                                 group=1, biography='some text'))

    def test6(self):
        s1 = self.db.Student[1]
        d = s1.to_dict(only=['id', 'name', 'group'])
        self.assertEqual(d, dict(id=1, name='S1', group=1))

    def test7(self):
        s1 = self.db.Student[1]
        d = s1.to_dict(['id', 'name', 'group'])
        self.assertEqual(d, dict(id=1, name='S1', group=1))

    def test8(self):
        s1 = self.db.Student[1]
        d = s1.to_dict(only='id, name, group')
        self.assertEqual(d, dict(id=1, name='S1', group=1))

    def test9(self):
        s1 = self.db.Student[1]
        d = s1.to_dict(only='id name group')
        self.assertEqual(d, dict(id=1, name='S1', group=1))

    def test10(self):
        s1 = self.db.Student[1]
        d = s1.to_dict('id name group')
        self.assertEqual(d, dict(id=1, name='S1', group=1))

    @raises_exception(AttributeError, 'Entity Student does not have attriute x')
    def test11(self):
        s1 = self.db.Student[1]
        d = s1.to_dict('id name x group')
        self.assertEqual(d, dict(id=1, name='S1', group=1))

    def test12(self):
        s1 = self.db.Student[1]
        d = s1.to_dict('id name group', related_objects=True)
        self.assertEqual(d, dict(id=1, name='S1', group=self.db.Group[1]))

    def test13(self):
        s1 = self.db.Student[1]
        d = s1.to_dict(exclude=['dob', 'gpa', 'scholarship'])
        self.assertEqual(d, dict(id=1, name='S1', group=1))

    def test14(self):
        s1 = self.db.Student[1]
        d = s1.to_dict(exclude='dob, gpa, scholarship')
        self.assertEqual(d, dict(id=1, name='S1', group=1))

    def test15(self):
        s1 = self.db.Student[1]
        d = s1.to_dict(exclude='dob gpa scholarship')
        self.assertEqual(d, dict(id=1, name='S1', group=1))

    @raises_exception(AttributeError, 'Entity Student does not have attriute x')
    def test16(self):
        s1 = self.db.Student[1]
        d = s1.to_dict(exclude='dob gpa x scholarship')
        self.assertEqual(d, dict(id=1, name='S1', group=1))

    def test17(self):
        s1 = self.db.Student[1]
        d = s1.to_dict(exclude='dob gpa scholarship', related_objects=True)
        self.assertEqual(d, dict(id=1, name='S1', group=self.db.Group[1]))

    def test18(self):
        s1 = self.db.Student[1]
        d = s1.to_dict(exclude='dob gpa scholarship', with_lazy=True)
        self.assertEqual(d, dict(id=1, name='S1', group=1, biography='some text'))

    def test19(self):
        s1 = self.db.Student[1]
        d = s1.to_dict(exclude='dob gpa scholarship biography', with_lazy=True)
        self.assertEqual(d, dict(id=1, name='S1', group=1))

    def test20(self):
        s1 = self.db.Student[1]
        d = s1.to_dict(exclude='dob gpa scholarship', with_collections=True)
        self.assertEqual(d, dict(id=1, name='S1', group=1, courses=[1, 2]))

    def test21(self):
        s1 = self.db.Student[1]
        d = s1.to_dict(exclude='dob gpa scholarship courses', with_collections=True)
        self.assertEqual(d, dict(id=1, name='S1', group=1))

    def test22(self):
        s1 = self.db.Student[1]
        d = s1.to_dict(only='id name group', exclude='dob group')
        self.assertEqual(d, dict(id=1, name='S1'))

    def test23(self):
        s1 = self.db.Student[1]
        d = s1.to_dict(only='id name group', exclude='dob group', with_collections=True, with_lazy=True)
        self.assertEqual(d, dict(id=1, name='S1'))

    def test24(self):
        c = self.db.Course(id=4, name='New Course')
        d = c.to_dict()  # should do flush and get c.id from the database
        self.assertEqual(d, dict(id=4, name='New Course'))


class TestSerializationToDict(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        db = cls.db = Database()

        class Student(db.Entity):
            name = Required(unicode)
            scholarship = Optional(int)
            gpa = Optional(Decimal, 3, 1)
            dob = Optional(date)
            group = Optional('Group')
            courses = Set('Course')
            biography = Optional(LongUnicode)

        class Group(db.Entity):
            number = PrimaryKey(int)
            students = Set(Student)

        class Course(db.Entity):
            name = Required(unicode, unique=True)
            students = Set(Student)

        setup_database(db)

        with db_session:
            g1 = Group(number=1)
            g2 = Group(number=2)
            c1 = Course(name='Math')
            c2 = Course(name='Physics')
            c3 = Course(name='Computer Science')
            Student(id=1, name='S1', group=g1, gpa=3.1, courses=[c1, c2], biography='some text')
            Student(id=2, name='S2', group=g1, gpa=3.2, scholarship=100, dob=date(2000, 1, 1))
            Student(id=3, name='S3', group=g1, gpa=3.3, scholarship=200, dob=date(2001, 1, 2), courses=[c2, c3])
            Student(id=4, name='S4')

    @classmethod
    def tearDownClass(cls):
        teardown_database(cls.db)

    def setUp(self):
        rollback()
        db_session.__enter__()

    def tearDown(self):
        rollback()
        db_session.__exit__()

    def test1(self):
        s4 = self.db.Student[4]
        self.assertEqual(s4.group, None)
        d = to_dict(s4)
        self.assertEqual(d, dict(Student={
            4 : dict(id=4, name='S4', group=None, dob=None, gpa=None, scholarship=None, courses=[])
            }))

if __name__ == '__main__':
    unittest.main()
