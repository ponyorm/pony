from __future__ import absolute_import, print_function, division

import unittest

from pony.orm.core import *
from pony.orm.tests.testutils import *
from pony.orm.tests import setup_database, teardown_database


class TestOneToManyRequired(unittest.TestCase):
    def setUp(self):
        db = Database()

        class Student(db.Entity):
            id = PrimaryKey(int)
            name = Required(unicode)
            group = Required('Group')

        class Group(db.Entity):
            number = PrimaryKey(int)
            students = Set(Student)

        self.db = db
        self.Group = Group
        self.Student = Student

        setup_database(db)

        with db_session:
            g101 = Group(number=101)
            g102 = Group(number=102)
            g103 = Group(number=103)
            s1 = Student(id=1, name='Student1', group=g101)
            s2 = Student(id=2, name='Student2', group=g101)
            s3 = Student(id=3, name='Student3', group=g102)
            s4 = Student(id=4, name='Student3', group=g102)

        db_session.__enter__()

    def tearDown(self):
        rollback()
        db_session.__exit__()
        teardown_database(self.db)

    @raises_exception(ValueError, 'Attribute Student[1].group is required')
    def test_1(self):
        self.Student[1].group = None

    def test_2(self):
        Student, Group = self.Student, self.Group
        s1 = Student[1]
        g = Group[101]
        g.students.remove(s1)
        self.assertEqual(s1._status_, 'marked_to_delete')

    def test_3(self):
        Student, Group = self.Student, self.Group
        s1, s2 = Student[1], Student[2]
        g = Group[101]
        g.students.clear()
        self.assertEqual(s1._status_, 'marked_to_delete')
        self.assertEqual(s2._status_, 'marked_to_delete')
        self.assertEqual(set(g.students), set())

    def test_4(self):
        Student, Group = self.Student, self.Group
        s1, s2, s3, s4 = Student.select().order_by(Student.id)
        g1, g2 = Group[101], Group[102]
        g1.students = g2.students
        self.assertEqual(set(g1.students), {s3, s4})
        self.assertEqual(s1._status_, 'marked_to_delete')
        self.assertEqual(s2._status_, 'marked_to_delete')

    @raises_exception(ValueError, 'A single Student instance or Student iterable is expected. Got: None')
    def test_5(self):
        Group, Student = self.Group, self.Student
        g = Group[101]
        g.students.add(None)

    @raises_exception(ValueError, 'A single Student instance or Student iterable is expected. Got: None')
    def test_6(self):
        Group, Student = self.Group, self.Student
        g = Group[101]
        g.students.remove(None)

    @raises_exception(ValueError, 'A single Student instance or Student iterable is expected. Got: None')
    def test_7(self):
        Group = self.Group
        g104 = Group(number=104, students=None)

    def test_8(self):
        db, Group, Student = self.db, self.Group, self.Student

        g = Group[101]
        s3 = Student[3]  # s3 is loaded now
        db._dblocal.last_sql = None
        g.students.add(s3)
        # Group.students.load should not attempt to load s3 from db
        self.assertEqual(db.last_sql, None)

    def test_9(self):
        db, Group, Student = self.db, self.Group, self.Student

        g = Group[101]
        e = g.students.is_empty()
        self.assertEqual(e, False)

        db._dblocal.last_sql = None
        e = g.students.is_empty()  # should take result from the cache
        self.assertEqual(e, False)
        self.assertEqual(db.last_sql, None)

        g = Group[103]
        e = g.students.is_empty()  # should take SQL from the SQL cache
        self.assertEqual(e, True)

        db._dblocal.last_sql = None
        e = g.students.is_empty()  # should take result from the cache
        self.assertEqual(e, True)
        self.assertEqual(db.last_sql, None)

    def test_10(self):
        db, Group = self.db, self.Group

        g = Group[101]
        c = len(g.students)
        self.assertEqual(c, 2)
        db._dblocal.last_sql = None
        e = g.students.is_empty()  # should take result from the cache
        self.assertEqual(e, False)
        self.assertEqual(db.last_sql, None)

        g = Group[102]
        c = g.students.count()
        self.assertEqual(c, 2)
        db._dblocal.last_sql = None
        e = g.students.is_empty()  # should take result from the cache
        self.assertEqual(e, False)
        self.assertEqual(db.last_sql, None)

        g = Group[103]
        c = len(g.students)
        self.assertEqual(c, 0)
        db._dblocal.last_sql = None
        e = g.students.is_empty()  # should take result from the cache
        self.assertEqual(e, True)
        self.assertEqual(db.last_sql, None)

    def test_11(self):
        db, Group, Student = self.db, self.Group, self.Student

        g = Group[101]
        s3 = Student[3]
        c = g.students.count()
        self.assertEqual(c, 2)

        db._dblocal.last_sql = None
        c = g.students.count()  # should take count from the cache
        self.assertEqual(c, 2)
        self.assertEqual(db.last_sql, None)

        g.students.add(s3)
        c = g.students.count()  # should take modified count from the cache
        self.assertEqual(c, 3)
        self.assertEqual(db.last_sql, None)

        g2 = Group[102]
        c = g2.students.count()  # should send query to the database
        self.assertEqual(c, 1)
        self.assertTrue(db.last_sql is not None)

    def test_12(self):
        Group, Student = self.Group, self.Student
        g = Group[101]

        s1 = Student[1]
        self.assertEqual(s1._rbits_, 0)
        self.assertTrue(s1 in g.students)
        self.assertEqual(s1._rbits_, Student._bits_[Student.group])

        s3 = Student[3]
        self.assertEqual(s3._rbits_, 0)
        self.assertTrue(s3 not in g.students)
        self.assertEqual(s3._rbits_, Student._bits_[Student.group])

        s5 = Student(id=5, name='Student5', group=g)
        self.assertEqual(s5._rbits_, None)
        self.assertTrue(s5 in g.students)
        self.assertEqual(s5._rbits_, None)

class TestOneToManyOptional(unittest.TestCase):

    def setUp(self):
        db = Database('sqlite', ':memory:', create_db=True)

        class Student(db.Entity):
            id = PrimaryKey(int)
            name = Required(unicode)
            group = Optional('Group')

        class Group(db.Entity):
            number = PrimaryKey(int)
            students = Set(Student)

        self.db = db
        self.Group = Group
        self.Student = Student

        db.generate_mapping(create_tables=True)

        with db_session:
            g101 = Group(number=101)
            g102 = Group(number=102)
            g103 = Group(number=103)
            s1 = Student(id=1, name='Student1', group=g101)
            s2 = Student(id=2, name='Student2', group=g101)
            s3 = Student(id=3, name='Student3', group=g102)
            s4 = Student(id=4, name='Student3', group=g102)

        db_session.__enter__()

    def tearDown(self):
        rollback()
        db_session.__exit__()

    def test_1(self):
        self.Student[1].group = None
        self.assertEqual(set(self.Group[101].students), {self.Student[2]})

    def test_2(self):
        Student, Group = self.Student, self.Group
        s1 = Student[1]
        g = Group[101]
        g.students.remove(s1)
        self.assertEqual(s1.group, None)

    def test_3(self):
        Student, Group = self.Student, self.Group
        s1, s2 = Student[1], Student[2]
        g = Group[101]
        g.students.clear()
        self.assertEqual(s1.group, None)
        self.assertEqual(s2.group, None)
        self.assertEqual(set(g.students), set())

    def test_4(self):
        Student, Group = self.Student, self.Group
        s1, s2, s3, s4 = Student.select().order_by(Student.id)
        g1, g2 = Group[101], Group[102]
        g1.students = g2.students
        self.assertEqual(set(g1.students), {s3, s4})
        self.assertEqual(s1.group, None)
        self.assertEqual(s2.group, None)

    @raises_exception(ValueError, 'A single Student instance or Student iterable is expected. Got: None')
    def test_5(self):
        Group, Student = self.Group, self.Student
        g = Group[101]
        g.students.add(None)

    @raises_exception(ValueError, 'A single Student instance or Student iterable is expected. Got: None')
    def test_6(self):
        Group, Student = self.Group, self.Student
        g = Group[101]
        g.students.remove(None)

    @raises_exception(ValueError, 'A single Student instance or Student iterable is expected. Got: None')
    def test_7(self):
        Group = self.Group
        g104 = Group(number=104, students=None)


if __name__ == '__main__':
    unittest.main()
