from __future__ import with_statement

import unittest
from testutils import *
from pony.orm.core import *

class TestOneToMany(unittest.TestCase):

    def setUp(self):
        db = Database('sqlite', ':memory:', create_db=True)

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

    @raises_exception(ConstraintError, 'Attribute Student[1].group is required')
    def test_1(self):
        self.Student[1].group = None

    @raises_exception(ConstraintError, 'Attribute Student[1].group is required')
    def test_2(self):
        Student, Group = self.Student, self.Group
        Student[2].delete()  # in order to make exception text deterministic
        Group[101].students = Group[102].students

    def test_3(self):
        db, Group, Student = self.db, self.Group, self.Student

        g = Group[101]
        s3 = Student[3]  # s3 is loaded now
        db._dblocal.last_sql = None
        g.students.add(s3)
        # Group.students.load should not attempt to load s3 from db
        self.assertEquals(db.last_sql, None)

    def test_4(self):
        db, Group, Student = self.db, self.Group, self.Student

        g = Group[101]
        e = g.students.is_empty()
        self.assertEquals(e, False)

        db._dblocal.last_sql = None
        e = g.students.is_empty()  # should take result from the cache
        self.assertEquals(e, False)
        self.assertEquals(db.last_sql, None)

        g = Group[103]
        e = g.students.is_empty()  # should take SQL from the SQL cache
        self.assertEquals(e, True)

        db._dblocal.last_sql = None
        e = g.students.is_empty()  # should take result from the cache
        self.assertEquals(e, True)
        self.assertEquals(db.last_sql, None)

    def test_5(self):
        db, Group = self.db, self.Group

        g = Group[101]
        c = len(g.students)
        self.assertEquals(c, 2)
        db._dblocal.last_sql = None
        e = g.students.is_empty()  # should take result from the cache
        self.assertEquals(e, False)
        self.assertEquals(db.last_sql, None)
        
        g = Group[102]
        c = g.students.count()
        self.assertEquals(c, 2)
        db._dblocal.last_sql = None
        e = g.students.is_empty()  # should take result from the cache
        self.assertEquals(e, False)
        self.assertEquals(db.last_sql, None)

        g = Group[103]
        c = len(g.students)
        self.assertEquals(c, 0)
        db._dblocal.last_sql = None
        e = g.students.is_empty()  # should take result from the cache
        self.assertEquals(e, True)
        self.assertEquals(db.last_sql, None)

    def test_6(self):
        db, Group, Student = self.db, self.Group, self.Student

        g = Group[101]
        s3 = Student[3]
        c = g.students.count()
        self.assertEquals(c, 2)

        db._dblocal.last_sql = None
        c = g.students.count()  # should take count from the cache
        self.assertEquals(c, 2)
        self.assertEquals(db.last_sql, None)

        g.students.add(s3)
        c = g.students.count()  # should take modified count from the cache
        self.assertEquals(c, 3)
        self.assertEquals(db.last_sql, None)

        g2 = Group[102]
        c = g2.students.count()  # should send query to the database
        self.assertEquals(c, 1)
        self.assertTrue(db.last_sql is not None)

    def test_7_rbits(self):
        Group, Student = self.Group, self.Student
        g = Group[101]

        s1 = Student[1]
        self.assertEquals(s1._rbits_, 0)
        self.assertTrue(s1 in g.students)
        self.assertEquals(s1._rbits_, Student._bits_[Student.group])

        s3 = Student[3]
        self.assertEquals(s3._rbits_, 0)
        self.assertTrue(s3 not in g.students)
        self.assertEquals(s3._rbits_, Student._bits_[Student.group])

        s5 = Student(id=5, name='Student5', group=g)
        self.assertEquals(s5._rbits_, None)
        self.assertTrue(s5 in g.students)
        self.assertEquals(s5._rbits_, None)

if __name__ == '__main__':
    unittest.main()
