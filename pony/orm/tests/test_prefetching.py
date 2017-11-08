import sys, unittest
from decimal import Decimal
from datetime import date

from pony.orm import *
from pony.orm.tests.testutils import *

db = Database('sqlite', ':memory:')

class Student(db.Entity):
    name = Required(str)
    scholarship = Optional(int)
    gpa = Optional(Decimal, 3, 1)
    dob = Optional(date)
    group = Required('Group')
    courses = Set('Course')
    biography = Optional(LongStr)

class Group(db.Entity):
    number = PrimaryKey(int)
    major = Required(str)
    students = Set(Student)

class Course(db.Entity):
    name = Required(str, unique=True)
    students = Set(Student)

db.generate_mapping(create_tables=True)

with db_session:
    g1 = Group(number=1, major='Math')
    g2 = Group(number=2, major='Computer Sciense')
    c1 = Course(name='Math')
    c2 = Course(name='Physics')
    c3 = Course(name='Computer Science')
    Student(id=1, name='S1', group=g1, gpa=3.1, courses=[c1, c2], biography='some text')
    Student(id=2, name='S2', group=g1, gpa=3.2, scholarship=100, dob=date(2000, 1, 1))
    Student(id=3, name='S3', group=g1, gpa=3.3, scholarship=200, dob=date(2001, 1, 2), courses=[c2, c3])

class TestPrefetching(unittest.TestCase):
    def test_1(self):
        with db_session:
            s1 = Student.select().first()
            g = s1.group
            self.assertEqual(g.major, 'Math')

    @raises_exception(DatabaseSessionIsOver, 'Cannot load attribute Group[1].major: the database session is over')
    def test_2(self):
        with db_session:
            s1 = Student.select().first()
            g = s1.group
        g.major

    def test_3(self):
        with db_session:
            s1 = Student.select().prefetch(Group).first()
            g = s1.group
        self.assertEqual(g.major, 'Math')

    def test_4(self):
        with db_session:
            s1 = Student.select().prefetch(Student.group).first()
            g = s1.group
        self.assertEqual(g.major, 'Math')

    @raises_exception(TypeError, 'Argument of prefetch() query method must be entity class or attribute. Got: 111')
    def test_5(self):
        with db_session:
            Student.select().prefetch(111)

    @raises_exception(DatabaseSessionIsOver, 'Cannot load attribute Group[1].major: the database session is over')
    def test_6(self):
        with db_session:
            name, group = select((s.name, s.group) for s in Student).first()
        group.major

    def test_7(self):
        with db_session:
            name, group = select((s.name, s.group) for s in Student).prefetch(Group).first()
        self.assertEqual(group.major, 'Math')

    @raises_exception(DatabaseSessionIsOver, 'Cannot load collection Student[1].courses: the database session is over')
    def test_8(self):
        with db_session:
            s1 = Student.select().first()
        set(s1.courses)

    @raises_exception(DatabaseSessionIsOver, 'Cannot load collection Student[1].courses: the database session is over')
    def test_9(self):
        with db_session:
            s1 = Student.select().prefetch(Course).first()
        set(s1.courses)

    def test_10(self):
        with db_session:
            s1 = Student.select().prefetch(Student.courses).first()
        self.assertEqual(set(s1.courses.name), {'Math', 'Physics'})

    @raises_exception(DatabaseSessionIsOver, 'Cannot load attribute Student[1].biography: the database session is over')
    def test_11(self):
        with db_session:
            s1 = Student.select().prefetch(Course).first()
        s1.biography

    def test_12(self):
        with db_session:
            s1 = Student.select().prefetch(Student.biography).first()
        self.assertEqual(s1.biography, 'some text')
        self.assertEqual(db.last_sql, '''SELECT "s"."id", "s"."name", "s"."scholarship", "s"."gpa", "s"."dob", "s"."group", "s"."biography"
FROM "student" "s"
ORDER BY 1
LIMIT 1''')

if __name__ == '__main__':
    unittest.main()
