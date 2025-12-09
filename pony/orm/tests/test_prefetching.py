import sys, unittest
from decimal import Decimal
from datetime import date

from pony.orm import *
from pony.orm.tests.testutils import *
from pony.orm.tests import setup_database, teardown_database

db = Database()


class Student(db.Entity):
    name = Required(str)
    scholarship = Optional(int)
    gpa = Optional(Decimal, 3, 1)
    dob = Optional(date)
    group = Required('Group')
    courses = Set('Course')
    mentor = Optional('Teacher')
    biography = Optional(LongStr)


class Group(db.Entity):
    number = PrimaryKey(int)
    major = Required(str, lazy=True)
    students = Set(Student)


class Course(db.Entity):
    name = Required(str, unique=True)
    students = Set(Student)


class Teacher(db.Entity):
    name = Required(str)
    students = Set(Student)


class TestPrefetching(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_database(db)
        with db_session:
            g1 = Group(number=1, major='Math')
            g2 = Group(number=2, major='Computer Sciense')
            c1 = Course(name='Math')
            c2 = Course(name='Physics')
            c3 = Course(name='Computer Science')
            t1 = Teacher(name='T1')
            t2 = Teacher(name='T2')
            Student(id=1, name='S1', group=g1, gpa=3.1, courses=[c1, c2], biography='S1 bio', mentor=t1)
            Student(id=2, name='S2', group=g1, gpa=4.2, scholarship=100, dob=date(2000, 1, 1), biography='S2 bio')
            Student(id=3, name='S3', group=g1, gpa=4.7, scholarship=200, dob=date(2001, 1, 2), courses=[c2, c3])
            Student(id=4, name='S4', group=g2, gpa=3.2, biography='S4 bio', courses=[c1, c3], mentor=t2)
            Student(id=5, name='S5', group=g2, gpa=4.5, biography='S5 bio', courses=[c1, c3])

    @classmethod
    def tearDownClass(cls):
        teardown_database(db)

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
            s1 = Student.select().prefetch(Group, Group.major).first()
            g = s1.group
        self.assertEqual(g.major, 'Math')

    def test_4(self):
        with db_session:
            s1 = Student.select().prefetch(Student.group, Group.major).first()
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
            name, group = select((s.name, s.group) for s in Student).prefetch(Group, Group.major).first()
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
        self.assertEqual(s1.biography, 'S1 bio')
        table_name = 'Student' if db.provider.dialect == 'SQLite' and pony.__version__ < '0.9' else 'student'
        expected_sql = '''SELECT "s"."id", "s"."name", "s"."scholarship", "s"."gpa", "s"."dob", "s"."group", "s"."mentor", "s"."biography"
FROM "%s" "s"
ORDER BY 1
LIMIT 1''' % table_name
        if db.provider.dialect == 'SQLite' and pony.__version__ >= '0.9':
            expected_sql = expected_sql.replace('"', '`')
        self.assertEqual(db.last_sql, expected_sql)

    def test_13(self):
        db.merge_local_stats()
        with db_session:
            q = select(g for g in Group)
            for g in q: # 1 query
                for s in g.students:  # 2 query
                    b = s.biography  # 5 queries
            query_count = db.local_stats[None].db_count
            self.assertEqual(query_count, 8)

    def test_14(self):
        db.merge_local_stats()
        with db_session:
            q = select(g for g in Group).prefetch(Group.students)
            for g in q:   # 1 query
                for s in g.students:  # 1 query
                    b = s.biography  # 5 queries
            query_count = db.local_stats[None].db_count
            self.assertEqual(query_count, 7)

    def test_15(self):
        with db_session:
            q = select(g for g in Group).prefetch(Group.students)
            q[:]
        db.merge_local_stats()
        with db_session:
            q = select(g for g in Group).prefetch(Group.students, Student.biography)
            for g in q:  # 1 query
                for s in g.students:  # 1 query
                    b = s.biography  # 0 queries
            query_count = db.local_stats[None].db_count
            self.assertEqual(query_count, 2)

    def test_16(self):
        db.merge_local_stats()
        with db_session:
            q = select(c for c in Course).prefetch(Course.students, Student.biography)
            for c in q:  # 1 query
                for s in c.students:  # 2 queries (as it is many-to-many relationship)
                    b = s.biography  # 0 queries
            query_count = db.local_stats[None].db_count
            self.assertEqual(query_count, 3)

    def test_17(self):
        db.merge_local_stats()
        with db_session:
            q = select(c for c in Course).prefetch(Course.students, Student.biography, Group, Group.major)
            for c in q:  # 1 query
                for s in c.students:  # 2 queries (as it is many-to-many relationship)
                    m = s.group.major  # 1 query
                    b = s.biography  # 0 queries
            query_count = db.local_stats[None].db_count
            self.assertEqual(query_count, 4)

    def test_18(self):
        db.merge_local_stats()
        with db_session:
            q = Group.select().prefetch(Group.students, Student.biography)
            for g in q:  # 2 queries
                for s in g.students:
                    m = s.mentor  # 0 queries
                    b = s.biography  # 0 queries
            query_count = db.local_stats[None].db_count
            self.assertEqual(query_count, 2)

    def test_19(self):
        db.merge_local_stats()
        with db_session:
            q = Group.select().prefetch(Group.students, Student.biography, Student.mentor)
            mentors = set()
            for g in q:  # 3 queries
                for s in g.students:
                    m = s.mentor  # 0 queries
                    if m is not None:
                        mentors.add(m)
                    b = s.biography  # 0 queries
            query_count = db.local_stats[None].db_count
            self.assertEqual(query_count, 3)

            for m in mentors:
                n = m.name  # 0 queries
            query_count = db.local_stats[None].db_count
            self.assertEqual(query_count, 3)


if __name__ == '__main__':
    unittest.main()
