import unittest
from datetime import date, datetime
from collections import defaultdict
from pony.orm import *
from pony.orm.migrations.virtuals import VirtualDB, VirtualEntity, VirtualAttribute
from pony.orm.migrations import virtuals
from pony.orm.migrations.migrate import make_difference
try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO


def show_db(db, file=None, sort=False):
    for entity in db.entities.values():
        show(entity, file, sort)


class TestVirtuals(unittest.TestCase):
    def test_1(self):
        first = StringIO()
        second = StringIO()

        db = Database('sqlite', ':memory:')

        class Person(db.Entity):
            fullname = Required(str)

        db.generate_mapping(create_tables=True)

        vdb = VirtualDB.from_db(db)
        db2 = Database('sqlite', ':memory:')
        vdb.to_db(db2)
        show_db(db, first)
        show_db(db2, second)

        self.assertEqual(first.getvalue(), second.getvalue())

    def test_2(self):
        first = StringIO()
        second = StringIO()

        db = Database('sqlite', ':memory:')

        class Person(db.Entity):
            fullname = Required(str)

        class Student(Person):
            gpa = Required(float)

        db.generate_mapping(create_tables=True)

        vdb = VirtualDB.from_db(db)
        db2 = Database('sqlite', ':memory:')
        vdb.to_db(db2)
        show_db(db, first)
        show_db(db2, second)

        self.assertEqual(first.getvalue(), second.getvalue())

    def test_3(self):
        first = StringIO()
        second = StringIO()

        db = Database('sqlite', ':memory:')

        class Person(db.Entity):
            fullname = Required(str)

        class Student(Person):
            gpa = Required(float)

        class Teacher(Person):
            salary = Required(int)

        db.generate_mapping(create_tables=True)

        vdb = VirtualDB.from_db(db)
        db2 = Database('sqlite', ':memory:')
        vdb.to_db(db2)
        show_db(db, first)
        show_db(db2, second)

        self.assertEqual(first.getvalue(), second.getvalue())

    def test_4(self):
        first = StringIO()
        second = StringIO()

        db = Database('sqlite', ':memory:')

        class Person(db.Entity):
            fullname = Required(str)

        class Student(Person):
            gpa = Required(float)
            teacher = Required('Teacher')

        class Teacher(Person):
            salary = Required(int)
            position = Required(str, max_len=120)
            students = Set(Student)

        db.generate_mapping(create_tables=True)

        vdb = VirtualDB.from_db(db)
        db2 = Database('sqlite', ':memory:')
        vdb.to_db(db2)
        show_db(db, first)
        show_db(db2, second)

        self.assertEqual(first.getvalue(), second.getvalue())

    def test_5(self):
        first = StringIO()
        second = StringIO()

        db = Database('sqlite', ':memory:')

        class Department(db.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, unique=True)
            groups = Set("Group")
            courses = Set("Course")

        class Group(db.Entity):
            number = PrimaryKey(int)
            major = Required(str)
            dept = Required("Department")
            students = Set("Student")

        class Course(db.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int)
            credits = Required(int)
            dept = Required(Department)
            students = Set("Student")
            PrimaryKey(name, semester)

        class Student(db.Entity):
            # _table_ = "public", "Students"  # Schema support
            # id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            tel = Optional(str)
            picture = Optional(buffer, lazy=True)
            gpa = Required(float, default=0)
            group = Required(Group)
            courses = Set(Course)
            composite_key(name, dob)

        class Enrolle(Student):
            submission = Optional(date)
            composite_key(submission, Student.name)

        db.generate_mapping(create_tables=True)

        vdb = VirtualDB.from_db(db)
        db2 = Database('sqlite', ':memory:')
        vdb.to_db(db2)
        show_db(db, first)
        show_db(db2, second)

        self.assertEqual(first.getvalue(), second.getvalue())

    def test_6(self):
        first = StringIO()
        second = StringIO()

        db = Database('sqlite', ':memory:')

        class Station(db.Entity):
            title = Required(str)
            stops = Set('Stop')

        class Stop(db.Entity):
            station = Required(Station)
            departure = Required(datetime)
            arrival = Required(datetime)
            train = Required('Train')

        class Train(db.Entity):
            number = Required(int)
            stops = Set(Stop)
            cars = Set('Car')

        class Car(db.Entity):
            _table_ = 'vagon'
            type = Required(str)
            assembled = Required(date)
            trains = Set(Train)

        db.generate_mapping(create_tables=True)

        vdb = VirtualDB.from_db(db)
        db2 = Database('sqlite', ':memory:')
        vdb.to_db(db2)
        show_db(db, first)
        show_db(db2, second)

        self.assertEqual(first.getvalue(), second.getvalue())

    def test_7(self):
        first = StringIO()
        second = StringIO()

        db = Database('sqlite', ':memory:')

        class Person(db.Entity):
            name = Required(str)
            age = Required(int)

        class Student(Person):
            _discriminator_ = 'stud'
            gpa = Required(float)

        vdb = VirtualDB.from_db(db)
        db2 = Database()
        vdb.to_db(db2)

        show_db(db, first)
        show_db(db2, second)

        self.assertEqual(first.getvalue(), second.getvalue())

    def test_8(self):
        vdb = VirtualDB()
        vdb.entities = {
            'Student': VirtualEntity(
                # vdb,
                'Student',
                attrs=[
                    virtuals.Required('name', str),
                    virtuals.Required('age', int),
                    virtuals.Required('group', 'Group', reverse='students')
                ]
            ),
            'Group': VirtualEntity(
                # vdb,
                'Group',
                attrs=[
                    virtuals.Set('students', 'Student', reverse='group'),
                    virtuals.Required('number', int)
                ]
            )
        }

        db = Database('sqlite', ':memory:')
        vdb.to_db(db)
        db.generate_mapping(create_tables=True)

        db2 = Database('sqlite', ':memory:')

        class Student(db2.Entity):
            name = Required(str)
            age = Required(int)
            group = Required('Group')

        class Group(db2.Entity):
            students = Set('Student')
            number = Required(int)

        db2.generate_mapping(create_tables=True)

        first = StringIO()
        second = StringIO()

        show_db(db, first)
        show_db(db2, second)

        self.assertEqual(first.getvalue(), second.getvalue())

    def test_9(self):
        db1 = Database('sqlite', ':memory:')
        db2 = Database('sqlite', ':memory:')

        class Person(db1.Entity):
            name = Required(str)
            last_name = Required(str)
            tickets = Set('Ticket')

        class Ticket(db1.Entity):
            person = Required(Person)
            price = Required(int)
            is_gold = Required(bool)

        db1.generate_mapping(create_tables=True)

        class Person(db2.Entity):
            name = Required(str)
            last_name = Required(str)
            age = Optional(int)
            tickets = Set('Ticket')

        class Ticket(db2.Entity):
            person = Required(Person)
            price = Required(int)

        db2.generate_mapping(create_tables=True)

        vdb1 = VirtualDB.from_db(db1)
        vdb2 = VirtualDB.from_db(db2)
        ops = make_difference(vdb1, vdb2)

        db3 = Database('sqlite', ':memory:')
        vdb1.to_db(db3)
        db3.generate_mapping(create_tables=True)

        first = StringIO()
        second = StringIO()

        show_db(db2, first, sort=True)
        show_db(db3, second, sort=True)

        self.assertEqual(first.getvalue(), second.getvalue())
