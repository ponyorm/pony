from __future__ import with_statement

import unittest
from testutils import *
from pony.orm.core import *

db = Database('sqlite', ':memory:', create_db=True)

class Student(db.Entity):
    id = PrimaryKey(int)
    name = Required(unicode)
    group = Required('Group')
    ext_info = Optional('ExtInfo')

class ExtInfo(db.Entity):
    id = PrimaryKey(int)
    info = Required(unicode)
    student = Optional(Student)

class Group(db.Entity):
    number = PrimaryKey(int)
    students = Set(Student)

db.generate_mapping(create_tables=True)

with db_session:
    g101 = Group(number=101)
    g102 = Group(number=102)

    s1 = Student(id=1, name='Student1', group=g101)
    s2 = Student(id=2, name='Student2', group=g102)
    ext_info = ExtInfo(id=100, info='ext_info1')
    ext_info2 = ExtInfo(id=200, info='ext_info2')

class TestOneToMany(unittest.TestCase):

    def setUp(self):
        rollback()
        db_session.__enter__()

    def tearDown(self):
        rollback()
        db_session.__exit__()

    @raises_exception(ConstraintError, 'Attribute Student.group cannot be set to None')
    def test_1(self):
        Student[1].group = None

    @raises_exception(ConstraintError, 'Attribute Student.group cannot be set to None')
    def test_2(self):
        Group[101].students = Group[102].students

    def test_3(self):
        g = Group[101]
        s2 = Student[2]  # s2 already loaded
        db._dblocal.last_sql = None
        g.students.add(s2)
        # Group.students.load should not attempt to load s2 from db
        self.assertEquals(db.last_sql, None)

if __name__ == '__main__':
    unittest.main()
