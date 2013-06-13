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

class TestORMUndo(unittest.TestCase):
    def setUp(self):
        rollback()
        db_session.__enter__()
    def tearDown(self):
        rollback()
        db_session.__exit__()
    @raises_exception(ConstraintError, 'Attribute Student.group cannot be set to None')
    def test2(self):
        Student[1].group = None
    @raises_exception(ConstraintError, 'Attribute Student.group cannot be set to None')
    def test3(self):
        Group[101].students = Group[102].students

if __name__ == '__main__':
    unittest.main()
