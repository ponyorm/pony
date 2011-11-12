import unittest
from pony.orm import *

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

g101 = Group(number=101)
g102 = Group(number=102)

s1 = Student(id=1, name='Student1', group=g101)
s2 = Student(id=2, name='Student2', group=g102)
ext_info = ExtInfo(id=100, info='ext_info1')
ext_info2 = ExtInfo(id=200, info='ext_info2')
commit()

class TestOneToOne(unittest.TestCase):
    def setUp(self):
        db.rollback()
    def test_one_to_one1(self):
        s1 = Student[1]
        self.assertEqual(s1.ext_info, None)
        rollback()        

        s1 =  Student[1]
        info1 = ExtInfo[100]
        s1.ext_info = info1
        self.assertEqual(info1._curr_.get('student'), s1)
        commit()
        rollback()

        s1 = Student[1]
        info2 = ExtInfo[200]
        s1.ext_info = info2
        self.assertEqual(info2._curr_.get('student'), s1)
        info1 = ExtInfo[100]
        self.assertEqual(info1._curr_.get('student'), None)
        commit()
        rollback()

        s2 = Student[2]
        s2.ext_info = ExtInfo[200]
        commit()
        rollback()

        s1 =  Student[1]
        self.assertEqual(s1._curr_.get('ext_info'), None)
        s2 = Student[2]
        s2.ext_info = None
        commit()
        self.assert_(True)
    def test_one_to_one2(self):
        s1 = Student[1]
        info2 = ExtInfo[200]
        info2.student = s1
        self.assertEqual(s1._curr_.get('ext_info'), info2)
        commit()
        rollback()

        s2 = Student[2]        
        info2 = ExtInfo[200]
        info2.student = s2
        self.assertEqual(s2._curr_.get('ext_info'), info2)
        commit()
        rollback()
        
        info = ExtInfo[200]
        info.student = None
        commit()
        self.assert_(True)

if __name__ == '__main__':
    unittest.main()
