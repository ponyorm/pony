import unittest
from pony.orm import *
from pony.db import Database
from model1 import *

class TestCRUD(unittest.TestCase):
    def setUp(self):
        prepare_database()

    def test_create(self):
        g1 = Group.create(number='1', department=2)

        self.assertEqual(Student._cached_create_sql_, None)
        s1 = Student.create(record=3, name='A', group=g1)
        s2 = Student.create(record=4, name='B', group=g1, scholarship=500)

        commit()
        sql = 'INSERT INTO "Students" ("record", "fio", "group", "scholarship") VALUES (?, ?, ?, ?)'
        self.assertEqual(Student._cached_create_sql_[0], sql)

        g1_row = db.get('* from Groups where number = 1')
        self.assertEqual(g1_row, ('1', 2))

        s1_row = db.get('* from Students where record = 3')
        self.assertEqual(s1_row, (3, 'A', '1', 0))

        s2_row = db.get('* from Students where record = 4')
        self.assertEqual(s2_row, (4, 'B', '1', 500))
        
    def test_read_find_all(self):
        students = Student.find_all()
        self.assertEqual(len(students), 5)

if __name__ == '__main__':
    unittest.main()    