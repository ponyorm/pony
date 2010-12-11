import unittest
from pony.orm import *
from pony.db import Database
from model1 import *

class TestCRUD(unittest.TestCase):
    def setUp(self):
        local.trans = Transaction()

    def test_find_all(self):
        students = Student.find_all()
        self.assertEqual(len(students), 5)

if __name__ == '__main__':
    unittest.main()    