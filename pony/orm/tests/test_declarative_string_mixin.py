from __future__ import with_statement

import unittest
from pony.orm.core import *

db = Database('sqlite', ':memory:')

class Student(db.Entity):
    name = Required(unicode)

db.generate_mapping(create_tables=True)

with db_session:
    Student(id=1, name="ABCDEF")
    Student(id=2, name="Bob")
    Student(id=3, name="Beth")
    Student(id=4, name="Jon")
    Student(id=5, name="Pete")

class TestStringMixin(unittest.TestCase):
    def setUp(self):
        rollback()
        db_session.__enter__()

    def tearDown(self):
        rollback()
        db_session.__exit__()

    def test1(self):
        name = "ABCDEF5"
        result = set(select(s for s in Student if s.name + "5" == name))
        self.assertEqual(result, set([Student[1]]))

    def test2(self):
        result = set(select(s for s in Student if s.name[0:2] == "ABCDEF"[0:2]))
        self.assertEqual(result, set([Student[1]]))

    def test3(self):
        result = set(select(s for s in Student if s.name[1:100] == "ABCDEF"[1:100]))
        self.assertEqual(result, set([Student[1]]))

    def test4(self):
        result = set(select(s for s in Student if s.name[:] == "ABCDEF"))
        self.assertEqual(result, set([Student[1]]))

    def test5(self):
        result = set(select(s for s in Student if s.name[:3] == "ABCDEF"[0:3]))
        self.assertEqual(result, set([Student[1]]))

    def test6(self):
        x = 4
        result = set(select(s for s in Student if s.name[:x] == "ABCDEF"[:x]))

    def test7(self):
        result = set(select(s for s in Student if s.name[0:] == "ABCDEF"[0:]))
        self.assertEqual(result, set([Student[1]]))

    def test8(self):
        x = 2
        result = set(select(s for s in Student if s.name[x:] == "ABCDEF"[x:]))
        self.assertEqual(result, set([Student[1]]))

    def test9(self):
        x = 4
        result = set(select(s for s in Student if s.name[0:x] == "ABCDEF"[0:x]))
        self.assertEqual(result, set([Student[1]]))

    def test10(self):
        x = 0
        result = set(select(s for s in Student if s.name[x:3] == "ABCDEF"[x:3]))
        self.assertEqual(result, set([Student[1]]))

    def test11(self):
        x = 1
        y = 4
        result = set(select(s for s in Student if s.name[x:y] == "ABCDEF"[x:y]))
        self.assertEqual(result, set([Student[1]]))

    def test12(self):
        x = 10
        y = 20
        result = set(select(s for s in Student if s.name[x:y] == "ABCDEF"[x:y]))
        self.assertEqual(result, set([Student[1], Student[2], Student[3], Student[4], Student[5]]))

    def test13(self):
        result = set(select(s for s in Student if s.name[1] == "ABCDEF"[1]))
        self.assertEqual(result, set([Student[1]]))

    def test14(self):
        x = 1
        result = set(select(s for s in Student if s.name[x] == "ABCDEF"[x]))
        self.assertEqual(result, set([Student[1]]))

    def test15(self):
        x = -1
        result = set(select(s for s in Student if s.name[x] == "ABCDEF"[x]))
        self.assertEqual(result, set([Student[1]]))

    def test16(self):
        result = set(select(s for s in Student if 'o' in s.name))
        self.assertEqual(result, set([Student[2], Student[4]]))

    def test17(self):
        x = 'o'
        result = set(select(s for s in Student if x in s.name))
        self.assertEqual(result, set([Student[2], Student[4]]))

    def test18(self):
        result = set(select(s for s in Student if 'o' not in s.name))
        self.assertEqual(result, set([Student[1], Student[3], Student[5]]))

if __name__ == '__main__':
    unittest.main()
