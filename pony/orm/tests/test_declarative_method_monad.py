from __future__ import with_statement

import unittest
from pony.orm.core import *
from testutils import *

db = Database('sqlite', ':memory:')

class Student(db.Entity):
    name = Required(unicode)
    scholarship = Optional(int)

db.generate_mapping(create_tables=True)

with db_session:
    Student(id=1, name="Joe", scholarship=None)
    Student(id=2, name=" Bob ", scholarship=100)
    Student(id=3, name=" Beth ", scholarship=500)
    Student(id=4, name="Jon", scholarship=500)
    Student(id=5, name="Pete", scholarship=700)

class TestMethodMonad(unittest.TestCase):
    def setUp(self):
        rollback()
        db_session.__enter__()

    def tearDown(self):
        rollback()
        db_session.__exit__()

    def test1(self):
        students = set(select(s for s in Student if not s.name.startswith('J')))
        self.assertEqual(students, set([Student[2], Student[3], Student[5]]))

    def test1a(self):
        x = "Pe"
        students = select(s for s in Student if s.name.startswith(x))[:]
        self.assertEqual(students, [Student[5]])

    def test1b(self):
        students = set(select(s for s in Student if not not s.name.startswith('J')))
        self.assertEqual(students, set([Student[1], Student[4]]))

    def test1c(self):
        students = set(select(s for s in Student if not not not s.name.startswith('J')))
        self.assertEqual(students, set([Student[2], Student[3], Student[5]]))

    def test2(self):
        students = set(select(s for s in Student if s.name.endswith('e')))
        self.assertEqual(students, set([Student[1], Student[5]]))

    def test2a(self):
        x = "te"
        students = select(s for s in Student if s.name.endswith(x))[:]
        self.assertEqual(students, [Student[5]])

    def test3(self):
        students = select(s for s in Student if s.name.strip() == 'Beth')[:]
        self.assertEqual(students, [Student[3]])

    @raises_exception(TypeError, "'chars' argument must be of 'unicode' type in s.name.strip(5), got: 'int'")
    def test3a(self):
        students = select(s for s in Student if s.name.strip(5) == 'Beth')[:]

    def test4(self):
        students = select(s for s in Student if s.name.rstrip('n') == 'Jo')[:]
        self.assertEqual(students, [Student[4]])

    def test5(self):
        students = select(s for s in Student if s.name.lstrip('P') == 'ete')[:]
        self.assertEqual(students, [Student[5]])

    @raises_exception(TypeError, "Expected 'unicode' argument but got 'int' in expression s.name.startswith(5)")
    def test6(self):
        students = select(s for s in Student if not s.name.startswith(5))[:]

    @raises_exception(TypeError, "Expected 'unicode' argument but got 'int' in expression s.name.endswith(5)")
    def test7(self):
        students = select(s for s in Student if not s.name.endswith(5))[:]

    def test8(self):
        result = select(s for s in Student if s.name.upper() == "JOE")[:]
        self.assertEqual(result, [Student[1]])

    def test9(self):
        result = select(s for s in Student if s.name.lower() == "joe")[:]
        self.assertEqual(result, [Student[1]])

    @raises_exception(AttributeError, "'unicode' object has no attribute 'unknown'")
    def test10(self):
        result = set(select(s for s in Student if s.name.unknown() == "joe"))

if __name__ == "__main__":
    unittest.main()
