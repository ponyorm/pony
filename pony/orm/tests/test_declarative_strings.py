from __future__ import absolute_import, print_function, division

import unittest

from pony.orm.core import *
from pony.orm.tests.testutils import *

db = Database('sqlite', ':memory:')

class Student(db.Entity):
    name = Required(unicode, autostrip=False)
    foo = Optional(unicode)
    bar = Optional(unicode)

db.generate_mapping(create_tables=True)

with db_session:
    Student(id=1, name="Jon", foo='Abcdef', bar='b%d')
    Student(id=2, name=" Bob ", foo='Ab%def', bar='b%d')
    Student(id=3, name=" Beth ", foo='Ab_def', bar='b%d')
    Student(id=4, name="Jonathan")
    Student(id=5, name="Pete")

class TestStringMethods(unittest.TestCase):
    def setUp(self):
        rollback()
        db_session.__enter__()

    def tearDown(self):
        rollback()
        db_session.__exit__()

    def test_nonzero(self):
        result = set(select(s for s in Student if s.foo))
        self.assertEqual(result, {Student[1], Student[2], Student[3]})

    def test_add(self):
        name = 'Jonny'
        result = set(select(s for s in Student if s.name + "ny" == name))
        self.assertEqual(result, {Student[1]})

    def test_slice_1(self):
        result = set(select(s for s in Student if s.name[0:3] == "Jon"))
        self.assertEqual(result, {Student[1], Student[4]})

    def test_slice_2(self):
        result = set(select(s for s in Student if s.name[:3] == "Jon"))
        self.assertEqual(result, {Student[1], Student[4]})

    def test_slice_3(self):
        x = 3
        result = set(select(s for s in Student if s.name[:x] == "Jon"))
        self.assertEqual(result, {Student[1], Student[4]})

    def test_slice_4(self):
        x = 3
        result = set(select(s for s in Student if s.name[0:x] == "Jon"))
        self.assertEqual(result, {Student[1], Student[4]})

    def test_slice_5(self):
        result = set(select(s for s in Student if s.name[0:10] == "Jon"))
        self.assertEqual(result, {Student[1]})

    def test_slice_6(self):
        result = set(select(s for s in Student if s.name[0:] == "Jon"))
        self.assertEqual(result, {Student[1]})

    def test_slice_7(self):
        result = set(select(s for s in Student if s.name[:] == "Jon"))
        self.assertEqual(result, {Student[1]})

    def test_slice_8(self):
        result = set(select(s for s in Student if s.name[1:] == "on"))
        self.assertEqual(result, {Student[1]})

    def test_slice_9(self):
        x = 1
        result = set(select(s for s in Student if s.name[x:] == "on"))
        self.assertEqual(result, {Student[1]})

    def test_slice_10(self):
        x = 0
        result = set(select(s for s in Student if s.name[x:3] == "Jon"))
        self.assertEqual(result, {Student[1], Student[4]})

    def test_slice_11(self):
        x = 1
        y = 3
        result = set(select(s for s in Student if s.name[x:y] == "on"))
        self.assertEqual(result, {Student[1], Student[4]})

    def test_slice_12(self):
        x = 10
        y = 20
        result = set(select(s for s in Student if s.name[x:y] == ''))
        self.assertEqual(result, {Student[1], Student[2], Student[3], Student[4], Student[5]})

    def test_getitem_1(self):
        result = set(select(s for s in Student if s.name[1] == 'o'))
        self.assertEqual(result, {Student[1], Student[4]})

    def test_getitem_2(self):
        x = 1
        result = set(select(s for s in Student if s.name[x] == 'o'))
        self.assertEqual(result, {Student[1], Student[4]})

    def test_getitem_3(self):
        result = set(select(s for s in Student if s.name[-1] == 'n'))
        self.assertEqual(result, {Student[1], Student[4]})

    def test_getitem_4(self):
        x = -1
        result = set(select(s for s in Student if s.name[x] == 'n'))
        self.assertEqual(result, {Student[1], Student[4]})

    def test_contains_1(self):
        result = set(select(s for s in Student if 'o' in s.name))
        self.assertEqual(result, {Student[1], Student[2], Student[4]})

    def test_contains_2(self):
        result = set(select(s for s in Student if 'on' in s.name))
        self.assertEqual(result, {Student[1], Student[4]})

    def test_contains_3(self):
        x = 'on'
        result = set(select(s for s in Student if x in s.name))
        self.assertEqual(result, {Student[1], Student[4]})

    def test_contains_4(self):
        x = 'on'
        result = set(select(s for s in Student if x not in s.name))
        self.assertEqual(result, {Student[2], Student[3], Student[5]})

    def test_contains_5(self):
        result = set(select(s for s in Student if '%' in s.foo))
        self.assertEqual(result, {Student[2]})

    def test_contains_6(self):
        x = '%'
        result = set(select(s for s in Student if x in s.foo))
        self.assertEqual(result, {Student[2]})

    def test_contains_7(self):
        result = set(select(s for s in Student if '_' in s.foo))
        self.assertEqual(result, {Student[3]})

    def test_contains_8(self):
        x = '_'
        result = set(select(s for s in Student if x in s.foo))
        self.assertEqual(result, {Student[3]})

    def test_contains_9(self):
        result = set(select(s for s in Student if s.foo in 'Abcdef'))
        self.assertEqual(result, {Student[1], Student[4], Student[5]})

    def test_contains_10(self):
        result = set(select(s for s in Student if s.bar in s.foo))
        self.assertEqual(result, {Student[2], Student[4], Student[5]})

    def test_startswith_1(self):
        students = set(select(s for s in Student if s.name.startswith('J')))
        self.assertEqual(students, {Student[1], Student[4]})

    def test_startswith_2(self):
        students = set(select(s for s in Student if not s.name.startswith('J')))
        self.assertEqual(students, {Student[2], Student[3], Student[5]})

    def test_startswith_3(self):
        students = set(select(s for s in Student if not not s.name.startswith('J')))
        self.assertEqual(students, {Student[1], Student[4]})

    def test_startswith_4(self):
        students = set(select(s for s in Student if not not not s.name.startswith('J')))
        self.assertEqual(students, {Student[2], Student[3], Student[5]})

    def test_startswith_5(self):
        x = "Pe"
        students = select(s for s in Student if s.name.startswith(x))[:]
        self.assertEqual(students, [Student[5]])

    def test_endswith_1(self):
        students = set(select(s for s in Student if s.name.endswith('n')))
        self.assertEqual(students, {Student[1], Student[4]})

    def test_endswith_2(self):
        x = "te"
        students = select(s for s in Student if s.name.endswith(x))[:]
        self.assertEqual(students, [Student[5]])

    def test_strip_1(self):
        students = select(s for s in Student if s.name.strip() == 'Beth')[:]
        self.assertEqual(students, [Student[3]])

    def test_rstrip(self):
        students = select(s for s in Student if s.name.rstrip('n') == 'Jo')[:]
        self.assertEqual(students, [Student[1]])

    def test_lstrip(self):
        students = select(s for s in Student if s.name.lstrip('P') == 'ete')[:]
        self.assertEqual(students, [Student[5]])

    def test_upper(self):
        result = select(s for s in Student if s.name.upper() == "JON")[:]
        self.assertEqual(result, [Student[1]])

    def test_lower(self):
        result = select(s for s in Student if s.name.lower() == "jon")[:]
        self.assertEqual(result, [Student[1]])

if __name__ == "__main__":
    unittest.main()
