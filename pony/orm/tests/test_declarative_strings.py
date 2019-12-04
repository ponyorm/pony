from __future__ import absolute_import, print_function, division

import unittest

from pony.orm.core import *
from pony.orm.tests.testutils import *
from pony.orm.tests import setup_database, teardown_database

db = Database()

class Student(db.Entity):
    name = Required(str)
    unstripped = Required(str, autostrip=False)
    foo = Optional(str)
    bar = Optional(str)

class TestStringMethods(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_database(db)
        with db_session:
            Student(id=1, name="Ann", unstripped="Ann", foo='Abcdef', bar='b%d')
            Student(id=2, name="Bob", unstripped=" Bob ", foo='Ab%def', bar='b%d')
            Student(id=3, name="Beth", unstripped="  Beth  ", foo='Ab_def', bar='b%d')
            Student(id=4, name="Jonathan", unstripped="\nJonathan\n")
            Student(id=5, name="Pete", unstripped="\n Pete\n ")

    @classmethod
    def tearDownClass(cls):
        teardown_database(db)

    def setUp(self):
        rollback()
        db_session.__enter__()

    def tearDown(self):
        rollback()
        db_session.__exit__()

    def test_getitem_01(self):
        result = set(select(s for s in Student if s.name[:] == 'Ann'))
        self.assertEqual(result, {Student[1]})

    def test_getitem_1(self):
        result = set(select(s for s in Student if s.name[1] == 'o'))
        self.assertEqual(result, {Student[2], Student[4]})

    def test_getitem_2(self):
        x = 1
        result = set(select(s for s in Student if s.name[x] == 'o'))
        self.assertEqual(result, {Student[2], Student[4]})

    def test_getitem_3(self):
        result = set(select(s for s in Student if s.name[-1] == 'n'))
        self.assertEqual(result, {Student[1], Student[4]})

    def test_getitem_4(self):
        x = -1
        result = set(select(s for s in Student if s.name[x] == 'n'))
        self.assertEqual(result, {Student[1], Student[4]})

    def test_getitem_5(self):
        result = set(select(s for s in Student if s.name[-2] == 't'))
        self.assertEqual(result, {Student[3], Student[5]})

    @sql_debugging
    def test_getitem_6(self):
        x = -2
        select((s.name, s.name[x]) for s in Student).show()
        result = set(select(s for s in Student if s.name[x] == 't'))
        self.assertEqual(result, {Student[3], Student[5]})

    def test_slice_1(self):
        result = set(select(s for s in Student if s.name[0:3] == "Jon"))
        self.assertEqual(result, {Student[4]})

    def test_slice_2(self):
        result = set(select(s for s in Student if s.name[:3] == "Jon"))
        self.assertEqual(result, {Student[4]})

    def test_slice_3(self):
        x = 3
        result = set(select(s for s in Student if s.name[:x] == "Jon"))
        self.assertEqual(result, {Student[4]})

    def test_slice_4(self):
        x = 3
        result = set(select(s for s in Student if s.name[0:x] == "Jon"))
        self.assertEqual(result, {Student[4]})

    def test_slice_5(self):
        result = set(select(s for s in Student if s.name[0:10] == "Ann"))
        self.assertEqual(result, {Student[1]})

    def test_slice_6(self):
        result = set(select(s for s in Student if s.name[0:] == "Ann"))
        self.assertEqual(result, {Student[1]})

    def test_slice_7(self):
        result = set(select(s for s in Student if s.name[:] == "Ann"))
        self.assertEqual(result, {Student[1]})

    def test_slice_8(self):
        result = set(select(s for s in Student if s.name[1:] == "nn"))
        self.assertEqual(result, {Student[1]})

    def test_slice_9(self):
        x = 1
        result = set(select(s for s in Student if s.name[x:] == "nn"))
        self.assertEqual(result, {Student[1]})

    def test_slice_10(self):
        x = 0
        result = set(select(s for s in Student if s.name[x:3] == "Ann"))
        self.assertEqual(result, {Student[1]})

    def test_slice_11(self):
        result = set(select(s for s in Student if s.name[1:3] == "et"))
        self.assertEqual(result, {Student[3], Student[5]})

    def test_slice_12(self):
        x = 1
        y = 3
        result = set(select(s for s in Student if s.name[x:y] == "et"))
        self.assertEqual(result, {Student[3], Student[5]})

    def test_slice_13(self):
        x = 10
        y = 20
        result = set(select(s for s in Student if s.name[x:y] == ''))
        self.assertEqual(result, {Student[1], Student[2], Student[3], Student[4], Student[5]})

    def test_slice_14(self):
        result = set(select(s for s in Student if s.name[-2:] == "nn"))
        self.assertEqual(result, {Student[1]})

    def test_nonzero(self):
        result = set(select(s for s in Student if s.foo))
        self.assertEqual(result, {Student[1], Student[2], Student[3]})

    def test_add(self):
        name = 'Bethy'
        result = set(select(s for s in Student if s.name + "y" == name))
        self.assertEqual(result, {Student[3]})

    def test_contains_1(self):
        result = set(select(s for s in Student if 'o' in s.name))
        self.assertEqual(result, {Student[2], Student[4]})

    def test_contains_2(self):
        result = set(select(s for s in Student if 'an' in s.name))
        self.assertEqual(result, {Student[4]})

    def test_contains_3(self):
        x = 'an'
        result = set(select(s for s in Student if x in s.name))
        self.assertEqual(result, {Student[4]})

    def test_contains_4(self):
        x = 'an'
        result = set(select(s for s in Student if x not in s.name))
        self.assertEqual(result, {Student[1], Student[2], Student[3], Student[5]})

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
        students = set(select(s for s in Student if s.name.startswith('B')))
        self.assertEqual(students, {Student[2], Student[3]})

    def test_startswith_2(self):
        students = set(select(s for s in Student if not s.name.startswith('B')))
        self.assertEqual(students, {Student[1], Student[4], Student[5]})

    def test_startswith_3(self):
        students = set(select(s for s in Student if not not s.name.startswith('B')))
        self.assertEqual(students, {Student[2], Student[3]})

    def test_startswith_4(self):
        students = set(select(s for s in Student if not not not s.name.startswith('B')))
        self.assertEqual(students, {Student[1], Student[4], Student[5]})

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

    def test_rstrip_1(self):
        students = select(s for s in Student if s.name.rstrip('n') == 'A')[:]
        self.assertEqual(students, [Student[1]])

    def test_rstrip_2(self):
        x = 'n'
        students = select(s for s in Student if s.name.rstrip(x) == 'A')[:]
        self.assertEqual(students, [Student[1]])

    def test_lstrip(self):
        students = select(s for s in Student if s.name.lstrip('P') == 'ete')[:]
        self.assertEqual(students, [Student[5]])

    def test_upper(self):
        result = select(s for s in Student if s.name.upper() == "ANN")[:]
        self.assertEqual(result, [Student[1]])

    def test_lower(self):
        result = select(s for s in Student if s.name.lower() == "ann")[:]
        self.assertEqual(result, [Student[1]])

if __name__ == "__main__":
    unittest.main()
