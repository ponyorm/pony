from __future__ import absolute_import, print_function, division

import unittest

from pony.orm.core import *
from pony.orm.tests.testutils import *
from pony.orm.tests import setup_database, teardown_database

db = Database()

class Student(db.Entity):
    name = Required(unicode)
    scholarship = Optional(int)
    group = Required(int)


class TestOrderbyLimit(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_database(db)
        with db_session:
            Student(id=1, name="B", scholarship=None, group=41)
            Student(id=2, name="C", scholarship=700, group=41)
            Student(id=3, name="A", scholarship=500, group=42)
            Student(id=4, name="D", scholarship=500, group=43)
            Student(id=5, name="E", scholarship=700, group=42)

    @classmethod
    def tearDownClass(cls):
        teardown_database(db)

    def setUp(self):
        rollback()
        db_session.__enter__()

    def tearDown(self):
        rollback()
        db_session.__exit__()

    def test1(self):
        students = set(select(s for s in Student).order_by(Student.name))
        self.assertEqual(students, {Student[3], Student[1], Student[2], Student[4], Student[5]})

    def test2(self):
        students = set(select(s for s in Student).order_by(Student.name.asc))
        self.assertEqual(students, {Student[3], Student[1], Student[2], Student[4], Student[5]})

    def test3(self):
        students = set(select(s for s in Student).order_by(Student.id.desc))
        self.assertEqual(students, {Student[5], Student[4], Student[3], Student[2], Student[1]})

    def test4(self):
        students = set(select(s for s in Student).order_by(Student.scholarship.asc, Student.group.desc))
        self.assertEqual(students, {Student[1], Student[4], Student[3], Student[5], Student[2]})

    def test5(self):
        students = set(select(s for s in Student).order_by(Student.name).limit(3))
        self.assertEqual(students, {Student[3], Student[1], Student[2]})

    def test6(self):
        students = set(select(s for s in Student).order_by(Student.name).limit(3, 1))
        self.assertEqual(students, {Student[1], Student[2], Student[4]})

    def test7(self):
        q = select(s for s in Student).order_by(Student.name).limit(3, 1)
        students = set(q)
        self.assertEqual(students, {Student[1], Student[2], Student[4]})
        students = set(q)
        self.assertEqual(students, {Student[1], Student[2], Student[4]})

    # @raises_exception(TypeError, "query.order_by() arguments must be attributes. Got: 'name'")
    # now generate: ExprEvalError: name raises NameError: name 'name' is not defined
    # def test8(self):
    # students = select(s for s in Student).order_by("name")

    def test9(self):
        students = set(select(s for s in Student).order_by(Student.id)[1:4])
        self.assertEqual(students, {Student[2], Student[3], Student[4]})

    def test10(self):
        students = set(select(s for s in Student).order_by(Student.id)[:4])
        self.assertEqual(students, {Student[1], Student[2], Student[3], Student[4]})

    # @raises_exception(TypeError, "Parameter 'stop' of slice object should be specified")
    # def test11(self):
    #     students = select(s for s in Student).order_by(Student.id)[4:]

    @raises_exception(TypeError, "Parameter 'start' of slice object cannot be negative")
    def test12(self):
        students = select(s for s in Student).order_by(Student.id)[-3:2]

    @raises_exception(TypeError, 'If you want apply index to a query, convert it to list first')
    def test13(self):
        students = select(s for s in Student).order_by(Student.id)[3]
        self.assertEqual(students, Student[4])

    # @raises_exception(TypeError, 'If you want apply index to query, convert it to list first')
    # def test14(self):
    #    students = select(s for s in Student).order_by(Student.id)["a"]

    def test15(self):
        students = set(select(s for s in Student).order_by(Student.id)[0:4][1:3])
        self.assertEqual(students, {Student[2], Student[3]})

    def test16(self):
        students = set(select(s for s in Student).order_by(Student.id)[0:4][1:])
        self.assertEqual(students, {Student[2], Student[3], Student[4]})

    def test17(self):
        students = set(select(s for s in Student).order_by(Student.id)[:4][1:])
        self.assertEqual(students, {Student[2], Student[3], Student[4]})

    def test18(self):
        students = set(select(s for s in Student).order_by(Student.id)[:])
        self.assertEqual(students, {Student[1], Student[2], Student[3], Student[4], Student[5]})

    def test19(self):
        q = select(s for s in Student).order_by(Student.id)
        students = q[1:3]
        self.assertEqual(students, [Student[2], Student[3]])
        students = q[2:4]
        self.assertEqual(students, [Student[3], Student[4]])
        students = q[:]
        self.assertEqual(students, [Student[1], Student[2], Student[3], Student[4], Student[5]])

    def test20(self):
        q = select(s for s in Student).limit(offset=2)
        self.assertEqual(set(q), {Student[3], Student[4], Student[5]})
        last_sql = db.last_sql
        if db.provider.dialect == 'PostgreSQL':
            self.assertTrue('LIMIT null OFFSET 2' in last_sql)
        else:
            self.assertTrue('LIMIT -1 OFFSET 2' in last_sql)

    def test21(self):
        q = select(s for s in Student).limit(0, offset=2)
        self.assertEqual(set(q), set())

    def test22(self):
        q = select(s for s in Student).order_by(Student.id).limit(offset=1)
        self.assertEqual(set(q), {Student[2], Student[3], Student[4], Student[5]})

    def test23(self):
        q = select(s for s in Student)[2:2]
        self.assertEqual(set(q), set())
        self.assertTrue('LIMIT 0' in db.last_sql)

    def test24(self):
        q = select(s for s in Student)[2:]
        self.assertEqual(set(q), {Student[3], Student[4], Student[5]})

    def test25(self):
        q = select(s for s in Student)[:2]
        self.assertEqual(set(q), {Student[2], Student[1]})


if __name__ == "__main__":
    unittest.main()
