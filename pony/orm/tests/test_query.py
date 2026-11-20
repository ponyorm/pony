import pickle
import unittest
from collections.abc import Iterator, Mapping
from datetime import date
from decimal import Decimal
from typing import TYPE_CHECKING

from pony.orm.core import *
from pony.orm.tests import setup_database, teardown_database
from pony.orm.tests.testutils import *

db = Database()

if TYPE_CHECKING:
    from pony.orm.core import Entity
else:
    Entity = db.Entity


class Student(Entity):
    name = Required(str)
    scholarship = Optional(int)
    gpa = Optional(Decimal, 3, 1)
    group = Required("Group")
    dob = Optional(date)


class Group(Entity):
    number = PrimaryKey(int)
    students = Set(Student)


class SimpleMapping(Mapping):
    """A read-only mapping that is not a dict and has no copy() method"""

    def __init__(self, data):
        self._data = dict(data)

    def __getitem__(self, key):
        return self._data[key]

    def __iter__(self) -> Iterator:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)


class TestQuery(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_database(db)
        with db_session:
            g1 = Group(number=1)
            Student(id=1, name="S1", group=g1, gpa=3.1)
            Student(
                id=2,
                name="S2",
                group=g1,
                gpa=3.2,
                scholarship=100,
                dob=date(2000, 1, 1),
            )
            Student(
                id=3,
                name="S3",
                group=g1,
                gpa=3.3,
                scholarship=200,
                dob=date(2001, 1, 2),
            )

    @classmethod
    def tearDownClass(cls):
        teardown_database(db)

    def setUp(self):
        rollback()
        db_session.__enter__()

    def tearDown(self):
        rollback()
        db_session.__exit__()

    @raises_exception(
        TypeError,
        "Query can only iterate over entity or another query (not a list of objects)",
    )
    def test1(self):
        select(s for s in [])

    @raises_exception(TypeError, "Cannot iterate over non-entity object X")
    def test2(self):
        X = [1, 2, 3]
        select("x for x in X")

    def test3(self):
        g = Group[1]
        students = select(s for s in g.students)
        self.assertEqual(set(g.students), set(students))

    @raises_exception(ExprEvalError, "`a` raises NameError: name 'a' is not defined")
    def test4(self):
        select(a for s in Student)  # error expected  # pyright: ignore[reportUndefinedVariable]

    @raises_exception(
        TypeError, "Incomparable types 'str' and 'StrArray' in expression: s.name == x"
    )
    def test5(self):
        x = ["A"]
        select(s for s in Student if s.name == x)

    def test6(self):
        def f1(x):
            return float(x) + 1

        students = select(s for s in Student if f1(s.gpa) > 4.25)[:]
        self.assertEqual({s.id for s in students}, {3})

    @raises_exception(NotImplementedError, "m1")
    def test7(self):
        class C1:
            def method1(self, a, b):
                return a + b

        c = C1()
        m1 = c.method1
        select(s for s in Student if m1(s.gpa, 1) > 3)

    @raises_exception(TypeError, "Expression `x` has unsupported type 'complex'")
    def test8(self):
        x = 1j
        select(s for s in Student if s.gpa == x)

    def test9(self):
        select(g for g in Group for s in db.Student)

    def test10(self):
        avg_gpa = avg(s.gpa for s in Student)
        self.assertEqual(round(avg_gpa, 6), 3.2)

    def test11(self):
        avg_gpa = avg(s.gpa for s in Student if s.id < 0)
        self.assertEqual(avg_gpa, None)

    def test12(self):
        sum_ss = sum(s.scholarship for s in Student)
        self.assertEqual(sum_ss, 300)

    def test13(self):
        sum_ss = sum(s.scholarship for s in Student if s.id < 0)
        self.assertEqual(sum_ss, 0)

    @raises_exception(TypeError, "'avg' is valid for numeric attributes only")
    def test14(self):
        avg(s.name for s in Student)

    def wrapper(self):
        return count(s for s in Student if s.scholarship > 0)

    def test15(self):
        c = self.wrapper()
        c = self.wrapper()
        self.assertEqual(c, 2)

    def test16(self):
        c = count(s.scholarship for s in Student if s.scholarship > 0)
        self.assertEqual(c, 2)

    def test17(self):
        s = get(s.scholarship for s in Student if s.id == 3)
        self.assertEqual(s, 200)

    def test18(self):
        s = get(s.scholarship for s in Student if s.id == 4)
        self.assertEqual(s, None)

    def test19(self):
        s = select(s for s in Student if s.id == 4).exists()
        self.assertEqual(s, False)

    def test20(self):
        r = min(s.scholarship for s in Student)
        self.assertEqual(r, 100)

    def test21(self):
        r = min(s.scholarship for s in Student if s.id < 2)
        self.assertEqual(r, None)

    def test22(self):
        r = max(s.scholarship for s in Student)
        self.assertEqual(r, 200)

    def test23(self):
        r = max(s.dob.year for s in Student)
        self.assertEqual(r, 2001)

    def test_select_kwargs_1(self):
        r = Student.select(scholarship=200)[:]
        self.assertEqual(r, [Student[3]])

    def test_select_kwargs_1a(self):
        g = Group[1]
        r = g.students.select(scholarship=200)[:]
        self.assertEqual(r, [Student[3]])

    def test_select_kwargs_2(self):
        r = Student.select(scholarship=1000)[:]
        self.assertEqual(r, [])

    def test_select_kwargs_2a(self):
        g = Group[1]
        r = g.students.select(scholarship=1000)[:]
        self.assertEqual(r, [])

    def test_select_kwargs_3(self):
        r = Student.select(group=Group[1])[:]
        self.assertEqual(set(r), {Student[1], Student[2], Student[3]})

    def test_select_kwargs_3a(self):
        g = Group[1]
        r = g.students.select(group=g)[:]
        self.assertEqual(set(r), {Student[1], Student[2], Student[3]})

    def test_select_kwargs_4(self):
        r = Student.select(group=Group[1], scholarship=200)[:]
        self.assertEqual(r, [Student[3]])

    def test_select_kwargs_4a(self):
        g = Group[1]
        r = g.students.select(group=g, scholarship=200)[:]
        self.assertEqual(r, [Student[3]])

    def test_first1(self):
        q = select(s for s in Student).order_by(Student.gpa)
        self.assertEqual(q.first(), Student[1])

    def test_first2(self):
        q = select((s.name, s.group) for s in Student)
        self.assertEqual(q.first(), ("S1", Group[1]))

    def test_first3(self):
        q = select(s for s in Student)
        self.assertEqual(q.first(), Student[1])

    def test_closures_1(self):
        def find_by_gpa(gpa):
            return lambda s: s.gpa > gpa

        fn = find_by_gpa(Decimal("3.1"))
        students = list(Student.select(fn))
        self.assertEqual(students, [Student[2], Student[3]])

    def test_closures_2(self):
        def find_by_gpa(gpa):
            return lambda s: s.gpa > gpa

        fn = find_by_gpa(Decimal("3.1"))
        q = select(s for s in Student)
        q = q.filter(fn)
        self.assertEqual(list(q), [Student[2], Student[3]])

    @raises_exception(
        NameError, "Free variable `gpa` referenced before assignment in enclosing scope"
    )
    def test_closures_3(self):
        def find_by_gpa():
            if False:
                gpa = Decimal("3.1")
            return lambda s: s.gpa > gpa  # error expected  # pyright: ignore[reportUndefinedVariable]

        fn = find_by_gpa()
        students = list(Student.select(fn))

    def test_pickle(self):
        objects = select(s for s in Student if s.scholarship > 0).order_by(
            desc(Student.id)
        )
        data = pickle.dumps(objects)
        rollback()
        objects = pickle.loads(data)
        self.assertEqual([obj.id for obj in objects], [3, 2])

    def test_bulk_delete_clear_query_cache(self):
        students1 = Student.select(lambda s: s.id > 1).order_by(Student.id)[:]
        self.assertEqual([s.id for s in students1], [2, 3])
        Student.select(lambda s: s.id < 3).delete(bulk=True)
        students2 = Student.select(lambda s: s.id > 1).order_by(Student.id)[:]
        self.assertEqual([s.id for s in students2], [3])
        rollback()
        students1 = Student.select(lambda s: s.id > 1).order_by(Student.id)[:]
        self.assertEqual([s.id for s in students1], [2, 3])

    def test_explicit_globals_and_locals(self):
        env = {"Student": Student}
        names = select("s.name for s in Student if s.id > x", env, {"x": 1})
        self.assertEqual(sorted(names), ["S2", "S3"])

    def test_explicit_locals_none_str(self):
        env = {"Student": Student}
        names = select("s.name for s in Student", env, None)
        self.assertEqual(sorted(names), ["S1", "S2", "S3"])

    def test_explicit_locals_none_lambda(self):
        env = {"Student": Student}
        students = Student.select("lambda s: s.id == 1", env, None)
        self.assertEqual([s.id for s in students], [1])

    def test_explicit_locals_none_generator(self):
        env = {"Student": Student}
        gen = (s.name for s in Student)
        names = select(gen, env, None)
        self.assertEqual(sorted(names), ["S1", "S2", "S3"])

    def test_explicit_locals_mapping_generator(self):
        env = {"Student": Student}
        x = 1
        gen = (s.name for s in Student if s.id > x)
        names = select(gen, env, SimpleMapping({}))
        self.assertEqual(sorted(names), ["S2", "S3"])

    def test_explicit_locals_mapping_str_lambda(self):
        env = {"Student": Student}
        locals_ = SimpleMapping({"x": 1})
        students = Student.select("lambda s: s.id > x", env, locals_)
        self.assertEqual([s.id for s in students.order_by(1)], [2, 3])

    def test_explicit_locals_mapping_closure(self):
        env = {"Student": Student}
        x = 2
        students = Student.select(lambda s: s.id > x, env, SimpleMapping({}))
        self.assertEqual([s.id for s in students], [3])

    def test_select_by_sql_mapping_locals(self):
        env = {"Student": Student}
        students = Student.select_by_sql(
            "select * from Student where id > $x", env, SimpleMapping({"x": 1})
        )
        self.assertEqual(sorted(s.id for s in students), [2, 3])

    @raises_exception(
        TypeError,
        "The third positional arguments should be locals dictionary. Got: 5",
    )
    def test_explicit_locals_invalid(self):
        env = {"Student": Student}
        select("s for s in Student", env, 5)  # pyright: ignore[reportArgumentType]


if __name__ == "__main__":
    unittest.main()
