import unittest
from decimal import Decimal

from pony.orm.core import *
from pony.orm.tests import only_for, setup_database, teardown_database

db = Database()


class Numbers(db.Entity):
    id = PrimaryKey(int)
    a = Optional(int)
    b = Optional(int)
    x = Optional(float)
    d = Optional(Decimal)


@only_for("sqlite")
class TestSQLiteDivision(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_database(db)
        with db_session:
            Numbers(id=1, a=7, b=2, x=2.5, d=Decimal("1.25"))
            Numbers(id=2, a=-7, b=2, x=-2.5, d=Decimal("2.75"))
            Numbers(id=3, a=7, b=-2, x=0.5, d=Decimal("0.50"))
            Numbers(id=4, a=-8, b=-3, x=-3.75, d=Decimal("3.00"))
            Numbers(id=5)  # all attributes are None

    @classmethod
    def tearDownClass(cls):
        teardown_database(db)

    def setUp(self):
        rollback()
        db_session.__enter__()

    def tearDown(self):
        rollback()
        db_session.__exit__()

    def test_int_floor_division(self):
        result = dict(select((n.id, n.a // n.b) for n in Numbers if n.a is not None)[:])
        self.assertEqual(result, {1: 7 // 2, 2: -7 // 2, 3: 7 // -2, 4: -8 // -3})

    def test_int_modulo(self):
        result = dict(select((n.id, n.a % n.b) for n in Numbers if n.a is not None)[:])
        self.assertEqual(result, {1: 7 % 2, 2: -7 % 2, 3: 7 % -2, 4: -8 % -3})

    def test_int_true_division(self):
        result = dict(select((n.id, n.a / n.b) for n in Numbers if n.a is not None)[:])
        self.assertEqual(result, {1: 7 / 2, 2: -7 / 2, 3: 7 / -2, 4: -8 / -3})
        for value in result.values():
            self.assertIsInstance(value, float)

    def test_modulo_inside_filter(self):
        result = set(select(n.id for n in Numbers if n.a % n.b == 1)[:])
        self.assertEqual(result, {1, 2})

    def test_true_division_inside_filter(self):
        result = set(select(n.id for n in Numbers if n.a / n.b > 3)[:])
        self.assertEqual(result, {1})

    def test_float_floor_division_and_modulo(self):
        result = dict(select((n.id, n.x // 1) for n in Numbers if n.x is not None)[:])
        self.assertEqual(result, {1: 2.5 // 1, 2: -2.5 // 1, 3: 0.5 // 1, 4: -3.75 // 1})
        result = dict(select((n.id, n.x % 1) for n in Numbers if n.x is not None)[:])
        self.assertEqual(result, {1: 2.5 % 1, 2: -2.5 % 1, 3: 0.5 % 1, 4: -3.75 % 1})

    def test_none_operand_gives_none(self):
        for result in (
            select((n.id, n.a / n.b) for n in Numbers)[:],
            select((n.id, n.a // n.b) for n in Numbers)[:],
            select((n.id, n.a % n.b) for n in Numbers)[:],
        ):
            self.assertIsNone(dict(result)[5])

    def test_decimal_column(self):
        result = dict(select((n.id, n.d % 1) for n in Numbers if n.d is not None)[:])
        self.assertEqual(
            result,
            {1: Decimal("0.25"), 2: Decimal("0.75"), 3: Decimal("0.5"), 4: Decimal("0")},
        )
        result = dict(select((n.id, n.d // 1) for n in Numbers if n.d is not None)[:])
        self.assertEqual(
            result, {1: Decimal("1"), 2: Decimal("2"), 3: Decimal("0"), 4: Decimal("3")}
        )

    def test_decimal_parameter(self):
        divisor = Decimal("2.5")
        result = dict(select((n.id, n.a % divisor) for n in Numbers if n.a is not None)[:])
        expected = {1: 7 % divisor, 2: -7 % divisor, 3: 7 % divisor, 4: -8 % divisor}
        self.assertEqual(result, expected)
        for value in result.values():
            self.assertIsInstance(value, Decimal)


if __name__ == "__main__":
    unittest.main()
