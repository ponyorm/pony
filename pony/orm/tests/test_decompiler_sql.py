"""SQL generation tests for the decompiler → translator pipeline.

Uses TestDatabase to intercept _exec_sql and capture the generated SQL
without a real database connection. The goal is to catch regressions where
the decompiler produces a valid-looking AST that generates wrong SQL.

Each test forces query execution (so the full pipeline runs), then asserts
structural properties of the captured SQL string.
"""
import sys
import unittest
from decimal import Decimal

from pony.orm.core import *
from pony.orm.tests.testutils import TestDatabase

db = TestDatabase()


class Product(db.Entity):
    name = Required(str)
    price = Required(Decimal, 10, 2)
    category = Required(str)
    in_stock = Required(bool)


class TestDecompilerSQL(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        db.bind(provider='sqlite', filename=':memory:')
        db.generate_mapping()

    @classmethod
    def tearDownClass(cls):
        db.disconnect()

    def setUp(self):
        rollback()
        db_session.__enter__()

    def tearDown(self):
        rollback()
        db_session.__exit__()

    def _run(self, query):
        """Execute query, return captured SQL (strips surrounding whitespace)."""
        list(query)
        return db.sql.strip()

    # -- Generator expression filters ------------------------------------------

    def test_simple_generator_filter(self):
        sql = self._run(select(p for p in Product if p.price > Decimal('10')))
        self.assertIn('SELECT', sql)
        self.assertIn('"Product"', sql)
        self.assertIn('"price"', sql)
        self.assertIn('WHERE', sql)

    def test_two_condition_and_filter(self):
        # Verifies both conditions survive the decompile→translate round-trip.
        # On Python 3.13, two adjacent LOAD_FAST ops become LOAD_FAST_LOAD_FAST;
        # both attributes must appear in the WHERE clause.
        sql = self._run(select(p for p in Product if p.price > Decimal('10') and p.in_stock))
        self.assertIn('"price"', sql)
        self.assertIn('"in_stock"', sql)
        self.assertIn('AND', sql)

    def test_or_filter(self):
        sql = self._run(select(p for p in Product if p.price < Decimal('5') or p.category == 'misc'))
        self.assertIn('"price"', sql)
        self.assertIn('"category"', sql)
        self.assertIn('OR', sql)

    def test_not_filter(self):
        sql = self._run(select(p for p in Product if not p.in_stock))
        self.assertIn('"in_stock"', sql)
        self.assertIn('WHERE', sql)

    def test_multiple_conditions_generator(self):
        sql = self._run(select(
            p for p in Product
            if p.in_stock and p.price < Decimal('20')
        ))
        self.assertIn('"in_stock"', sql)
        self.assertIn('"price"', sql)

    # -- Lambda filters --------------------------------------------------------

    def test_lambda_simple_filter(self):
        sql = self._run(Product.select(lambda p: p.category == 'hardware'))
        self.assertIn('"category"', sql)
        self.assertIn('WHERE', sql)

    def test_lambda_two_attribute_filter(self):
        # lambda that reads two attributes — LOAD_FAST_LOAD_FAST in Python 3.13
        sql = self._run(Product.select(lambda p: p.price > Decimal('10') and p.in_stock))
        self.assertIn('"price"', sql)
        self.assertIn('"in_stock"', sql)
        self.assertIn('AND', sql)

    def test_lambda_with_closure(self):
        threshold = Decimal('10')
        sql = self._run(Product.select(lambda p: p.price > threshold))
        self.assertIn('"price"', sql)
        self.assertIn('WHERE', sql)

    def test_lambda_with_default(self):
        # SET_FUNCTION_ATTRIBUTE (Python 3.13) must propagate lambda defaults
        # as visible constants when the query is translated.
        min_price = Decimal('10')

        def make_filter(threshold=min_price):
            return lambda p: p.price > threshold

        sql = self._run(Product.select(make_filter()))
        self.assertIn('"price"', sql)
        self.assertIn('WHERE', sql)

    # -- Subquery / exists() ---------------------------------------------------

    def test_exists_subquery(self):
        # Python 3.13 makes the outer loop var a cell var when an inner generator
        # references it; the cell var must resolve to the same "p" alias.
        sql = self._run(select(
            p for p in Product
            if exists(p2 for p2 in Product if p2.category == p.category and p2.id != p.id)
        ))
        self.assertIn('EXISTS', sql)
        self.assertIn('"category"', sql)

    def test_exists_correlated_uses_outer_alias(self):
        # The inner query must reference the outer alias, not a standalone value,
        # so that "p.category" in the subquery correlates with the outer row.
        sql = self._run(select(
            p for p in Product
            if exists(p2 for p2 in Product if p2.category == p.category)
        ))
        # Both the outer table and a second reference (inner) must appear
        self.assertEqual(sql.count('"Product"'), 2)

    # -- F-strings -------------------------------------------------------------

    def test_fstring_plain_variable(self):
        # FORMAT_SIMPLE: an f-string with a plain Python variable is folded to a
        # constant before translation, so the WHERE clause compares against a param.
        prefix = 'Widget'
        sql = self._run(select(p for p in Product if p.name == f'{prefix}'))
        self.assertIn('"name"', sql)
        self.assertIn('WHERE', sql)

    # -- Aggregates ------------------------------------------------------------

    def test_count_aggregate(self):
        with db_session:
            count(p for p in Product)
        sql = db.sql.strip()
        self.assertIn('COUNT', sql)
        self.assertIn('"Product"', sql)

    def test_min_aggregate(self):
        with db_session:
            min(p.price for p in Product)
        sql = db.sql.strip()
        self.assertIn('MIN', sql)
        self.assertIn('"price"', sql)

    def test_max_aggregate(self):
        with db_session:
            max(p.price for p in Product)
        sql = db.sql.strip()
        self.assertIn('MAX', sql)
        self.assertIn('"price"', sql)

    # -- Ordering --------------------------------------------------------------

    def test_order_by_single_attr(self):
        sql = self._run(select(p for p in Product).order_by(Product.price))
        self.assertIn('ORDER BY', sql)
        self.assertIn('"price"', sql)

    def test_order_by_desc(self):
        sql = self._run(select(p for p in Product).order_by(desc(Product.price)))
        self.assertIn('ORDER BY', sql)
        self.assertIn('DESC', sql)

    # -- Starargs --------------------------------------------------------------

    def test_starargs_call_in_filter(self):
        # LOAD_GLOBAL with push_null puts a NULL sentinel below the callable;
        # CALL_FUNCTION_EX must clean it up so the generator translates correctly.
        def fmt(*parts):
            return ' '.join(parts)

        sql = self._run(select(p for p in Product if p.name == fmt('Widget')))
        self.assertIn('"name"', sql)
        self.assertIn('WHERE', sql)


if __name__ == '__main__':
    unittest.main()
