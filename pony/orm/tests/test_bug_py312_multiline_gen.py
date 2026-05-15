"""
Regression tests for Python 3.12 decompiler bug with multiline generator expressions.

In Python 3.12, the single JUMP_BACKWARD instruction (shared between "filter failed,
try next item" and "value yielded, loop back") appears *after* YIELD_VALUE instead of
before it. Pony's decompiler expected to find that jump before the yield to locate
the filter boundary. When it wasn't there, an internal counter stayed at 0 and the
decompiler misclassified or/and short-circuits as filter conditions, crashing on any
multiline query with a compound filter or or/and in the element expression.
"""
import unittest

from pony import orm
from pony.orm.tests import setup_database, teardown_database

db = orm.Database()


class Product(db.Entity):
    name     = orm.Required(str)
    price    = orm.Required(float)
    discount = orm.Optional(float)


class TestPy312MultilineGenerator(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_database(db)
        with orm.db_session:
            Product(name='Widget',  price=9.99,  discount=0.10)
            Product(name='Gadget',  price=49.99, discount=None)
            Product(name='Doohick', price=4.99,  discount=0.05)

    @classmethod
    def tearDownClass(cls):
        teardown_database(db)

    def test_multiline_filter_or(self):
        # Single-line and multiline forms of the same query must return identical results.
        with orm.db_session:
            single = set(orm.select(p.name for p in Product if p.discount is not None or p.price < 5.0))
            multi  = set(orm.select(
                p.name
                for p in Product
                if (p.discount is not None or p.price < 5.0)
            ))
        self.assertEqual(single, multi)
        self.assertEqual(multi, {'Widget', 'Doohick'})

    def test_multiline_filter_and(self):
        with orm.db_session:
            single = set(orm.select(p.name for p in Product if p.discount is not None and p.price < 20.0))
            multi  = set(orm.select(
                p.name
                for p in Product
                if (p.discount is not None and p.price < 20.0)
            ))
        self.assertEqual(single, multi)
        self.assertEqual(multi, {'Widget', 'Doohick'})

    def test_multiline_multi_for_compound_filter(self):
        with orm.db_session:
            single = set(orm.select((p.name, q.name) for p in Product for q in Product if p.price < q.price and p.discount is not None))
            multi  = set(orm.select(
                (p.name, q.name)
                for p in Product
                for q in Product
                if p.price < q.price and p.discount is not None
            ))
        self.assertEqual(single, multi)
        self.assertEqual(multi, {('Widget', 'Gadget'), ('Doohick', 'Widget'), ('Doohick', 'Gadget')})
