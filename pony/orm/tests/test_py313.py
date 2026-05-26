"""
End-to-end integration tests for Python 3.13 bytecode compatibility.

Each test exercises a code path that changed in Python 3.13 and verifies
the full pipeline (decompile → translate → SQL → DB result) works correctly.
"""
import sys
import unittest
from decimal import Decimal

from pony.orm.core import *
from pony.orm.tests.testutils import *
from pony.orm.tests import setup_database, teardown_database


db = Database()


class Product(db.Entity):
    name = Required(str)
    price = Required(Decimal, 10, 2)
    category = Required(str)
    in_stock = Required(bool)


class TestPy313Integration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_database(db)
        with db_session:
            Product(id=1, name='Widget',  price=Decimal('9.99'),  category='hardware', in_stock=True)
            Product(id=2, name='Gadget',  price=Decimal('49.99'), category='hardware', in_stock=False)
            Product(id=3, name='Doohickey', price=Decimal('4.99'), category='misc',    in_stock=True)
            Product(id=4, name='Thingamajig', price=Decimal('19.99'), category='misc', in_stock=True)
            Product(id=5, name='Whatsit', price=Decimal('99.99'), category='premium',  in_stock=False)

    @classmethod
    def tearDownClass(cls):
        teardown_database(db)

    def setUp(self):
        rollback()
        db_session.__enter__()

    def tearDown(self):
        rollback()
        db_session.__exit__()

    def test_nested_subquery_cellvar(self):
        # When an inner subquery references the outer generator's loop variable,
        # Python 3.13 makes that variable a cell var and LOAD_FAST's oparg spans
        # co_varnames + co_cellvars combined. exists() triggers this pattern.
        q = select(
            p for p in Product
            if exists(p2 for p2 in Product if p2.category == p.category and p2.id != p.id)
        ).order_by(Product.id)
        # hardware (1, 2) and misc (3, 4) each have a sibling in the same category
        self.assertEqual([x.id for x in q], [1, 2, 3, 4])

    def test_fused_opcode_lambda_filter(self):
        # Python 3.13 fuses adjacent LOAD_FAST pairs into LOAD_FAST_LOAD_FAST;
        # a filter lambda that reads two attributes exercises this in its body
        q = Product.select(lambda p: p.price > Decimal('10') and p.in_stock)
        ids = sorted(p.id for p in q)
        self.assertEqual(ids, [4])

    def test_lambda_with_default_in_filter(self):
        # Python 3.13 replaced MAKE_FUNCTION flags with SET_FUNCTION_ATTRIBUTE;
        # a lambda with a default arg must have its default correctly restored
        # so the value is visible as a constant when the query is translated
        min_price = Decimal('10')

        def make_filter(threshold=min_price):
            return lambda p: p.price > threshold

        q = Product.select(make_filter())
        ids = sorted(p.id for p in q)
        self.assertEqual(ids, [2, 4, 5])

    @raises_exception(NotImplementedError, 'You cannot specify conversion type for f-string expression in query')
    def test_fstring_ascii_conversion_raises(self):
        # CONVERT_VALUE with !a (Python 3.13's f-string opcode for ascii())
        # must surface a clear NotImplementedError, not a crash on an unknown opcode
        select(p.id for p in Product if f'{p.name!a}')[:]

    def test_fstring_variable_interpolation(self):
        # FORMAT_SIMPLE — an f-string interpolating a plain Python variable
        # is folded to a constant by PonyORM and must produce the correct result
        prefix = 'Widget'
        q = select(p.id for p in Product if p.name == f'{prefix}')
        self.assertEqual(list(q), [1])

    @raises_exception(NotImplementedError, 'You cannot set width and precision for f-string expression in query')
    def test_fstring_format_spec_raises(self):
        # FORMAT_WITH_SPEC for a DB-attribute expression must surface a clear
        # NotImplementedError, not crash with an unhandled opcode
        select(p.id for p in Product if f'{p.price:.2f}')[:]

    def test_call_result_used_as_callable(self):
        # Python 3.11+ inserts PUSH_NULL after a call result before treating it
        # as a callable (foo()(...) pattern); the decompiler must handle the sentinel
        def make_threshold():
            return Decimal('10')

        q = select(p.id for p in Product if p.price > make_threshold())
        ids = sorted(list(q))
        self.assertEqual(ids, [2, 4, 5])

    def test_starargs_in_query_element(self):
        # LOAD_GLOBAL with push_null leaves a NULL sentinel below the callable;
        # CALL_FUNCTION_EX must clean it up so the generator element decompiles correctly
        def fmt(*parts):
            return ' '.join(parts)

        q = select(p.id for p in Product if p.name == fmt('Widget'))
        self.assertEqual(list(q), [1])

    def test_list_comp_over_query_result(self):
        # Python 3.13 inlines list comprehensions, causing GET_ITER to be called
        # on the query result iterator — requires QueryResultIterator.__iter__
        q = Product.select(lambda p: p.in_stock).order_by(Product.id)
        names = [p.name for p in q]
        self.assertEqual(names, ['Widget', 'Doohickey', 'Thingamajig'])

    def test_list_of_query_result(self):
        # list() on a query also goes through the iterator protocol
        result = list(Product.select(lambda p: p.category == 'hardware').order_by(Product.id))
        self.assertEqual([p.id for p in result], [1, 2])

    def test_query_with_multiple_conditions(self):
        # comprehension nodes built internally must include is_async=0,
        # which became a required field in Python 3.13
        q = select(
            p for p in Product
            if p.in_stock and p.price < Decimal('20')
        ).order_by(Product.id)
        self.assertEqual([p.id for p in q], [1, 3, 4])


if __name__ == '__main__':
    unittest.main()
