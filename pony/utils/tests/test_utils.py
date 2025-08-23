import ast
import unittest
from copy import deepcopy

from pony.py23compat import PY36
from pony.utils import pickle_ast, unpickle_ast, IntegerGenerator


class PickleTest(unittest.TestCase):
    @unittest.skipUnless(PY36, "requires Python 3")
    def test_persistent_id(self):
        src = ast.literal_eval("...")
        res = unpickle_ast(pickle_ast(src))
        self.assertEqual(res, src)

    def test_simple_ast(self):
        src = ast.parse("(x for x in [])")
        res = unpickle_ast(pickle_ast(src))
        self.assertEqual(ast.dump(res), ast.dump(src))


class IntegerGeneratorTest(unittest.TestCase):
    def test_from_zero(self):
        c = IntegerGenerator()
        self.assertEqual(next(c), 0)
        self.assertEqual(next(c), 1)

    def test_from_start(self):
        c = IntegerGenerator(42)
        self.assertEqual(next(c), 42)
        self.assertEqual(next(c), 43)

    def test_copy(self):
        c = IntegerGenerator(42)
        self.assertEqual(next(c), 42)
        d = deepcopy(c)
        self.assertEqual(next(c), next(d))
