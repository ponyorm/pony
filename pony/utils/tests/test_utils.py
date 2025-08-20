import ast
import unittest
from pony.py23compat import PY36
from pony.utils import pickle_ast, unpickle_ast


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
