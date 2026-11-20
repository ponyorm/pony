import ast
import unittest
from copy import deepcopy

from pony.utils import IntegerGenerator, get_lambda_args, pickle_ast, unpickle_ast


class PickleTest(unittest.TestCase):
    def test_persistent_id(self):
        src = ast.literal_eval("...")
        res = unpickle_ast(pickle_ast(src))
        self.assertEqual(res, src)

    def test_simple_ast(self):
        src = ast.parse("(x for x in [])")
        res = unpickle_ast(pickle_ast(src))  # pyright: ignore[reportArgumentType]
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


def parse_lambda(source):
    tree = ast.parse(source, mode="eval").body
    assert isinstance(tree, ast.Lambda)
    return tree


class GetLambdaArgsTest(unittest.TestCase):
    def test_names(self):
        self.assertEqual(
            get_lambda_args(parse_lambda("lambda x, y: x + y")), ["x", "y"]
        )
        self.assertEqual(get_lambda_args(lambda x, y: x + y), ["x", "y"])

    def test_varargs(self):
        message = r"^\*args is not supported$"
        with self.assertRaisesRegex(TypeError, message):
            get_lambda_args(parse_lambda("lambda *args: 1"))
        with self.assertRaisesRegex(TypeError, message):
            get_lambda_args(lambda *args: 1)

    def test_kwargs(self):
        message = r"^\*\*kwargs is not supported$"
        with self.assertRaisesRegex(TypeError, message):
            get_lambda_args(parse_lambda("lambda **kwargs: 1"))
        with self.assertRaisesRegex(TypeError, message):
            get_lambda_args(lambda **kwargs: 1)
