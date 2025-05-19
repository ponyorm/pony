import ast
import re
import sys
import textwrap
import unittest

from pony.orm.asttranslation import ast2src
from pony.orm.decompiling import Decompiler, test_lines


def generate_gens():
    patterns = [
        "(x * y) * [z * j)",
        "([x * y) * z) * j",
        "(x * [y * z)) * j",
        "x * ([y * z) * j)",
        "x * (y * [z * j))",
    ]

    ops = ("and", "or")
    nots = (True, False)

    result = []

    for pat in patterns:
        cur = pat
        for op1 in ops:
            for op2 in ops:
                for op3 in ops:
                    res = cur.replace("*", op1, 1)
                    res = res.replace("*", op2, 1)
                    res = res.replace("*", op3, 1)
                    result.append(res)

    final = []

    for res in result:
        for par1 in nots:
            for par2 in nots:
                for a in nots:
                    for b in nots:
                        for c in nots:
                            for d in nots:
                                cur = res.replace("(", "not(") if not par1 else res
                                if not par2:
                                    cur = cur.replace("[", "not(")
                                else:
                                    cur = cur.replace("[", "(")
                                if not a:
                                    cur = cur.replace("x", "not x")
                                if not b:
                                    cur = cur.replace("y", "not y")
                                if not c:
                                    cur = cur.replace("z", "not z")
                                if not d:
                                    cur = cur.replace("j", "not j")
                                final.append(cur)

    return final


def create_test(gen):
    def wrapped_test(self):
        def get_condition_values(cond):
            result = []
            vals = (True, False)
            for x in vals:
                for y in vals:
                    for z in vals:
                        for j in vals:
                            result.append(eval(cond, {"x": x, "y": y, "z": z, "j": j}))
            return result

        src1 = "(a for a in [] if %s)" % gen
        src2 = "lambda x, y, z, j: (%s)" % gen
        src3 = "(m for m in [] if %s for n in [] if %s)" % (gen, gen)

        code1 = compile(src1, "<?>", "eval").co_consts[0]
        ast1 = Decompiler(code1).ast
        res1 = ast2src(ast1).replace(".0", "[]")
        res1 = res1[res1.find("if") + 2 : -1]

        code2 = compile(src2, "<?>", "eval").co_consts[0]
        ast2 = Decompiler(code2).ast
        res2 = ast2src(ast2).replace(".0", "[]")
        res2 = res2[res2.find(":") + 1 :]

        code3 = compile(src3, "<?>", "eval").co_consts[0]
        ast3 = Decompiler(code3).ast
        res3 = ast2src(ast3).replace(".0", "[]")
        res3 = res3[res3.find("if") + 2 : res3.rfind("for") - 1]

        if get_condition_values(gen) != get_condition_values(res1):
            self.fail("Incorrect generator decompilation: %s -> %s" % (gen, res1))

        if get_condition_values(gen) != get_condition_values(res2):
            self.fail("Incorrect lambda decompilation: %s -> %s" % (gen, res2))

        if get_condition_values(gen) != get_condition_values(res3):
            self.fail(
                "Incorrect multi-for generator decompilation: %s -> %s" % (gen, res3)
            )

    return wrapped_test


def create_test_line_test(line_number, source):
    def wrapped_test(self):
        self.assertTestLineDecompiles(source)

    wrapped_test.__name__ = "test_decompiling_line_%03d" % line_number
    wrapped_test.__doc__ = source
    return wrapped_test


class TestDecompiler(unittest.TestCase):
    def assertTestLineDecompiles(self, source):
        code = compile(source, "<?>", "eval").co_consts[0]
        expected = ast.parse(source).body[0]
        expected.value.generators[0].iter.id = ".0"
        actual = ast.Expr(Decompiler(code).ast)
        self.maxDiff = None
        self.assertEqual(ast.dump(expected), ast.dump(actual))

    def assertDecompilesTo(self, src, expected):
        # skip test due to ast.dump has no indent parameter
        if sys.version_info[:2] <= (3, 8):  # noqa: UP036
            return

        code = compile(src, "<?>", "eval").co_consts[0]
        # import dis
        # print(dis.dis(code))
        dc = Decompiler(code)
        expected = textwrap.dedent(expected).strip()
        self.maxDiff = None
        self.assertMultiLineEqual(expected, ast.dump(dc.ast, indent=2))

    def test_ast1(self):
        self.assertDecompilesTo(
            "(a for a in [] if x and y and z and j)",
            """
            GeneratorExp(
              elt=Name(id='a', ctx=Load()),
              generators=[
                comprehension(
                  target=Name(id='a', ctx=Store()),
                  iter=Name(id='.0', ctx=Load()),
                  ifs=[
                    BoolOp(
                      op=And(),
                      values=[
                        Name(id='x', ctx=Load()),
                        Name(id='y', ctx=Load()),
                        Name(id='z', ctx=Load()),
                        Name(id='j', ctx=Load())])],
                  is_async=0)])
            """,
        )

    def test_ast2(self):
        self.assertDecompilesTo(
            "lambda x, y, z, j: (x and y and z and j)",
            """
            BoolOp(
              op=And(),
              values=[
                Name(id='x', ctx=Load()),
                Name(id='y', ctx=Load()),
                Name(id='z', ctx=Load()),
                Name(id='j', ctx=Load())])
            """,
        )

    def test_ast3(self):
        self.assertDecompilesTo(
            "(m for m in [] if x and y and z and j for n in [] if x and y and z and j)",
            """
            GeneratorExp(
              elt=Name(id='m', ctx=Load()),
              generators=[
                comprehension(
                  target=Name(id='m', ctx=Store()),
                  iter=Name(id='.0', ctx=Load()),
                  ifs=[
                    BoolOp(
                      op=And(),
                      values=[
                        Name(id='x', ctx=Load()),
                        Name(id='y', ctx=Load()),
                        Name(id='z', ctx=Load()),
                        Name(id='j', ctx=Load())])],
                  is_async=0),
                comprehension(
                  target=Name(id='n', ctx=Store()),
                  iter=Constant(value=()),
                  ifs=[
                    BoolOp(
                      op=And(),
                      values=[
                        Name(id='x', ctx=Load()),
                        Name(id='y', ctx=Load()),
                        Name(id='z', ctx=Load()),
                        Name(id='j', ctx=Load())])],
                  is_async=0)])
            """,
        )

    def test_ast_none(self):
        self.assertDecompilesTo(
            "(m for m in [] if (x is None or y))",
            """
            GeneratorExp(
              elt=Name(id='m', ctx=Load()),
              generators=[
                comprehension(
                  target=Name(id='m', ctx=Store()),
                  iter=Name(id='.0', ctx=Load()),
                  ifs=[
                    BoolOp(
                      op=Or(),
                      values=[
                        Compare(
                          left=Name(id='x', ctx=Load()),
                          ops=[
                            Is()],
                          comparators=[
                            Constant(value=None)]),
                        Name(id='y', ctx=Load())])],
                  is_async=0)])

            """,
        )

    def test_ast_not_none(self):
        self.assertDecompilesTo(
            "(m for m in [] if (x is not None or y))",
            """
            GeneratorExp(
              elt=Name(id='m', ctx=Load()),
              generators=[
                comprehension(
                  target=Name(id='m', ctx=Store()),
                  iter=Name(id='.0', ctx=Load()),
                  ifs=[
                    BoolOp(
                      op=Or(),
                      values=[
                        Compare(
                          left=Name(id='x', ctx=Load()),
                          ops=[
                            IsNot()],
                          comparators=[
                            Constant(value=None)]),
                        Name(id='y', ctx=Load())])],
                  is_async=0)])

            """,
        )

    def test_multiline_jump_to_loop(self):
        for condition in ("x", "not x", "x is None", "x is not None"):
            with self.subTest(condition=condition):
                source = (
                    """(m
                    for m in T
                    if %s)"""
                    % condition
                )
                self.assertTestLineDecompiles(source)

    def test_ast_multiline(self):
        expr = """(m
                for m in []
                if (x is None or y))"""
        expected_result = """
            GeneratorExp(
              elt=Name(id='m', ctx=Load()),
              generators=[
                comprehension(
                  target=Name(id='m', ctx=Store()),
                  iter=Name(id='.0', ctx=Load()),
                  ifs=[
                    BoolOp(
                      op=Or(),
                      values=[
                        Compare(
                          left=Name(id='x', ctx=Load()),
                          ops=[
                            Is()],
                          comparators=[
                            Constant(value=None)]),
                        Name(id='y', ctx=Load())])],
                  is_async=0)])
            """
        self.assertDecompilesTo(re.sub("\n", " ", expr), expected_result)
        self.assertDecompilesTo(expr, expected_result)

    def test_ast_copy(self):
        expr = """( s
                for s in DirectorySyncSettings
                if s.enabled and s.company == self.company and (not self.id or s.id != self.id)
            )"""

        expected_result = """
            GeneratorExp(
              elt=Name(id='s', ctx=Load()),
              generators=[
                comprehension(
                  target=Name(id='s', ctx=Store()),
                  iter=Name(id='.0', ctx=Load()),
                  ifs=[
                    BoolOp(
                      op=And(),
                      values=[
                        Attribute(
                          value=Name(id='s', ctx=Load()),
                          attr='enabled',
                          ctx=Load()),
                        Compare(
                          left=Attribute(
                            value=Name(id='s', ctx=Load()),
                            attr='company',
                            ctx=Load()),
                          ops=[
                            Eq()],
                          comparators=[
                            Attribute(
                              value=Name(id='self', ctx=Load()),
                              attr='company',
                              ctx=Load())]),
                        BoolOp(
                          op=Or(),
                          values=[
                            UnaryOp(
                              op=Not(),
                              operand=Attribute(
                                value=Name(id='self', ctx=Load()),
                                attr='id',
                                ctx=Load())),
                            Compare(
                              left=Attribute(
                                value=Name(id='s', ctx=Load()),
                                attr='id',
                                ctx=Load()),
                              ops=[
                                NotEq()],
                              comparators=[
                                Attribute(
                                  value=Name(id='self', ctx=Load()),
                                  attr='id',
                                  ctx=Load())])])])],
                  is_async=0)])

        """

        self.assertDecompilesTo(re.sub("\n", " ", expr), expected_result)
        self.assertDecompilesTo(expr, expected_result)


for i, gen in enumerate(generate_gens()):
    generated_test = create_test(gen)
    generated_test.__name__ = "test_decompiler_%d" % i
    setattr(TestDecompiler, generated_test.__name__, generated_test)


for line_number, source in enumerate(test_lines.split("\n")):
    source = source.strip()
    if not source or source.startswith("#"):
        continue
    generated_test = create_test_line_test(line_number, source)
    setattr(TestDecompiler, generated_test.__name__, generated_test)
