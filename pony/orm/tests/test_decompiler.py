import textwrap
import unittest
import ast
import sys
import re

from pony.orm.decompiling import Decompiler
from pony.orm.asttranslation import ast2src


def generate_gens():
    patterns = [
        '(x * y) * [z * j)',
        '([x * y) * z) * j',
        '(x * [y * z)) * j',
        'x * ([y * z) * j)',
        'x * (y * [z * j))'
    ]

    ops = ('and', 'or')
    nots = (True, False)

    result = []

    for pat in patterns:
        cur = pat
        for op1 in ops:
            for op2 in ops:
                for op3 in ops:
                    res = cur.replace('*', op1, 1)
                    res = res.replace('*', op2, 1)
                    res = res.replace('*', op3, 1)
                    result.append(res)

    final = []

    for res in result:
        for par1 in nots:
            for par2 in nots:
                for a in nots:
                    for b in nots:
                        for c in nots:
                            for d in nots:
                                cur = res.replace('(', 'not(') if not par1 else res
                                if not par2:
                                    cur = cur.replace('[', 'not(')
                                else:
                                    cur = cur.replace('[', '(')
                                if not a: cur = cur.replace('x', 'not x')
                                if not b: cur = cur.replace('y', 'not y')
                                if not c: cur = cur.replace('z', 'not z')
                                if not d: cur = cur.replace('j', 'not j')
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
                            result.append(eval(cond, {'x': x, 'y': y, 'z': z, 'j': j}))
            return result
        src1 = '(a for a in [] if %s)' % gen
        src2 = 'lambda x, y, z, j: (%s)' % gen
        src3 = '(m for m in [] if %s for n in [] if %s)' % (gen, gen)

        code1 = compile(src1, '<?>', 'eval').co_consts[0]
        ast1 = Decompiler(code1).ast
        res1 = ast2src(ast1).replace('.0', '[]')
        res1 = res1[res1.find('if')+2:-1]

        code2 = compile(src2, '<?>', 'eval').co_consts[0]
        ast2 = Decompiler(code2).ast
        res2 = ast2src(ast2).replace('.0', '[]')
        res2 = res2[res2.find(':')+1:]

        code3 = compile(src3, '<?>', 'eval').co_consts[0]
        ast3 = Decompiler(code3).ast
        res3 = ast2src(ast3).replace('.0', '[]')
        res3 = res3[res3.find('if')+2: res3.rfind('for')-1]

        if get_condition_values(gen) != get_condition_values(res1):
            self.fail("Incorrect generator decompilation: %s -> %s" % (gen, res1))

        if get_condition_values(gen) != get_condition_values(res2):
            self.fail("Incorrect lambda decompilation: %s -> %s" % (gen, res2))

        if get_condition_values(gen) != get_condition_values(res3):
            self.fail("Incorrect multi-for generator decompilation: %s -> %s" % (gen, res3))

    return wrapped_test


class TestDecompiler(unittest.TestCase):
    def assertDecompilesTo(self, src, expected):
        # skip test due to ast.dump has no indent parameter
        if sys.version_info[:2] <= (3, 8):
            return

        code = compile(src, '<?>', 'eval').co_consts[0]
        dc = Decompiler(code)
        expected = textwrap.dedent(expected).strip()
        if sys.version_info[:2] >= (3, 13):
            # Python 3.13+ ast.dump omits empty sequence fields (e.g. ifs=[], keywords=[])
            expected = re.sub(r',?\n\s+\w+=\[\](?=[,)])', '', expected)
        self.maxDiff = None
        self.assertMultiLineEqual(expected, ast.dump(dc.ast, indent=2))

    def test_ast1(self):
        self.assertDecompilesTo(
            '(a for a in [] if x and y and z and j)',
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
            """)

    def test_ast2(self):
        self.assertDecompilesTo(
            'lambda x, y, z, j: (x and y and z and j)',
            """
            BoolOp(
              op=And(),
              values=[
                Name(id='x', ctx=Load()),
                Name(id='y', ctx=Load()),
                Name(id='z', ctx=Load()),
                Name(id='j', ctx=Load())])
            """)

    def test_ast3(self):
        self.assertDecompilesTo(
            '(m for m in [] if x and y and z and j for n in [] if x and y and z and j)',
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
            """)

    def test_ast_none(self):
        self.assertDecompilesTo(
            '(m for m in [] if (x is None or y))',
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

            """
            )


    def test_ast_not_none(self):
        self.assertDecompilesTo(
            '(m for m in [] if (x is not None or y))',
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

            """
            )

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
        self.assertDecompilesTo( expr, expected_result)

    def test_ast_elt_and(self):
        self.assertDecompilesTo(
            '(x and y for z in T)',
            """
            GeneratorExp(
              elt=BoolOp(
                op=And(),
                values=[
                  Name(id='x', ctx=Load()),
                  Name(id='y', ctx=Load())]),
              generators=[
                comprehension(
                  target=Name(id='z', ctx=Store()),
                  iter=Name(id='.0', ctx=Load()),
                  ifs=[],
                  is_async=0)])
            """)

    def test_ast_elt_or(self):
        self.assertDecompilesTo(
            '(x or y for z in T)',
            """
            GeneratorExp(
              elt=BoolOp(
                op=And(),
                values=[
                  UnaryOp(
                    op=Not(),
                    operand=Name(id='x', ctx=Load())),
                  Name(id='y', ctx=Load())]),
              generators=[
                comprehension(
                  target=Name(id='z', ctx=Store()),
                  iter=Name(id='.0', ctx=Load()),
                  ifs=[],
                  is_async=0)])
            """)

    def test_ast_elt_and_compare(self):
        self.assertDecompilesTo(
            '(len(a) == len(b) and a != b for x in T)',
            """
            GeneratorExp(
              elt=BoolOp(
                op=And(),
                values=[
                  Compare(
                    left=Call(
                      func=Name(id='len', ctx=Load()),
                      args=[
                        Name(id='a', ctx=Load())],
                      keywords=[]),
                    ops=[
                      Eq()],
                    comparators=[
                      Call(
                        func=Name(id='len', ctx=Load()),
                        args=[
                          Name(id='b', ctx=Load())],
                        keywords=[])]),
                  Compare(
                    left=Name(id='a', ctx=Load()),
                    ops=[
                      NotEq()],
                    comparators=[
                      Name(id='b', ctx=Load())])]),
              generators=[
                comprehension(
                  target=Name(id='x', ctx=Store()),
                  iter=Name(id='.0', ctx=Load()),
                  ifs=[],
                  is_async=0)])
            """)

    def test_ast_no_filter(self):
        self.assertDecompilesTo(
            '(x for x in T)',
            """
            GeneratorExp(
              elt=Name(id='x', ctx=Load()),
              generators=[
                comprehension(
                  target=Name(id='x', ctx=Store()),
                  iter=Name(id='.0', ctx=Load()),
                  ifs=[],
                  is_async=0)])
            """)

    def test_ast_multiline_not_none(self):
        expr = """(m
                for m in []
                if (x is not None or y))"""
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
                            IsNot()],
                          comparators=[
                            Constant(value=None)]),
                        Name(id='y', ctx=Load())])],
                  is_async=0)])
            """
        self.assertDecompilesTo(re.sub("\n", " ", expr), expected_result)
        self.assertDecompilesTo(expr, expected_result)

    def test_ast_multiline_multi_for(self):
        expr = """(m
                for m in []
                if x and y and z and j
                for n in []
                if x and y and z and j)"""
        expected_result = """
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
            """
        self.assertDecompilesTo(re.sub("\n", " ", expr), expected_result)
        self.assertDecompilesTo(expr, expected_result)

    # ── Python 3.13 fixes ──────────────────────────────────────────────────

    def test_ast_nested_gen_cellvar(self):
        # x is a cell var in the outer generator (referenced in the inner one);
        # Python 3.13 unified the LOAD_FAST index space to cover varnames + cellvars
        self.assertDecompilesTo(
            '(x*y for x in T for y in (z for z in x))',
            """
            GeneratorExp(
              elt=BinOp(
                left=Name(id='x', ctx=Load()),
                op=Mult(),
                right=Name(id='y', ctx=Load())),
              generators=[
                comprehension(
                  target=Name(id='x', ctx=Store()),
                  iter=Name(id='.0', ctx=Load()),
                  ifs=[],
                  is_async=0),
                comprehension(
                  target=Name(id='y', ctx=Store()),
                  iter=GeneratorExp(
                    elt=Name(id='z', ctx=Load()),
                    generators=[
                      comprehension(
                        target=Name(id='z', ctx=Store()),
                        iter=Name(id='x', ctx=Load()),
                        ifs=[],
                        is_async=0)]),
                  ifs=[],
                  is_async=0)])
            """)

    def test_ast_fused_load_fast(self):
        # Python 3.13 fuses adjacent loads into LOAD_FAST_LOAD_FAST;
        # the lambda body uses a single instruction to load both x and y
        self.assertDecompilesTo(
            'lambda x, y: x + y',
            """
            BinOp(
              left=Name(id='x', ctx=Load()),
              op=Add(),
              right=Name(id='y', ctx=Load()))
            """)

    def test_ast_lambda_with_default(self):
        # Python 3.13 replaced MAKE_FUNCTION flags with SET_FUNCTION_ATTRIBUTE;
        # flag=1 carries the default arg values tuple
        if sys.version_info[:2] < (3, 13):
            return
        self.assertDecompilesTo(
            '(a for a in T if (lambda x, y=10: x + y)(a))',
            """
            GeneratorExp(
              elt=Name(id='a', ctx=Load()),
              generators=[
                comprehension(
                  target=Name(id='a', ctx=Store()),
                  iter=Name(id='.0', ctx=Load()),
                  ifs=[
                    Call(
                      func=Lambda(
                        args=arguments(
                          args=[
                            arg(arg='x'),
                            arg(arg='y')],
                          defaults=[
                            Constant(value=10)]),
                        body=BinOp(
                          left=Name(id='x', ctx=Load()),
                          op=Add(),
                          right=Name(id='y', ctx=Load()))),
                      args=[
                        Name(id='a', ctx=Load())],
                      keywords=[])],
                  is_async=0)])
            """)

    def test_ast_fstring_simple(self):
        # Python 3.13 replaced FORMAT_VALUE with FORMAT_SIMPLE for plain f-string slots
        if sys.version_info[:2] < (3, 13):
            return
        self.assertDecompilesTo(
            '(f"{x}" for a in T)',
            """
            GeneratorExp(
              elt=FormattedValue(
                value=Name(id='x', ctx=Load()),
                conversion=-1),
              generators=[
                comprehension(
                  target=Name(id='a', ctx=Store()),
                  iter=Name(id='.0', ctx=Load()),
                  ifs=[],
                  is_async=0)])
            """)

    def test_ast_fstring_conversion_r(self):
        # Python 3.13 uses CONVERT_VALUE for !r/!s/!a; !r maps to conversion=114
        if sys.version_info[:2] < (3, 13):
            return
        self.assertDecompilesTo(
            '(f"{x!r}" for a in T)',
            """
            GeneratorExp(
              elt=FormattedValue(
                value=Name(id='x', ctx=Load()),
                conversion=114),
              generators=[
                comprehension(
                  target=Name(id='a', ctx=Store()),
                  iter=Name(id='.0', ctx=Load()),
                  ifs=[],
                  is_async=0)])
            """)

    def test_ast_fstring_conversion_a(self):
        # CONVERT_VALUE with !a (ascii); maps to conversion=97
        if sys.version_info[:2] < (3, 13):
            return
        self.assertDecompilesTo(
            '(f"{x!a}" for a in T)',
            """
            GeneratorExp(
              elt=FormattedValue(
                value=Name(id='x', ctx=Load()),
                conversion=97),
              generators=[
                comprehension(
                  target=Name(id='a', ctx=Store()),
                  iter=Name(id='.0', ctx=Load()),
                  ifs=[],
                  is_async=0)])
            """)

    def test_ast_fstring_with_spec(self):
        # Python 3.13 uses FORMAT_WITH_SPEC when a format spec is present
        if sys.version_info[:2] < (3, 13):
            return
        self.assertDecompilesTo(
            '(f"{x:.2f}" for a in T)',
            """
            GeneratorExp(
              elt=FormattedValue(
                value=Name(id='x', ctx=Load()),
                conversion=-1,
                format_spec=Constant(value='.2f')),
              generators=[
                comprehension(
                  target=Name(id='a', ctx=Store()),
                  iter=Name(id='.0', ctx=Load()),
                  ifs=[],
                  is_async=0)])
            """)

    def test_ast_call_of_call_result(self):
        # Python 3.11+ inserts PUSH_NULL after an expression result when it is
        # used as a callable, allowing foo()(1)-style chained calls
        self.assertDecompilesTo(
            '(foo()(1) for a in T)',
            """
            GeneratorExp(
              elt=Call(
                func=Call(
                  func=Name(id='foo', ctx=Load()),
                  args=[],
                  keywords=[]),
                args=[
                  Constant(value=1)],
                keywords=[]),
              generators=[
                comprehension(
                  target=Name(id='a', ctx=Store()),
                  iter=Name(id='.0', ctx=Load()),
                  ifs=[],
                  is_async=0)])
            """)

    def test_ast_starargs_null_sentinel(self):
        # LOAD_GLOBAL with push_null pushes a NULL sentinel below the callable;
        # CALL_FUNCTION_EX must clean it up after popping the callable
        if sys.version_info[:2] < (3, 11):
            return
        self.assertDecompilesTo(
            '(foo(*args) for a in T)',
            """
            GeneratorExp(
              elt=Call(
                func=Name(id='foo', ctx=Load()),
                args=[
                  Starred(
                    value=Name(id='args', ctx=Load()),
                    ctx=Load())],
                keywords=None),
              generators=[
                comprehension(
                  target=Name(id='a', ctx=Store()),
                  iter=Name(id='.0', ctx=Load()),
                  ifs=[],
                  is_async=0)])
            """)

    def test_ast_kwargs_in_element(self):
        # keyword arguments in a generator element
        self.assertDecompilesTo(
            '(foo(x=1) for a in T)',
            """
            GeneratorExp(
              elt=Call(
                func=Name(id='foo', ctx=Load()),
                args=[],
                keywords=[
                  keyword(
                    arg='x',
                    value=Constant(value=1))]),
              generators=[
                comprehension(
                  target=Name(id='a', ctx=Store()),
                  iter=Name(id='.0', ctx=Load()),
                  ifs=[],
                  is_async=0)])
            """)


for i, gen in enumerate(generate_gens()):
    test_method = create_test(gen)
    test_method.__name__ = 'test_decompiler_%d' % i
    setattr(TestDecompiler, test_method.__name__, test_method)
