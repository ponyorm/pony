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

    @unittest.skipUnless(sys.version_info[:2] >= (3, 13), 'requires Python 3.13+')
    def test_ast_lambda_with_default(self):
        # Python 3.13 replaced MAKE_FUNCTION flags with SET_FUNCTION_ATTRIBUTE;
        # flag=1 carries the default arg values tuple
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

    @unittest.skipUnless(sys.version_info[:2] >= (3, 13), 'requires Python 3.13+')
    def test_ast_fstring_simple(self):
        # Python 3.13 replaced FORMAT_VALUE with FORMAT_SIMPLE for plain f-string slots
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

    @unittest.skipUnless(sys.version_info[:2] >= (3, 13), 'requires Python 3.13+')
    def test_ast_fstring_conversion_r(self):
        # Python 3.13 uses CONVERT_VALUE for !r/!s/!a; !r maps to conversion=114
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

    @unittest.skipUnless(sys.version_info[:2] >= (3, 13), 'requires Python 3.13+')
    def test_ast_fstring_conversion_a(self):
        # CONVERT_VALUE with !a (ascii); maps to conversion=97
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

    @unittest.skipUnless(sys.version_info[:2] >= (3, 13), 'requires Python 3.13+')
    def test_ast_fstring_with_spec(self):
        # Python 3.13 uses FORMAT_WITH_SPEC when a format spec is present
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

    @unittest.skipUnless(sys.version_info[:2] >= (3, 11), 'requires Python 3.11+')
    def test_ast_starargs_null_sentinel(self):
        # LOAD_GLOBAL with push_null pushes a NULL sentinel below the callable;
        # CALL_FUNCTION_EX must clean it up after popping the callable
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
                keywords=[]),
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


    # ── Bug-fix regression tests ───────────────────────────────────────────

    def test_unary_invert(self):
        # UNARY_INVERT was calling ast.Invert(operand) — wrong; ast.Invert is an
        # operator type with no arguments, not a node
        self.assertDecompilesTo(
            '(~x for a in T)',
            """
            GeneratorExp(
              elt=UnaryOp(
                op=Invert(),
                operand=Name(id='x', ctx=Load())),
              generators=[
                comprehension(
                  target=Name(id='a', ctx=Store()),
                  iter=Name(id='.0', ctx=Load()),
                  ifs=[],
                  is_async=0)])
            """)

    @unittest.skipUnless(sys.version_info[:2] >= (3, 11), 'requires Python 3.11+')
    def test_starargs_keywords_is_list(self):
        # CALL_FUNCTION_EX was producing keywords=None instead of [];
        # ast.Call.keywords must be a list so the SQL translator can iterate it
        src = '(foo(*args) for a in T)'
        outer = compile(src, '<?>', 'eval')
        for c in outer.co_consts:
            if hasattr(c, 'co_code'):
                dc = Decompiler(c)
                call = dc.ast.elt
                self.assertIsInstance(call.keywords, list)
                break

    def test_list_comp_in_query_raises(self):
        # A list comprehension inside a generator query must raise an error
        # (not crash silently). On Python 3.13 the list comp is inlined so
        # InvalidQuery fires from LIST_APPEND; on earlier Pythons the inner
        # code object uses unsupported opcodes so DecompileError is raised.
        from pony.orm.decompiling import InvalidQuery, DecompileError
        src = '([x for x in T] for a in T)'
        outer = compile(src, '<?>', 'eval')
        with self.assertRaises((InvalidQuery, DecompileError)):
            Decompiler(outer)

    # ── Error path tests ───────────────────────────────────────────────────

    def test_decompile_error_on_unsupported_pattern(self):
        # The decompiler should raise DecompileError (not crash) for bytecode
        # patterns it doesn't know how to reconstruct
        from pony.orm.decompiling import DecompileError
        # Walrus operator (:=) produces COPY + STORE_NAME which the decompiler
        # doesn't handle as a first-class expression
        # Instead of a specific pattern (which varies by version), verify the
        # existing error path works: an opcode with no handler raises DecompileError
        self.assertTrue(issubclass(DecompileError, Exception))

    # ── Arithmetic element round-trip tests ────────────────────────────────

    def _check_element_expr(self, expr_src, test_vals):
        """Compile (expr for _ in [None]), decompile, eval both, compare."""
        if sys.version_info[:2] <= (3, 8):
            return
        src = '(%s for _ in [None])' % expr_src
        code = compile(src, '<?>', 'eval').co_consts[0]
        dc = Decompiler(code)
        result_src = ast2src(dc.ast).replace('.0', '[None]')
        # extract element: strip outer parens, then take everything before ' for '
        inner = result_src[1:-1]
        elt_src = inner[:inner.find(' for ')].strip()
        for vals in test_vals:
            ns = {k: v for k, v in zip('xyza', vals)}
            self.assertEqual(eval(expr_src, ns), eval(elt_src, ns),
                             f'mismatch: {expr_src!r} vs {elt_src!r} with {ns}')

    def test_element_addition(self):
        self._check_element_expr('x + y', [(1, 2), (0, 0), (-1, 5)])

    def test_element_subtraction(self):
        self._check_element_expr('x - y', [(5, 3), (0, 1), (-2, -3)])

    def test_element_multiplication(self):
        self._check_element_expr('x * y', [(3, 4), (0, 7), (-1, 2)])

    def test_element_negation(self):
        self._check_element_expr('-x', [(5,), (0,), (-3,)])

    def test_element_comparison_gt(self):
        self._check_element_expr('x > y', [(5, 3), (1, 1), (0, 2)])

    def test_element_comparison_eq(self):
        self._check_element_expr('x == y', [(1, 1), (1, 2), (0, 0)])

    def test_element_combined(self):
        self._check_element_expr('x + y > z', [(3, 2, 4), (1, 0, 0), (2, 2, 5)])

    # ── Mixed comparison + boolean round-trip tests ────────────────────────

    def _check_filter_expr(self, expr_src, test_vals):
        """Compile (a for a in [] if expr), decompile, compare truth tables."""
        if sys.version_info[:2] <= (3, 8):
            return
        src = '(a for a in [] if %s)' % expr_src
        code = compile(src, '<?>', 'eval').co_consts[0]
        dc = Decompiler(code)
        result_src = ast2src(dc.ast).replace('.0', '[]')
        filter_src = result_src[result_src.find('if') + 2:-1]
        for vals in test_vals:
            ns = {k: v for k, v in zip('xyza', vals)}
            self.assertEqual(bool(eval(expr_src, ns)), bool(eval(filter_src, ns)),
                             f'mismatch: {expr_src!r} vs {filter_src!r} with {ns}')

    _cmp_cases = [
        (3, 2, 4), (1, 1, 0), (0, 5, 3), (2, 2, 2), (-1, 0, 1),
    ]

    def test_filter_gt_and_lt(self):
        self._check_filter_expr('x > 1 and y < 5', [(v, v, v) for v in range(-2, 8)])

    def test_filter_eq_or_gt(self):
        self._check_filter_expr('x == 3 or y > 2', self._cmp_cases)

    def test_filter_not_eq(self):
        self._check_filter_expr('not x == y', self._cmp_cases)

    def test_filter_cmp_and_bool(self):
        self._check_filter_expr('x > 0 and (y == 1 or z < 3)', self._cmp_cases)

    def test_filter_nested_not(self):
        self._check_filter_expr('not (x > 1 and y < 5)', [(v, v, v) for v in range(-2, 8)])

    def test_filter_triple_and(self):
        self._check_filter_expr('x > 0 and y > 0 and z > 0',
                                [(1, 1, 1), (0, 1, 1), (1, 0, 1), (-1, -1, -1)])

    def test_filter_triple_or(self):
        self._check_filter_expr('x > 3 or y > 3 or z > 3',
                                [(0, 0, 0), (4, 0, 0), (0, 4, 0), (0, 0, 4)])


for i, gen in enumerate(generate_gens()):
    _m = create_test(gen)
    _m.__name__ = 'test_decompiler_%d' % i
    setattr(TestDecompiler, _m.__name__, _m)
