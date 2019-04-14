import unittest

from pony.thirdparty.compiler.transformer import parse
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
    pass


for i, gen in enumerate(generate_gens()):
    test_method = create_test(gen)
    test_method.__name__ = 'test_decompiler_%d' % i
    setattr(TestDecompiler, test_method.__name__, test_method)
