from pony.decompiler import *
import compiler

# test entities

class Meta(type):
      def __iter__(self):
           return iter([])

class Entity(object):
    __metaclass__ = Meta

class Student(Entity):
      pass


class TestDecompiler:

    def verify(self, expr):
        expected = str(compiler.parse(expr))
        result = self.add_framing(str(decompile_to_ast(eval(expr))))
        if result != expected:
            print 'result = ' + result
            print 'expected = ' + expected
            raise Exception('Test failed')

    def add_framing(self, result):
        result = str(result)
        result = result.replace("Name('.0')", "Name('Student')")
        return "Module(None, Stmt([Discard(GenExpr(" + result + "))]))"

    def test_1(self):
        self.verify('(a for b in Student)')

    def test_2(self):
        self.verify('(a for b, c in Student)')

    def test_3(self):
        self.verify('(a for b in Student for c in [])')

    def test_4(self):
        self.verify('(a for b in Student for c in [] for d in [])')

    def test_5(self):
        self.verify('(a for b in Student if f)')

#    def test_6(self):                                      # bytecode is the same as for (a for b in Student if f and h)
#        self.verify('(a for b in Student if f if h)')      # parse is not

    def test_7(self):
        self.verify('(a for b in Student if f and h)')

    def test_8(self):
        self.verify('(a for b in Student if f and h or t)')

    def test_9(self):
        self.verify('(a for b in Student if f == 5 and r or t)')


    def test_13(self):
        self.verify('(a for b in Student if f and (r and t) )')


#    def test_12(self):
#        self.verify('(a for b in Student if c > d > e)')
########################### UNARY_ operations
    def test_u1(self):
        self.verify('(a for b in Student if f == 5 and +r or not t)')

    def test_u2(self):
        self.verify('(a for b in Student if -t and ~r or `f`)')

#    def test_u3(self):						 # None is parsed as Name('None')
#        self.verify('(a for b in Student if +t and r is None)') # bytecode is Const(None)

########################### BINARY_ operations ###############################
    def test_b1(self):
        self.verify('(a**2 for b in Student if t * r > y / 3)')

    def test_b2(self):
        self.verify('(a + 2 for b in Student if t + r > y // 3)')

    def test_b3(self):
        self.verify('(a[2,v] for b in Student if t - r > y[3])')

    def test_b4(self):
        self.verify('((a + 2) * 3 for b in Student if t[r, e] > y[3, r * 4, t])')

    def test_b5(self):
        self.verify('(a<<2 for b in Student if t>>e > r & (y & u))')

    def test_b6(self):
        self.verify('(a|b for c in Student if t^e > r | (y & (u & (w % z))))')     # no optimization yet
########################### List, Tuple, Dict ###############################
    def test_l1(self):
        self.verify('([a, b, c] for d in Student )')

    def test_l2(self):
        self.verify('([a, b, 4] for d in Student if a[4, b] > b[1,v,3])')

    def test_l3(self):
        self.verify('((a, b, c) for d in Student )')

    def test_l4(self):
        self.verify('({} for d in Student )')

    def test_l5(self):
        self.verify("({'a' : x, 'b' : y} for a, b in Student)")

    def test_l6(self):
        self.verify("(({'a' : x, 'b' : y}, {'c' : x1, 'd' : 1}) for a, b, c, d in Student)")

    def test_l7(self):
        self.verify("([{'a' : x, 'b' : y}, {'c' : x1, 'd' : 1}] for a, b, c, d in Student)")

########################### SLICE ############################################
    def test_s1(self):
        self.verify('(a[1:2] for b in Student)')

    def test_s2(self):
        self.verify('(a[:2] for b in Student)')

    def test_s3(self):
        self.verify('(a[2:] for b in Student)')

    def test_s4(self):
        self.verify('(a[:] for b in Student)')

    def test_s5(self):
        self.verify('(a[1:2:3] for b in Student)')

    def test_s6(self):
        self.verify('(a[1:2, 3:4] for b in Student)')

    def test_s7(self):
        self.verify('(a[2:4:6,6:8] for a, y in Student)')

########################### ATTR ################################################
    def test_a1(self):
        self.verify('(a.b.c for d.e.f.g in Student)')

    def test_a2(self):
        self.verify('(a.b.c for d[g] in Student)')
########################### IFS ##################################################
    def test_i1(self):
        self.verify('( (s,d,w) for t in Student if (((4 != x.a) or (a * 3 > 20)) and (a * 2 < 5) ))')

    def test_i2(self):
        self.verify('( [s,d,w] for t in Student if ((4 != x.amount or (amount * 3 > 20 or amount * 2 < 5)) and (amount * 8 == 20)))')

    def test_i3(self):
        self.verify('( [s,d,w] for t in Student if ((4 != x.a or (a * 3 > 20 or (a * 2 < 5 or (4 == 5)))) and (a * 8 == 20)))')

    def test_i4(self):
        self.verify("(s for s in Student if s.age > 20 and (s.group.number == 4142 or 'Math' in s.marks.subject.name))")

    def test_i5(self):
        self.verify("(a for b in Student if c > d for e in Student if f < g)")
########################### FUNC #################################################
    def test_f1(self):
        self.verify('(func1(a, a.attr, keyarg=123) for s in Student)')

    def test_f2(self):
        self.verify('(func1(a, a.attr, keyarg=123, *e) for s in Student)')

    def test_f3(self):
        self.verify("(func1(a, b, a.attr1, a.b.c, keyarg1=123, keyarg2='mx', *e, **f) for s in Student)")

    def test_f4(self):
        self.verify("(func(a, a.attr, keyarg=123) for a in Student if a.method(x, *y, **z) == 4)")

    #g = (a(lambda x,y: x > 0) for a in [])
    #g = (a(b, lambda x,y: x > 0) for a in [])
    #g = (a(b, lambda x,y: x > 0) for a,b,x,y in [])

    #g = (a for b in Student if c > d > e)
    #g = (a for b in Student if c > d > d2)

def main():
    t = TestDecompiler()
    for attr in dir(t):
        if attr.startswith('test_'):
            getattr(t, attr)()
            print attr + ' done.'

if __name__ == '__main__':
      main()
