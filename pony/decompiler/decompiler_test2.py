from decompiler2 import *
import compiler

class TestDecompiler2:

    def verify(self, expr):
        expected = str(compiler.parse(expr))
	result = self.add_framing(decompile_to_aststring(eval(expr)))
	assert result == expected

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

    def test_10(self):
        self.verify('(a for b in Student if f == 5 and r > 5 or not t)')

    def test_11(self):
        self.verify('(a for b in Student if -t and ~r or `f`)')

#    def test_12(self):						 # None is parsed as Name('None')
#        self.verify('(a for b in Student if +t and r is None)') # bytecode is Const(None)

    def test_13(self):
        self.verify('(a for b in Student if f and (r and t) )')


#    def test_12(self):
#        self.verify('(a for b in Student if c > d > e)')


