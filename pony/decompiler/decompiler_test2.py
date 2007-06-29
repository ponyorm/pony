from decompiler import *

class TestDecompiler2:

    def setup_method(self, method):
        self.d = Decompiler()

    def test_1(self):
        g =        (a for b in Student)
	expected = "GenExprInner(Name('a'), GenExprFor(AssName('b', 'OP_ASSIGN'), Name('.0'), []))"
	result = decompile_to_aststring(g)
	assert result == expected

    def test_2(self):
        g =        (a for b, c in Student)
	expected = "GenExprInner(Name('a'), GenExprFor(AssTuple([AssName('b', 'OP_ASSIGN'), AssName('c', 'OP_ASSIGN')]), Name('.0'), []))"
	result = decompile_to_aststring(g)
	assert result == expected
        

    
    