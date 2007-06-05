from decompiler import *

class TestDecompiler:

    def setup_method(self, method):
        self.d = Decompiler()

    def test_1(self):
        g =        (s for s in Student)
	expected = "s for s in .0"
	result = decompile_tostring(g)
	assert result == expected

    def test_2(self):
        g =   ((s, d, w) for t in Student   if ((4 != x.amount or amount * 3 > 20 or amount * 2 < 5) and amount * 8 == 20))
	expected = "(s, d, w) for t in .0 if ((((4 != x.amount) or ((amount * 3 > 20) or (amount * 2 < 5)))) and (amount * 8 == 20))"
	result = decompile_tostring(g)
	assert result == expected

    def test_3(self):
        g =   (s for s in Student if   s.age > 20 and   (s.group.number == 4142  or 'Math' in s.marks.subject.name))
	expected = "s for s in .0 if ((s.age > 20) and ((s.group.number == 4142) or ('Math' in s.marks.subject.name)))"
	result = decompile_tostring(g)
	assert result == expected

    def test_4(self):
        g = ((s, d, w) for t in Student   if ((4 != x.a) or (a * 3 > 20) or (a * 2 < 5) and (a * 8 == 20)))
	expected = "(s, d, w) for t in .0 if ((4 != x.a) or ((a * 3 > 20) or ((a * 2 < 5) and (a * 8 == 20))))"
	result = decompile_tostring(g)
	assert result == expected

    def test_5(self):
        g = ((s, d, w) for t in Student   if ( 4 != x.a  or  a * 3 > 20 ) and ( a * 2 < 5  or  a * 8 == 20 ))
	expected = "(s, d, w) for t in .0 if ((((4 != x.a) or (a * 3 > 20))) and ((a * 2 < 5) or (a * 8 == 20)))"
	result = decompile_tostring(g)
	assert result == expected

    def test_6(self):
        g =   (s for t in Student if a == 5)
	expected = "s for t in .0 if a == 5"
	result = decompile_tostring(g)
	assert result == expected

    def test_7(self):
        g =   ((s, t, w) for t in Student if ((4 != x.a.b or a * 3 > 20 or a * 2 < 5 and v == 6) and a * 8 == 20 or (f > 4) ))
	expected = "(s, t, w) for t in .0 if ((((((4 != x.a.b) or ((a * 3 > 20) or ((a * 2 < 5) and (v == 6))))) and (a * 8 == 20))) or (f > 4))"
	result = decompile_tostring(g)
	assert result == expected


