import unittest
from decimal import Decimal
from pony.orm import *
from testutils import *

db = Database('sqlite', ':memory:')

class Student(db.Entity):
    name = Required(unicode)
    scholarship = Optional(int)
    gpa = Optional(Decimal,3,1)
    group = Required('Group')
    
class Group(db.Entity):
    number = PrimaryKey(int)
    students = Set(Student)
    
db.generate_mapping(create_tables=True)        

g1 = Group(number=1)
Student(id=1, name='S1', group=g1, gpa=3.1)
Student(id=2, name='S2', group=g1, gpa=3.2, scholarship=100)
Student(id=3, name='S3', group=g1, gpa=3.3, scholarship=200)
commit()

class TestQuery(unittest.TestCase):
    def setUp(self):
        rollback()
    @raises_exception(NotImplementedError, "Query iterator has unexpected type 'setiterator'")
    def test_exception1(self):
        g = Group[1]
        select(s for s in g.students).all()
    @raises_exception(NameError, 'a')
    def test_exception2(self):
        select(a for s in Student).all()
    @raises_exception(TypeError,"Variable 'x' has unexpected type 'list'")
    def test_exception3(self):
        x = ['A']
        select(s for s in Student if s.name == x).all()
    @raises_exception(TypeError,"Function 'f1' cannot be used inside query")
    def test_exception4(self):
        def f1(x):
            return x + 1
        select(s for s in Student if f1(s.gpa) > 3).all()
    @raises_exception(TypeError,"Method 'method1' cannot be used inside query")
    def test_exception5(self):
        class C1(object):
            def method1(self, a, b):
                return a + b
        c = C1()
        m1 = c.method1
        select(s for s in Student if m1(s.gpa, 1) > 3).all() 
    @raises_exception(TypeError, "Variable 'x' has unexpected type 'complex'")  
    def test_exception6(self):
        x = 1j
        select(s for s in Student if s.gpa == x).all()
    def test1(self):
        select(g for g in Group for s in db.Student).all()
        self.assert_(True)
    def test2(self):
        avg_gpa = select.avg(s.gpa for s in Student)
        self.assertEquals(avg_gpa, Decimal('3.2'))
    def test21(self):
        avg_gpa = select.avg(s.gpa for s in Student if s.id < 0)
        self.assertEquals(avg_gpa, None)
    def test3(self):
        sum_ss = select.sum(s.scholarship for s in Student)
        self.assertEquals(sum_ss, 300)
    def test31(self):
        sum_ss = select.sum(s.scholarship for s in Student if s.id < 0)
        self.assertEquals(sum_ss, 0)
    @raises_exception(TranslationError, "'avg' is valid for numeric attributes only")
    def test4(self):
        select.avg(s.name for s in Student)
    def wrapper(self):
        return select.count(s for s in Student if s.scholarship > 0)
    def test5(self):
        c = self.wrapper()
        c = self.wrapper()
        self.assertEquals(c, 2)
    def test6(self):
        c = select.count(s.scholarship for s in Student if s.scholarship > 0)
        self.assertEquals(c, 2)
    def test7(self):
        s = select(s.scholarship for s in Student if s.id == 3).get()
        self.assertEquals(s, 200)
    def test8(self):
        s = select(s.scholarship for s in Student if s.id == 4).get()
        self.assertEquals(s, None)
    def test9(self):
        s = select(s for s in Student if s.id == 4).exists()
        self.assertEquals(s, False)
    def test10(self):
        r = select.min(s.scholarship for s in Student)
        self.assertEquals(r, 100)
    def test11(self):
        r = select.min(s.scholarship for s in Student if s.id < 2)
        self.assertEquals(r, None)
    def test12(self):
        r = select.max(s.scholarship for s in Student)
        self.assertEquals(r, 200)
    
if __name__ == '__main__':
    unittest.main()    
