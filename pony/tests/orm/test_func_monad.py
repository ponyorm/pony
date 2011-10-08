import unittest
from pony.orm import *
from testutils import *
from datetime import date, datetime
from decimal import Decimal

class Student(Entity):
    id = PrimaryKey(int)
    name = Required(unicode)
    dob = Required(date)
    last_visit = Required(datetime)
    scholarship = Required(Decimal, 6, 2)
    phd = Required(bool)
    group = Required('Group')

class Group(Entity):
    number = PrimaryKey(int)
    students = Set(Student)

db = Database('sqlite', ':memory:')
db.generate_mapping(create_tables=True)

@with_transaction
def populate_db():
    g1 = Group.create(number=1)
    g2 = Group.create(number=2)

    Student.create(id=1, name="AA", dob=date(1981, 01, 01), last_visit=datetime(2011, 01, 01, 11, 11, 11),
                   scholarship=Decimal("0"), phd=True, group=g1)

    Student.create(id=2, name="BB", dob=date(1982, 02, 02), last_visit=datetime(2011, 02, 02, 12, 12, 12),
                   scholarship=Decimal("202.2"), phd=True, group=g1)

    Student.create(id=3, name="CC", dob=date(1983, 03, 03), last_visit=datetime(2011, 03, 03, 13, 13, 13),
                   scholarship=Decimal("303.3"), phd=False, group=g1)

    Student.create(id=4, name="DD", dob=date(1984, 04, 04), last_visit=datetime(2011, 04, 04, 14, 14, 14),
                   scholarship=Decimal("404.4"), phd=False, group=g2)

    Student.create(id=5, name="EE", dob=date(1985, 05, 05), last_visit=datetime(2011, 05, 05, 15, 15, 15),
                   scholarship=Decimal("505.5"), phd=False, group=g2)

populate_db()


class TestFuncMonad(unittest.TestCase):
    def setUp(self):
        rollback()
    def tearDown(self):
        rollback()
    def test_minmax1(self):
        result = set(select(s for s in Student if max(s.id, 3) == 3 ))
        self.assertEquals(result, set([Student[1], Student[2], Student[3]]))
    def test_minmax2(self):
        result = set(select(s for s in Student if min(s.id, 3) == 3 ))
        self.assertEquals(result, set([Student[4], Student[5], Student[3]]))
    def test_minmax3(self):
        result = set(select(s for s in Student if max(s.name, "CC") == "CC" ))
        self.assertEquals(result, set([Student[1], Student[2], Student[3]]))
    def test_minmax4(self):
        result = set(select(s for s in Student if min(s.name, "CC") == "CC" ))
        self.assertEquals(result, set([Student[4], Student[5], Student[3]]))
    @raises_exception(TypeError)
    def test_minmax5(self):
        x = chr(128)
        result = set(select(s for s in Student if min(s.name, x) == "CC" ))
    @raises_exception(TypeError)
    def test_minmax6(self):
        x = chr(128)
        result = set(select(s for s in Student if min(s.name, x, "CC") == "CC" ))        
##    @raises_exception(TypeError)
##    def test_minmax5(self):
##        result = set(select(s for s in Student if min(s.phd, 2) == 2 ))
    def test_date_func1(self):
        result = set(select(s for s in Student if s.dob >= date(1983, 3, 3)))
        self.assertEquals(result, set([Student[3], Student[4], Student[5]]))
    @raises_exception(TypeError, "'month' argument of date(year, month, day) function must be int")
    def test_date_func2(self):
        result = set(select(s for s in Student if s.dob >= date(1983, 'three', 3)))        
    @raises_exception(NotImplementedError)
    def test_date_func3(self):
        d = 3
        result = set(select(s for s in Student if s.dob >= date(1983, d, 3)))
    def test_datetime_func1(self):
        result = set(select(s for s in Student if s.last_visit >= date(2011, 3, 3)))
        self.assertEquals(result, set([Student[3], Student[4], Student[5]]))
    def test_datetime_func2(self):
        result = set(select(s for s in Student if s.last_visit >= datetime(2011, 3, 3)))
        self.assertEquals(result, set([Student[3], Student[4], Student[5]]))        
    def test_datetime_func3(self):
        result = set(select(s for s in Student if s.last_visit >= datetime(2011, 3, 3, 13, 13, 13)))
        self.assertEquals(result, set([Student[3], Student[4], Student[5]]))        
##    @raises_exception(TypeError, "'month' argument of date(year, month, day) function must be int")
##    def test_datetime_func4(self):
##        Student = self.Student
##        result = set(select(s for s in Student if s.last_visit >= date(1983, 'three', 3)))        
    @raises_exception(NotImplementedError)
    def test_datetime_func5(self):
        d = 3
        result = set(select(s for s in Student if s.last_visit >= date(1983, d, 3)))
    def test_decimal_func(self):
        result = set(select(s for s in Student if s.scholarship >= Decimal("303.3")))
        self.assertEquals(result, set([Student[3], Student[4], Student[5]]))
##    def test_bool(self):
##        Student = self.Student
##        result = set(select(s for s in Student if s.phd == True))
##        self.assertEquals(result, set([Student[3], Student[4], Student[5]]))
        
if __name__ == '__main__':
    unittest.main()
