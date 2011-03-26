import unittest
from pony.orm import *
from testutils import *
from datetime import date, datetime
from decimal import Decimal

class TestFuncMonad(unittest.TestCase):
    def setUp(self):
        _diagram_ = Diagram()
        self.diagram = _diagram_        
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
        self.Student = Student
        self.Group = Group
        self.db = Database('sqlite', ':memory:')
        con = self.db.get_connection()
        con.executescript("""
        drop table if exists Student;
        create table Student(
            id integer primary key,
            name varchar(20),
            dob date,
            last_visit datetime,
            scholarship decimal,
            phd boolean,
            [group] integer
        );
        drop table if exists [Group];
        create table [Group](
            number integer primary key
        );
        insert into Student values (1, "AA", '1981-01-01', '2011-01-01 11:11:11.000', 0    , 1, 1);
        insert into Student values (2, "BB", '1982-02-02', '2011-02-02 12:12:12.000', 202.2, 1, 1);
        insert into Student values (3, "CC", '1983-03-03', '2011-03-03 13:13:13.000', 303.3, 0, 1);
        insert into Student values (4, "DD", '1984-04-04', '2011-04-04 14:14:14.000', 404.4, 0, 2);
        insert into Student values (5, "EE", '1985-05-05', '2011-05-05 15:15:15.000', 505.5, 0, 2);
        insert into [Group] values (1);
        insert into [Group] values (2);
        """)
        generate_mapping(self.db,  check_tables=True)
    def test_minmax1(self):
        Student = self.Student
        result = set(select(s for s in Student if max(s.id, 3) == 3 ))
        self.assertEquals(result, set([Student(1), Student(2), Student(3)]))
    def test_minmax2(self):
        Student = self.Student
        result = set(select(s for s in Student if min(s.id, 3) == 3 ))
        self.assertEquals(result, set([Student(4), Student(5), Student(3)]))
    def test_minmax3(self):
        Student = self.Student
        result = set(select(s for s in Student if max(s.name, "CC") == "CC" ))
        self.assertEquals(result, set([Student(1), Student(2), Student(3)]))
    def test_minmax4(self):
        Student = self.Student
        result = set(select(s for s in Student if min(s.name, "CC") == "CC" ))
        self.assertEquals(result, set([Student(4), Student(5), Student(3)]))
##    @raises_exception(TypeError)
##    def test_minmax5(self):
##        Student = self.Student
##        Group = self.Group
##        result = set(select(s for s in Student if min(s.phd, 2) == 2 ))
    def test_date_func1(self):
        Student = self.Student
        result = set(select(s for s in Student if s.dob >= date(1983, 3, 3)))
        self.assertEquals(result, set([Student(3), Student(4), Student(5)]))
    @raises_exception(TypeError, "'month' argument of date(year, month, day) function must be int")
    def test_date_func2(self):
        Student = self.Student
        result = set(select(s for s in Student if s.dob >= date(1983, 'three', 3)))        
    @raises_exception(NotImplementedError)
    def test_date_func3(self):
        Student = self.Student
        d = 3
        result = set(select(s for s in Student if s.dob >= date(1983, d, 3)))

    def test_datetime_func1(self):
        Student = self.Student
        result = set(select(s for s in Student if s.last_visit >= date(2011, 3, 3)))
        self.assertEquals(result, set([Student(3), Student(4), Student(5)]))
    def test_datetime_func2(self):
        Student = self.Student
        result = set(select(s for s in Student if s.last_visit >= datetime(2011, 3, 3)))
        self.assertEquals(result, set([Student(3), Student(4), Student(5)]))        
    def test_datetime_func3(self):
        Student = self.Student
        result = set(select(s for s in Student if s.last_visit >= datetime(2011, 3, 3, 13, 13, 13)))
        self.assertEquals(result, set([Student(3), Student(4), Student(5)]))        
##    @raises_exception(TypeError, "'month' argument of date(year, month, day) function must be int")
##    def test_datetime_func4(self):
##        Student = self.Student
##        result = set(select(s for s in Student if s.last_visit >= date(1983, 'three', 3)))        
    @raises_exception(NotImplementedError)
    def test_datetime_func5(self):
        Student = self.Student
        d = 3
        result = set(select(s for s in Student if s.last_visit >= date(1983, d, 3)))

    def test_decimal_func(self):
        Student = self.Student
        result = set(select(s for s in Student if s.scholarship >= Decimal("303.3")))
        self.assertEquals(result, set([Student(3), Student(4), Student(5)]))

##    def test_bool(self):
##        Student = self.Student
##        result = set(select(s for s in Student if s.phd == True))
##        self.assertEquals(result, set([Student(3), Student(4), Student(5)]))
        
if __name__ == '__main__':
    unittest.main()
        