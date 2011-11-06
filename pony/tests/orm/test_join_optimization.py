import unittest
from datetime import date
from pony.orm import *
from testutils import *

db = Database('sqlite', ':memory:')

class Group(db.Entity):
    number = PrimaryKey(int)
    major = Required(unicode)
    students = Set("Student")

class Course(db.Entity):
    name = Required(unicode)
    semester = Required(int)
    credits = Required(int)
    students = Set("Student")
    PrimaryKey(name, semester)
    
class Student(db.Entity):
    id = PrimaryKey(int, auto=True)
    name = Required(unicode)
    dob = Required(date)
    picture = Optional(buffer)
    gpa = Required(float, default=0)
    group = Required(Group)
    courses = Set(Course)


db.generate_mapping(create_tables=True)

def flatten(x):
    result = []
    for el in x:
        if hasattr(el, "__iter__") and not isinstance(el, basestring):
            result.extend(flatten(el))
        else:
            result.append(el)
    return result

class TestM2MOptimization(unittest.TestCase):
    def setUp(self):
        rollback()
    def test1(self):
        q = select(s for s in Student if len(s.courses) > 2)
        self.assertEquals(Course._table_ not in flatten(q._translator.conditions), True)
    def test2(self):
        q = select(s for s in Student if max(s.courses.semester) > 2)
        self.assertEquals(Course._table_ not in flatten(q._translator.conditions), True)
    def test3(self):
        q = select(s for s in Student if max(s.courses.credits) > 2)
        self.assertEquals(Course._table_ in flatten(q._translator.conditions), True)
        self.assertEquals(Course.students.table in flatten(q._translator.conditions), True)
    def test4(self):
        q = select(g for g in Group if sum(g.students.gpa) > 5)
        self.assertEquals(Group._table_ not in flatten(q._translator.conditions), True)
    def test5(self):
        q = select(s for s in Student if s.group.number == 1 or s.group.major == '1')
        self.assertEquals(Group._table_ in flatten(q._translator.from_), True)
    def test6(self):
        q = select(s for s in Student if s.group == Group[101])
        #select(s for s in Student if Course('1', 1) in s.courses).all()
        self.assertEquals(Group._table_ not in flatten(q._translator.from_), True)



if __name__ == '__main__':
    unittest.main()