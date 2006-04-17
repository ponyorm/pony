import re
from datetime import date, timedelta
from decimal import Decimal

from pony.main import *

re_group_number = re.compile(r'^\d{4}[A-Za-z]$')

class Group(Persistent):
    number   = PrimaryKey(str, check=lambda x: re_group_number.match(x))
    subjects = Set('Subject', max=10)
    students = List('Student', max=20)

    def __init__(self, number, **keyargs):
        Persistent.__init__(self, number=number, **keyargs)


class Student(Persistent):
    number      = PrimaryKey(int)
    group       = Required(Group)
    marks       = Set('Mark')
    first_name  = Required(string, check=lambda x: x == x.strip())
    middle_name = Optional(string, check=lambda x: x == x.strip())
    last_name   = Required(string, check=lambda x: x == x.strip())
    birth_date  = Required(date,   check=lambda x:
                                   x < date.today() - timedelta(365*16))
    hobbies  = Set(unicode)

    def _get_full_name(self):
        if not self.middle_name:
              list = [self.first_name, self.middle_name, self.last_name]
        else: list = [self.first_name, self.last_name]
        return ' '.join(list)
    full_name = property(_get_full_name)
        
class Subject(Persistent):
    name   = PrimaryKey(unicode)
    groups = Set(Group)
    marks  = Set('Mark')

    def __init__(self, name, **keyargs):
        Persistent.__init__(self, name=name, **keyargs)

class Mark(Persistent):
    subject = Required(Subject)
    student = Required(Student)
    date    = Required(date)
    value   = Required(Decimal, sqlprec=3, scale=2)
    _primary_key_ = (subject, student)
