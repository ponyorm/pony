import unittest
from pony.orm3 import *
from pony.db import Database

sql =  """
drop table if exists "Groups";
drop table if exists Students;
drop table if exists Subjects;
drop table if exists Exams;
drop table if exists Group_Subject;

create table Groups(
  number varchar(6) primary key,
  department integer not null
);

insert into Groups values ('4145', 44);
insert into Groups values ('4146', 44);
insert into Groups values ('3132', 33);

create table Students(
  record integer primary key,
  fio varchar(50) not null,
  group_number varchar(6) not null references Groups(number),
  scholarship integer not null default 0
);

insert into Students values(101, 'Bob', '4145', 0);
insert into Students values(102, 'Joe', '4145', 800);
insert into Students values(103, 'Alex', '4145', 0);
insert into Students values(104, 'Brad', '3132', 500);
insert into Students values(105, 'John', '3132', 1000);

create table Subjects(
  name varchar(50) primary key
);

insert into Subjects values('Physics');
insert into Subjects values('Chemistry');
insert into Subjects values('Math');

create table Group_Subject(
  group_number varchar(6) not null references Groups(number),
  subject_name varchar(50) not null references Subjects(name),
  primary key (group_number, subject_name)
);

insert into Group_Subject values('4145', 'Physics');
insert into Group_Subject values('4145', 'Chemistry');
insert into Group_Subject values('4145', 'Math');
insert into Group_Subject values('3132', 'Physics');
insert into Group_Subject values('3132', 'Math');

create table Exams(
  student integer not null references Students(number),
  subject varchar(50) not null references Subjects(name),
  value integer not null,
  primary key (student, subject)
);

insert into Exams values (101, 'Physics', 4);
insert into Exams values (101, 'Math', 3);
insert into Exams values (102, 'Chemistry', 5);
insert into Exams values (103, 'Physics', 2);
insert into Exams values (103, 'Chemistry', 4);
"""

class TestTransaction(unittest.TestCase):
    def setUp(self):
        _diagram_ = Diagram()
        
        class Student(Entity):
            _table_ = "Students"
            record = PrimaryKey(int)
            name = Required(unicode, column="fio")
            group = Required("Group")
            scholarship = Required(int, default=0)
            marks = Set("Mark")
            
        class Group(Entity):
            _table_ = "Groups"
            number = PrimaryKey(str)
            department = Required(int)
            students = Set("Student")
            subjects = Set("Subject")

        class Subject(Entity):
            _table_ = "Subjects"
            name = PrimaryKey(unicode)
            groups = Set("Group")
            marks = Set("Mark")

        class Mark(Entity):
            _table_ = "Exams"
            student = Required(Student, column="student")
            subject = Required(Subject, column="subject")
            value = Required(int)
            PrimaryKey(student, subject)
            
        self.diagram = _diagram_
        self.db = Database('sqlite', ':memory:')
        self.db.get_connection().executescript(sql)
        self.db.commit()
        self.diagram.generate_mapping(self.db, check_tables = True)

    def test_find_one(self):
        Student = self.diagram.entities.get('Student')
        s = Student.find_one(101)
        self.assert_(s.record == 101)

    def test_rbits(self):
        Student = self.diagram.entities.get('Student')
        Group = self.diagram.entities.get('Group')
        g1 = Group.create(number='4142', department=44)
        s1 = Student.create(record=123, name='John', group=g1)
        self.assert_(True)


if __name__ == '__main__':
    unittest.main()    