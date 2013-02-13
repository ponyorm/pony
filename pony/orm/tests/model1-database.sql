drop table if exists Groups;
create table Groups(
  number varchar(6) primary key,
  department integer not null
);

drop table if exists Students;
create table Students(
  record integer primary key,
  fio varchar(50) not null,
  [group] varchar(6) not null references Groups(number),
  scholarship integer not null default 0
);

drop table if exists Subjects;
create table Subjects(
  name varchar(50) primary key
);

drop table if exists Group_Subject;
create table Group_Subject(
  [group] varchar(6) not null references Groups(number),
  subject varchar(50) not null references Subjects(name),
  primary key ([group], subject)
);

drop table if exists Exams;
create table Exams(
  student integer not null references Students(number),
  subject varchar(50) not null references Subjects(name),
  value integer not null,
  primary key (student, subject)
);

insert into Groups values ('4145', 44);
insert into Groups values ('4146', 44);
insert into Groups values ('3132', 33);

insert into Students values(101, 'Bob', '4145', 0);
insert into Students values(102, 'Joe', '4145', 800);
insert into Students values(103, 'Alex', '4145', 0);
insert into Students values(104, 'Brad', '3132', 500);
insert into Students values(105, 'John', '3132', 1000);

insert into Subjects values('Physics');
insert into Subjects values('Chemistry');
insert into Subjects values('Math');

insert into Group_Subject values('4145', 'Physics');
insert into Group_Subject values('4145', 'Chemistry');
insert into Group_Subject values('4145', 'Math');
insert into Group_Subject values('3132', 'Physics');
insert into Group_Subject values('3132', 'Math');

insert into Exams values (101, 'Physics', 4);
insert into Exams values (101, 'Math', 3);
insert into Exams values (102, 'Chemistry', 5);
insert into Exams values (103, 'Physics', 2);
insert into Exams values (103, 'Chemistry', 4);
