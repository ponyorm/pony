from pony.orm import *

m = Mapping()

Group = m.table('Group',
    Column('number', str, pk=True),
    Column('faculty', int, not_null=True),
    Column('graduate_year', int, not_null=True))

Subject = m.table('Subject',
    Column('name', unicode, pk=True))

Group_Subject = m.table('Group_Subject',
    Column('group', str, pk=True, fk=Group['number']),
    Column('subject', unicode, pk=True, fk=Subject['name']))

Student = m.table('Student',
    Column('number', int, pk=True),
    Column('first_name', unicode, not_null=True)
    Column('mid_name', unicode),
    Column('last_name', unicode, not_null=True),
    Column('group', str, not_null=True, fk=Group['number']))
              
Mark = m.table('Mark',
    Column('student', int, not_null=True, pk=True, fk=Student['number']),
    Column('subject', unicode, not_null=True, pk=True, fk=Subject['name'])
    Column('value', int, not_null=True))
