# -*- coding: cp1251 -*-

from pony.orm import *

GROUP = Table('Group')
GROUP['number'] = Column(str)
GROUP['faculty'] = Column(int, not_null=True)
GROUP['graduate_year'] = Column(int, not_null=True)
GROUP.set_primary_key('number')

SUBJECT = Table('Subject')
SUBJECT['name'] = Column(unicode)
SUBJECT.set_primary_key('name')

GROUP_SUBJECT = Table('Group_Subject')
GROUP_SUBJECT['group'] = Column(str, not_null=True, foreign_key=GROUP['number'])
GROUP_SUBJECT['subject'] = Column(unicode, not_null=True,
                                  foreign_key = SUBJECT['name'])
GROUP_SUBJECT.set_primary_key('group', 'subject')

STUDENT = Table('Student')
STUDENT['number'] = Column(int)
STUDENT['first_name'] = Column(unicode, not_null=True)        
STUDENT['mid_name'] = Column(unicode)
STUDENT['last_name'] = Column(unicode, not_null=True)
STUDENT['group'] = Column(str, not_null=True, foreign_key=GROUP['number'])
STUDENT.set_primary_key('number')

MARK = Table('Mark')
MARK['student'] = Column(int, not_null=True, foreign_key=STUDENT['number'])
MARK['subject'] = Column(unicode, not_null=True, foreign_key=SUBJECT['name'])
MARK['value'] = Column(int, not_null=True)
MARK.set_primary_key(MARK['student'], MARK['subject'])












