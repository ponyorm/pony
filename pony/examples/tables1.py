# -*- coding: cp1251 -*-

import pony
from pony import *

GROUP = Table('Group')
GROUP['number'] = Column(str, primary_key=True)
GROUP['faculty'] = Column(int, not_null=True)
GROUP['graduate_year'] = Column(int, not_null=True)

SUBJECT = Table('Subject')
SUBJECT['name'] = Column(unicode, primary_key=True)

GROUP_SUBJECT = Table('Group_Subject')
GROUP_SUBJECT['group'] = Column(str, not_null=True, foreign_key=GROUP.number)
GROUP_SUBJECT['subject'] = Column(unicode, not_null=True,
                                  foreign_key = SUBJECT.name)

STUDENT = Table('Student')
STUDENT['number'] = Column(int, primary_key=True)
STUDENT['first_name'] = Column(unicode, not_null=True)        
STUDENT['mid_name'] = Column(unicode, not_null=False)
STUDENT['last_name'] = Column(unicode, not_null=True)
STUDENT['group'] = Column(str, not_null=True, foreign_key=GROUP.number)

MARK = Table('Mark')
MARK['student'] = Column(int, not_null=True, foreign_key=STUDENT.number)
MARK['subject'] = Column(unicode, not_null=True, foreign_key=SUBJECT.name)
MARK['value'] = Column(int, not_null=True)
MARK.set_primary_key(MARK.student, MARK.subject)












