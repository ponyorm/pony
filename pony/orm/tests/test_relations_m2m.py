from __future__ import with_statement

import unittest
from pony.orm.core import *

db = Database('sqlite', ':memory:')

class Group(db.Entity):
    number = PrimaryKey(int)
    subjects = Set("Subject")

class Subject(db.Entity):
    name = PrimaryKey(str)
    groups = Set(Group)

db.generate_mapping(create_tables=True)

with db_session:
   g1 = Group(number=101)
   g2 = Group(number=102)
   s1 = Subject(name='Subj1')
   s2 = Subject(name='Subj2')
   s3 = Subject(name='Subj3')
   s4 = Subject(name='Subj4')
   g1.subjects = [ s1, s2 ]

class TestManyToManyNonComposite(unittest.TestCase):
    def setUp(self):
        rollback()
        db_session.__enter__()
    def tearDown(self):
        rollback()
        db_session.__exit__()
    def test_add_remove(self):
        g = Group.get(number=101)
        subjects = Subject.select()[:]
        g.subjects.remove(subjects[:2])
        g.subjects.add(subjects[-2:])
        commit()
        rollback()
        self.assertEqual(Group[101].subjects, set([Subject['Subj3'], Subject['Subj4']]))
        db_subjects = db.select('subject from Group_Subject where "group" = 101')
        self.assertEqual(db_subjects , ['Subj3', 'Subj4'])

if __name__ == "__main__":
    unittest.main()
