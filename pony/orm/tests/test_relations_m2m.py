from __future__ import with_statement

import unittest
from pony.orm.core import *

class TestManyToManyNonComposite(unittest.TestCase):

    def setUp(self):
        db = Database('sqlite', ':memory:')

        class Group(db.Entity):
            number = PrimaryKey(int)
            subjects = Set("Subject")

        class Subject(db.Entity):
            name = PrimaryKey(str)
            groups = Set(Group)

        self.db = db
        self.Group = Group
        self.Subject = Subject
        
        self.db.generate_mapping(create_tables=True)

        with db_session:
           g1 = Group(number=101)
           g2 = Group(number=102)
           s1 = Subject(name='Subj1')
           s2 = Subject(name='Subj2')
           s3 = Subject(name='Subj3')
           s4 = Subject(name='Subj4')
           g1.subjects = [ s1, s2 ]

    def test_1(self):
        db, Group, Subject = self.db, self.Group, self.Subject

        with db_session:
            g = Group.get(number=101)
            s1, s2, s3, s4 = Subject.select()[:]
            g.subjects.remove([s1, s2])
            g.subjects.add([s3, s4])

        with db_session:
            db_subjects = db.select('subject from Group_Subject where "group" = 101')
            self.assertEqual(db_subjects , ['Subj3', 'Subj4'])
            self.assertEqual(Group[101].subjects, set([Subject['Subj3'], Subject['Subj4']]))

if __name__ == "__main__":
    unittest.main()
