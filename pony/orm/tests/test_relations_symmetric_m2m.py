from __future__ import with_statement

import unittest
from pony.orm.core import *

db = Database('sqlite', ':memory:')

class Person(db.Entity):
    name = Required(unicode)
    friends = Set('Person', reverse='friends')
db.generate_mapping(create_tables=True)


class TestSymmetric(unittest.TestCase):
    def setUp(self):
        rollback()
        with db_session:
            for p in Person.select(): p.delete()
            commit()
            db.insert('person', id=1, name='A')
            db.insert('person', id=2, name='B')
            db.insert('person', id=3, name='C')
            db.insert('person', id=4, name='D')
            db.insert('person', id=5, name='E')
            db.insert('person_friends', person=1, person_2=2)
            db.insert('person_friends', person=2, person_2=1)
            db.insert('person_friends', person=1, person_2=3)
            db.insert('person_friends', person=3, person_2=1)
        rollback()
        db_session.__enter__()
    def tearDown(self):
        rollback()
        db_session.__exit__()
    def test1a(self):
        p1 = Person[1]
        p4 = Person[4]
        p1.friends.add(p4)
        self.assertEqual(set(p4.friends), set([p1]))
    def test1b(self):
        p1 = Person[1]
        p4 = Person[4]
        p1.friends.add(p4)
        self.assertEqual(set(p1.friends), set([Person[2], Person[3], p4]))
    def test1c(self):
        p1 = Person[1]
        p4 = Person[4]
        p1.friends.add(p4)
        commit()
        rows = db.select("* from person_friends order by person, person_2")
        self.assertEqual(rows, [(1,2), (1,3), (1,4), (2,1), (3,1), (4,1)])

    def test2a(self):
        p1 = Person[1]
        p2 = Person[2]
        p1.friends.remove(p2)
        self.assertEqual(set(p1.friends), set([Person[3]]))
    def test2b(self):
        p1 = Person[1]
        p2 = Person[2]
        p1.friends.remove(p2)
        self.assertEqual(set(Person[3].friends), set([p1]))
    def test2c(self):
        p1 = Person[1]
        p2 = Person[2]
        p1.friends.remove(p2)
        self.assertEqual(set(p2.friends), set())
    def test2d(self):
        p1 = Person[1]
        p2 = Person[2]
        p1.friends.remove(p2)
        commit()
        rows = db.select("* from person_friends order by person, person_2")
        self.assertEqual(rows, [(1,3), (3,1)])

    def test3a(self):
        db.execute('delete from person_friends')
        db.insert('person_friends', person=1, person_2=2)
        p1 = Person[1]
        p2 = Person[2]
        p2_friends = set(p2.friends)
        self.assertEqual(p2_friends, set())
        try:
            p1_friends = set(p1.friends)
        except UnrepeatableReadError, e:
            self.assertEqual(e.args[0], "Phantom object Person[1] appeared in collection Person[2].friends")
        else: self.assert_(False)
    def test3b(self):
        db.execute('delete from person_friends')
        db.insert('person_friends', person=1, person_2=2)
        p1 = Person[1]
        p2 = Person[2]
        p1_friends = set(p1.friends)
        self.assertEqual(p1_friends, set([p2]))
        try:
            p2_friends = set(p2.friends)
        except UnrepeatableReadError, e:
            self.assertEqual(e.args[0], "Phantom object Person[1] disappeared from collection Person[2].friends")
        else: self.assert_(False)

if __name__ == '__main__':
    unittest.main()
