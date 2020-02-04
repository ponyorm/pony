from __future__ import absolute_import, print_function, division

import unittest
from pony.orm.core import *
from pony.orm.tests import setup_database, teardown_database

db = Database()


class Person(db.Entity):
    name = Required(unicode)
    friends = Set('Person', reverse='friends')


class TestSymmetricM2M(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_database(db)

    @classmethod
    def tearDownClass(cls):
        teardown_database(db)

    def setUp(self):
        with db_session:
            for p in Person.select(): p.delete()
        with db_session:
            db.insert(Person, id=1, name='A')
            db.insert(Person, id=2, name='B')
            db.insert(Person, id=3, name='C')
            db.insert(Person, id=4, name='D')
            db.insert(Person, id=5, name='E')
            db.insert(Person.friends, person=1, person_2=2)
            db.insert(Person.friends, person=2, person_2=1)
            db.insert(Person.friends, person=1, person_2=3)
            db.insert(Person.friends, person=3, person_2=1)
        db_session.__enter__()
    def tearDown(self):
        rollback()
        db_session.__exit__()
    def test1a(self):
        p1 = Person[1]
        p4 = Person[4]
        p1.friends.add(p4)
        self.assertEqual(set(p4.friends), {p1})
    def test1b(self):
        p1 = Person[1]
        p4 = Person[4]
        p1.friends.add(p4)
        self.assertEqual(set(p1.friends), {Person[2], Person[3], p4})
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
        self.assertEqual(set(p1.friends), {Person[3]})
    def test2b(self):
        p1 = Person[1]
        p2 = Person[2]
        p1.friends.remove(p2)
        self.assertEqual(set(Person[3].friends), {p1})
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
        db.insert(Person.friends, person=1, person_2=2)
        p1 = Person[1]
        p2 = Person[2]
        p2_friends = set(p2.friends)
        self.assertEqual(p2_friends, set())
        try: p1_friends = set(p1.friends)
        except UnrepeatableReadError as e: self.assertEqual(e.args[0],
            "Phantom object Person[1] appeared in collection Person[2].friends")
        else: self.fail()
    def test3b(self):
        db.execute('delete from person_friends')
        db.insert(Person.friends, person=1, person_2=2)
        p1 = Person[1]
        p2 = Person[2]
        p1_friends = set(p1.friends)
        self.assertEqual(p1_friends, {p2})
        try: p2_friends = set(p2.friends)
        except UnrepeatableReadError as e: self.assertEqual(e.args[0],
            "Phantom object Person[1] disappeared from collection Person[2].friends")
        else: self.fail()

if __name__ == '__main__':
    unittest.main()
