from __future__ import absolute_import, print_function, division

import unittest

from pony.orm.core import *
from pony.orm.tests.testutils import raises_exception
from pony.orm.tests import setup_database, teardown_database, only_for

db = Database()


class Person(db.Entity):
    name = Required(unicode)
    spouse = Optional('Person', reverse='spouse')


class TestSymmetricOne2One(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_database(db)

    @classmethod
    def tearDownClass(cls):
        teardown_database(db)

    def setUp(self):
        with db_session:
            db.execute('update person set spouse=null')
            db.execute('delete from person')
            db.insert(Person, id=1, name='A')
            db.insert(Person, id=2, name='B', spouse=1)
            db.execute('update person set spouse=2 where id=1')
            db.insert(Person, id=3, name='C')
            db.insert(Person, id=4, name='D', spouse=3)
            db.execute('update person set spouse=4 where id=3')
            db.insert(Person, id=5, name='E', spouse=None)
        db_session.__enter__()
    def tearDown(self):
        db_session.__exit__()
    def test1(self):
        p1 = Person[1]
        p2 = Person[2]
        p5 = Person[5]
        p1.spouse = p5
        commit()
        self.assertEqual(p1._vals_.get(Person.spouse), p5)
        self.assertEqual(p5._vals_.get(Person.spouse), p1)
        self.assertEqual(p2._vals_.get(Person.spouse), None)
        data = db.select('spouse from person order by id')
        self.assertEqual([5, None, 4, 3, 1], data)
    def test2(self):
        p1 = Person[1]
        p2 = Person[2]
        p1.spouse = None
        commit()
        self.assertEqual(p1._vals_.get(Person.spouse), None)
        self.assertEqual(p2._vals_.get(Person.spouse), None)
        data = db.select('spouse from person order by id')
        self.assertEqual([None, None, 4, 3, None], data)
    def test3(self):
        p1 = Person[1]
        p2 = Person[2]
        p3 = Person[3]
        p4 = Person[4]
        p1.spouse = p3
        commit()
        self.assertEqual(p1._vals_.get(Person.spouse), p3)
        self.assertEqual(p2._vals_.get(Person.spouse), None)
        self.assertEqual(p3._vals_.get(Person.spouse), p1)
        self.assertEqual(p4._vals_.get(Person.spouse), None)
        data = db.select('spouse from person order by id')
        self.assertEqual([3, None, 1, None, None], data)
    def test4(self):
        persons = set(select(p for p in Person if p.spouse.name in ('B', 'D')))
        self.assertEqual(persons, {Person[1], Person[3]})
    @raises_exception(UnrepeatableReadError, 'Multiple Person objects linked with the same Person[2] object. '
                                             'Maybe Person.spouse attribute should be Set instead of Optional')
    def test5(self):
        db.execute('update person set spouse = 3 where id = 2')
        p1 = Person[1]
        p1.spouse
        p2 = Person[2]
        p2.name
    def test6(self):
        db.execute('update person set spouse = 3 where id = 2')
        p1 = Person[1]
        p2 = Person[2]
        p2.name
        p1.spouse
        self.assertEqual(p2._vals_.get(Person.spouse), p1)

if __name__ == '__main__':
    unittest.main()
