import unittest
from pony.orm.core import *
from testutils import raises_exception

db = Database('sqlite', ':memory:')

class Person(db.Entity):
    name = Required(unicode)
    spouse = Optional('Person', reverse='spouse')

db.generate_mapping(create_tables=True)

class TestSymmetric(unittest.TestCase):
    def setUp(self):
        rollback()
        db.execute('update person set spouse=null')
        db.execute('delete from person')
        db.insert('person', id=1, name='A')
        db.insert('person', id=2, name='B', spouse=1)
        db.execute('update person set spouse=2 where id=1')
        db.insert('person', id=3, name='C')
        db.insert('person', id=4, name='D', spouse=3)
        db.execute('update person set spouse=4 where id=3')
        db.insert('person', id=5, name='E', spouse=None)
        commit()
        rollback()
    def test1(self):
        p1 = Person[1]
        p2 = Person[2]
        p5 = Person[5]
        p1.spouse = p5
        commit()
        self.assertEqual(p1._vals_.get('spouse'), p5)
        self.assertEqual(p5._vals_.get('spouse'), p1)
        self.assertEqual(p2._vals_.get('spouse'), None)
        data = db.select('spouse from person order by id')
        self.assertEqual([5, None, 4, 3, 1], data)
    def test2(self):
        p1 = Person[1]
        p2 = Person[2]
        p1.spouse = None
        commit()
        self.assertEqual(p1._vals_.get('spouse'), None)
        self.assertEqual(p2._vals_.get('spouse'), None)
        data = db.select('spouse from person order by id')
        self.assertEqual([None, None, 4, 3, None], data)
    def test3(self):
        p1 = Person[1]
        p2 = Person[2]
        p3 = Person[3]
        p4 = Person[4]
        p1.spouse = p3
        commit()
        self.assertEqual(p1._vals_.get('spouse'), p3)
        self.assertEqual(p2._vals_.get('spouse'), None)
        self.assertEqual(p3._vals_.get('spouse'), p1)
        self.assertEqual(p4._vals_.get('spouse'), None)
        data = db.select('spouse from person order by id')
        self.assertEqual([3, None, 1, None, None], data)
    def test4(self):
        persons = set(select(p for p in Person if p.spouse.name in ('B', 'D')))
        self.assertEqual(persons, set([Person[1], Person[3]]))
    @raises_exception(OptimisticCheckError, 'Value of Person.spouse for Person[1] was updated outside of current transaction')
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
        self.assertEqual(p2._vals_.get('spouse'), p1)

if __name__ == '__main__':
    unittest.main()
