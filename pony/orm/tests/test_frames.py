from __future__ import absolute_import, print_function, division

import unittest

from pony.orm.core import *
import pony.orm.decompiling
from pony.orm.tests.testutils import *
from pony.orm.tests import setup_database, teardown_database

db = Database()

class Person(db.Entity):
    name = Required(unicode)
    age = Required(int)


class TestFrames(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_database(db)
        with db_session:
            p1 = Person(id=1, name='John', age=22)
            p2 = Person(id=2, name='Mary', age=18)
            p3 = Person(id=3, name='Mike', age=25)

    @classmethod
    def tearDownClass(cls):
        db.drop_all_tables(with_all_data=True)

    @db_session
    def test_select(self):
        x = 20
        result = select(p.id for p in Person if p.age > x)[:]
        self.assertEqual(set(result), {1, 3})

    @db_session
    def test_select_str(self):
        x = 20
        result = select('p.id for p in Person if p.age > x')[:]
        self.assertEqual(set(result), {1, 3})

    @db_session
    def test_left_join(self):
        x = 20
        result = left_join(p.id for p in Person if p.age > x)[:]
        self.assertEqual(set(result), {1, 3})

    @db_session
    def test_left_join_str(self):
        x = 20
        result = left_join('p.id for p in Person if p.age > x')[:]
        self.assertEqual(set(result), {1, 3})

    @db_session
    def test_get(self):
        x = 23
        result = get(p.id for p in Person if p.age > x)
        self.assertEqual(result, 3)

    @db_session
    def test_get_str(self):
        x = 23
        result = get('p.id for p in Person if p.age > x')
        self.assertEqual(result, 3)

    @db_session
    def test_exists(self):
        x = 23
        result = exists(p for p in Person if p.age > x)
        self.assertEqual(result, True)

    @db_session
    def test_exists_str(self):
        x = 23
        result = exists('p for p in Person if p.age > x')
        self.assertEqual(result, True)

    @db_session
    def test_entity_get(self):
        x = 23
        p = Person.get(lambda p: p.age > x)
        self.assertEqual(p, Person[3])

    @db_session
    def test_entity_get_str(self):
        x = 23
        p = Person.get('lambda p: p.age > x')
        self.assertEqual(p, Person[3])

    @db_session
    def test_entity_get_by_sql(self):
        x = 25
        p = Person.get_by_sql('select * from Person where age = $x')
        self.assertEqual(p, Person[3])

    @db_session
    def test_entity_select_by_sql(self):
        x = 25
        p = Person.select_by_sql('select * from Person where age = $x')
        self.assertEqual(p, [ Person[3] ])

    @db_session
    def test_entity_exists(self):
        x = 23
        result = Person.exists(lambda p: p.age > x)
        self.assertTrue(result)

    @db_session
    def test_entity_exists_str(self):
        x = 23
        result = Person.exists('lambda p: p.age > x')
        self.assertTrue(result)

    @db_session
    def test_entity_select(self):
        x = 20
        result = Person.select(lambda p: p.age > x)[:]
        self.assertEqual(set(result), {Person[1], Person[3]})

    @db_session
    def test_entity_select_str(self):
        x = 20
        result = Person.select('lambda p: p.age > x')[:]
        self.assertEqual(set(result), {Person[1], Person[3]})

    @db_session
    def test_order_by(self):
        x = 20
        y = -1
        result = Person.select(lambda p: p.age > x).order_by(lambda p: p.age * y)[:]
        self.assertEqual(result, [Person[3], Person[1]])

    @db_session
    def test_order_by_str(self):
        x = 20
        y = -1
        result = Person.select('lambda p: p.age > x').order_by('p.age * y')[:]
        self.assertEqual(result, [Person[3], Person[1]])

    @db_session
    def test_filter(self):
        x = 20
        y = 'M'
        result = Person.select(lambda p: p.age > x).filter(lambda p: p.name.startswith(y))[:]
        self.assertEqual(result, [Person[3]])

    @db_session
    def test_filter_str(self):
        x = 20
        y = 'M'
        result = Person.select('lambda p: p.age > x').filter('p.name.startswith(y)')[:]
        self.assertEqual(result, [Person[3]])

    @db_session
    def test_db_select(self):
        x = 20
        result = db.select('name from Person where age > $x order by name')
        self.assertEqual(result, ['John', 'Mike'])

    @db_session
    def test_db_get(self):
        x = 18
        result = db.get('name from Person where age = $x')
        self.assertEqual(result, 'Mary')

    @db_session
    def test_db_execute(self):
        x = 18
        result = db.execute('select name from Person where age = $x').fetchone()
        self.assertEqual(result, ('Mary',))

    @db_session
    def test_db_exists(self):
        x = 18
        result = db.exists('name from Person where age = $x')
        self.assertEqual(result, True)

    @raises_exception(pony.orm.decompiling.InvalidQuery,
                      'Use generator expression (... for ... in ...) '
                      'instead of list comprehension [... for ... in ...] inside query')
    @db_session
    def test_inner_list_comprehension(self):
        result = select(p.id for p in Person if p.age not in [
            p2.age for p2 in Person if p2.name.startswith('M')])[:]

    @db_session
    def test_outer_list_comprehension(self):
        names = ['John', 'Mary', 'Mike']
        persons = [ Person.select(lambda p: p.name == name).first() for name in names ]
        self.assertEqual(set(p.name for p in persons), {'John', 'Mary', 'Mike'})

if __name__ == '__main__':
    unittest.main()
