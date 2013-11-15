import unittest

from pony.orm.core import *

db = Database('sqlite', ':memory:')

class Person(db.Entity):
    name = Required(unicode)
    age = Required(int)

db.generate_mapping(create_tables=True)

with db_session:
    p1 = Person(name='John', age=22)
    p2 = Person(name='Mary', age=18)
    p3 = Person(name='Mike', age=25)

class TestFrames(unittest.TestCase):

    @db_session
    def test_select(self):
        x = 20
        result = select(p.id for p in Person if p.age > x)[:]
        self.assertEqual(set(result), set([1, 3]))

    @db_session
    def test_select_str(self):
        x = 20
        result = select('p.id for p in Person if p.age > x')[:]
        self.assertEqual(set(result), set([1, 3]))

    @db_session
    def test_left_join(self):
        x = 20
        result = left_join(p.id for p in Person if p.age > x)[:]
        self.assertEqual(set(result), set([1, 3]))

    @db_session
    def test_left_join_str(self):
        x = 20
        result = left_join('p.id for p in Person if p.age > x')[:]
        self.assertEqual(set(result), set([1, 3]))

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
        self.assertEqual(set(result), set([Person[1], Person[3]]))

    @db_session
    def test_entity_select_str(self):
        x = 20
        result = Person.select('lambda p: p.age > x')[:]
        self.assertEqual(set(result), set([Person[1], Person[3]]))

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

if __name__ == '__main__':
    unittest.main()
