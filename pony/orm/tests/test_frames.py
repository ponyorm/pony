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
    def test_get(self):
        x = 23
        p = Person.get(lambda p: p.age > x)
        self.assertEquals(p, Person[3])

    @db_session
    def test_get_2(self):
        x = 23
        p = Person.get('lambda p: p.age > x')
        self.assertEquals(p, Person[3])

    @db_session
    def test_select(self):
        x = 20
        result = Person.select(lambda p: p.age > x)[:]
        self.assertEquals(set(result), set([Person[1], Person[3]]))

    @db_session
    def test_select_2(self):
        x = 20
        result = Person.select('lambda p: p.age > x')[:]
        self.assertEquals(set(result), set([Person[1], Person[3]]))

    @db_session
    def test_order_by(self):
        x = 20
        y = -1
        result = Person.select(lambda p: p.age > x).order_by(lambda p: p.age * y)[:]
        self.assertEquals(result, [Person[3], Person[1]])

    @db_session
    def test_order_by_2(self):
        x = 20
        y = -1
        result = Person.select('lambda p: p.age > x').order_by('p.age * y')[:]
        self.assertEquals(result, [Person[3], Person[1]])

    @db_session
    def test_filter(self):
        x = 20
        y = 'M'
        result = Person.select(lambda p: p.age > x).filter(lambda p: p.name.startswith(y))[:]
        self.assertEquals(result, [Person[3]])

    @db_session
    def test_filter_2(self):
        x = 20
        y = 'M'
        result = Person.select('lambda p: p.age > x').filter('p.name.startswith(y)')[:]
        self.assertEquals(result, [Person[3]])

if __name__ == '__main__':
    unittest.main()
