import unittest

from pony.orm import *
from pony.orm.tests.testutils import raises_exception

db = Database('sqlite', ':memory:')

class Person(db.Entity):
    name = Required(str)
    tel = Optional(str)

db.generate_mapping(create_tables=True)

class TestAutostrip(unittest.TestCase):

    @db_session
    def test_1(self):
        p = Person(name=' John  ', tel=' ')
        p.flush()
        self.assertEqual(p.name, 'John')
        self.assertEqual(p.tel, '')

    @raises_exception(ValueError, 'Attribute Person.name is required')
    @db_session
    def test_2(self):
        p = Person(name=' ')

if __name__ == '__main__':
    unittest.main()
