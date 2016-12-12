import unittest

from pony.orm import *
from pony.orm.tests.testutils import raises_exception

db = Database('sqlite', ':memory:')

class Person(db.Entity):
    id = PrimaryKey(int)
    name = Required(str)
    tel = Optional(str)

db.generate_mapping(check_tables=False)

with db_session:
    db.execute("""
        create table Person(
            id int primary key,
            name text,
            tel text
        )
    """)


class TestValidate(unittest.TestCase):

    @db_session
    def setUp(self):
        db.execute('delete from Person')

    @db_session
    def test_1(self):
        db.insert('Person', id=1, name='', tel='111')
        p = Person.get(id=1)
        self.assertEqual(p.name, '')

    @db_session
    def test_2(self):
        db.insert('Person', id=1, name=None, tel='111')
        p = Person.get(id=1)
        self.assertEqual(p.name, None)

if __name__ == '__main__':
    unittest.main()
