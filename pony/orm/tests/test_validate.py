import unittest, warnings

from pony.orm import *
from pony.orm import core
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

warnings.simplefilter('error', )


class TestValidate(unittest.TestCase):

    @db_session
    def setUp(self):
        db.execute('delete from Person')
        registry = getattr(core, '__warningregistry__', {})
        for key in list(registry):
            if type(key) is not tuple: continue
            text, category, lineno = key
            if category is DatabaseContainsIncorrectEmptyValue:
                del registry[key]

    @db_session
    def test_1a(self):
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', DatabaseContainsIncorrectEmptyValue)
            db.insert('Person', id=1, name='', tel='111')
            p = Person.get(id=1)
            self.assertEqual(p.name, '')

    @raises_exception(DatabaseContainsIncorrectEmptyValue,
                      'Database contains empty string for required attribute Person.name')
    @db_session
    def test_1b(self):
        with warnings.catch_warnings():
            warnings.simplefilter('error', DatabaseContainsIncorrectEmptyValue)
            db.insert('Person', id=1, name='', tel='111')
            p = Person.get(id=1)

    @db_session
    def test_2a(self):
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', DatabaseContainsIncorrectEmptyValue)
            db.insert('Person', id=1, name=None, tel='111')
            p = Person.get(id=1)
            self.assertEqual(p.name, None)

    @raises_exception(DatabaseContainsIncorrectEmptyValue,
                      'Database contains NULL for required attribute Person.name')
    @db_session
    def test_2b(self):
        with warnings.catch_warnings():
            warnings.simplefilter('error', DatabaseContainsIncorrectEmptyValue)
            db.insert('Person', id=1, name=None, tel='111')
            p = Person.get(id=1)


if __name__ == '__main__':
    unittest.main()
