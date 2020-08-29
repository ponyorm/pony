import unittest, warnings

from pony.orm import *
from pony.orm import core
from pony.orm.tests.testutils import raises_exception
from pony.orm.tests import db_params, teardown_database

db = Database()

class Person(db.Entity):
    id = PrimaryKey(int)
    name = Required(str)
    tel = Optional(str)


table_name = 'person'

class TestValidate(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        db.bind(**db_params)
        db.generate_mapping(check_tables=False)
        db.drop_all_tables(with_all_data=True)
        with db_session(ddl=True):
            db.execute("""
                create table "%s"(
                    id int primary key,
                    name text,
                    tel text
                )
            """ % table_name)

    @classmethod
    def tearDownClass(cls):
        teardown_database(db)

    @db_session
    def setUp(self):
        db.execute('delete from "%s"' % table_name)
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
            db.insert(table_name, id=1, name='', tel='111')
            p = Person.get(id=1)
            self.assertEqual(p.name, '')

    @raises_exception(DatabaseContainsIncorrectEmptyValue,
                      'Database contains empty string for required attribute Person.name')
    @db_session
    def test_1b(self):
        with warnings.catch_warnings():
            warnings.simplefilter('error', DatabaseContainsIncorrectEmptyValue)
            db.insert(table_name, id=1, name='', tel='111')
            p = Person.get(id=1)

    @db_session
    def test_2a(self):
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', DatabaseContainsIncorrectEmptyValue)
            db.insert(table_name, id=1, name=None, tel='111')
            p = Person.get(id=1)
            self.assertEqual(p.name, None)

    @raises_exception(DatabaseContainsIncorrectEmptyValue,
                      'Database contains NULL for required attribute Person.name')
    @db_session
    def test_2b(self):
        with warnings.catch_warnings():
            warnings.simplefilter('error', DatabaseContainsIncorrectEmptyValue)
            db.insert(table_name, id=1, name=None, tel='111')
            p = Person.get(id=1)


if __name__ == '__main__':
    unittest.main()
