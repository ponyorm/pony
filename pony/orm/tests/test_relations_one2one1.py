from __future__ import absolute_import, print_function, division

import unittest
from pony.orm.core import *
from pony.orm.tests import setup_database, teardown_database

db = Database()


class Male(db.Entity):
    name = Required(unicode)
    wife = Optional('Female', column='wife')


class Female(db.Entity):
    name = Required(unicode)
    husband = Optional('Male')


class TestOneToOne(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_database(db)

    @classmethod
    def tearDownClass(cls):
        teardown_database(db)

    def setUp(self):
        with db_session:
            db.execute('delete from male')
            db.execute('delete from female')
            db.insert(Female, id=1, name='F1')
            db.insert(Female, id=2, name='F2')
            db.insert(Female, id=3, name='F3')
            db.insert(Male, id=1, name='M1', wife=1)
            db.insert(Male, id=2, name='M2', wife=2)
            db.insert(Male, id=3, name='M3', wife=None)

    @db_session
    def test_1(self):
        Male[3].wife = Female[3]

        self.assertEqual(Male[3]._vals_[Male.wife], Female[3])
        self.assertEqual(Female[3]._vals_[Female.husband], Male[3])
        commit()
        wives = db.select('wife from Male order by Male.id')
        self.assertEqual([1, 2, 3], wives)

    @db_session
    def test_2(self):
        Female[3].husband = Male[3]

        self.assertEqual(Male[3]._vals_[Male.wife], Female[3])
        self.assertEqual(Female[3]._vals_[Female.husband], Male[3])
        commit()
        wives = db.select('wife from Male order by Male.id')
        self.assertEqual([1, 2, 3], wives)

    @db_session
    def test_3(self):
        Male[1].wife = None

        self.assertEqual(Male[1]._vals_[Male.wife], None)
        self.assertEqual(Female[1]._vals_[Female.husband], None)
        commit()
        wives = db.select('wife from Male order by Male.id')
        self.assertEqual([None, 2, None], wives)

    @db_session
    def test_4(self):
        Female[1].husband = None

        self.assertEqual(Male[1]._vals_[Male.wife], None)
        self.assertEqual(Female[1]._vals_[Female.husband], None)
        commit()
        wives = db.select('wife from Male order by Male.id')
        self.assertEqual([None, 2, None], wives)

    @db_session
    def test_5(self):
        Male[1].wife = Female[3]

        self.assertEqual(Male[1]._vals_[Male.wife], Female[3])
        self.assertEqual(Female[1]._vals_[Female.husband], None)
        self.assertEqual(Female[3]._vals_[Female.husband], Male[1])
        commit()
        wives = db.select('wife from Male order by Male.id')
        self.assertEqual([3, 2, None], wives)

    @db_session
    def test_6(self):
        Female[3].husband = Male[1]

        self.assertEqual(Male[1]._vals_[Male.wife], Female[3])
        self.assertEqual(Female[1]._vals_[Female.husband], None)
        self.assertEqual(Female[3]._vals_[Female.husband], Male[1])
        commit()
        wives = db.select('wife from Male order by Male.id')
        self.assertEqual([3, 2, None], wives)

    @db_session
    def test_7(self):
        Male[1].wife = Female[2]

        self.assertEqual(Male[1]._vals_[Male.wife], Female[2])
        self.assertEqual(Male[2]._vals_[Male.wife], None)
        self.assertEqual(Female[1]._vals_[Female.husband], None)
        self.assertEqual(Female[2]._vals_[Female.husband], Male[1])
        commit()
        wives = db.select('wife from Male order by Male.id')
        self.assertEqual([2, None, None], wives)

    @db_session
    def test_8(self):
        Female[2].husband = Male[1]

        self.assertEqual(Male[1]._vals_[Male.wife], Female[2])
        self.assertEqual(Male[2]._vals_[Male.wife], None)
        self.assertEqual(Female[1]._vals_[Female.husband], None)
        self.assertEqual(Female[2]._vals_[Female.husband], Male[1])
        commit()
        wives = db.select('wife from Male order by Male.id')
        self.assertEqual([2, None, None], wives)

    @db_session
    def test_9(self):
        f4 = Female(id=4, name='F4')
        m4 = Male(id=4, name='M4', wife=f4)
        flush()
        self.assertEqual(f4._status_, 'inserted')
        self.assertEqual(m4._status_, 'inserted')

    @db_session
    def test_10(self):
        m4 = Male(id=4, name='M4')
        f4 = Female(id=4, name='F4', husband=m4)
        flush()
        self.assertEqual(f4._status_, 'inserted')
        self.assertEqual(m4._status_, 'inserted')

    @db_session
    def test_to_dict_1(self):
        m = Male[1]
        d = m.to_dict()
        self.assertEqual(d, dict(id=1, name='M1', wife=1))

    @db_session
    def test_to_dict_2(self):
        m = Male[3]
        d = m.to_dict()
        self.assertEqual(d, dict(id=3, name='M3', wife=None))

    @db_session
    def test_to_dict_3(self):
        f = Female[1]
        d = f.to_dict()
        self.assertEqual(d, dict(id=1, name='F1', husband=1))

    @db_session
    def test_to_dict_4(self):
        f = Female[3]
        d = f.to_dict()
        self.assertEqual(d, dict(id=3, name='F3', husband=None))


if __name__ == '__main__':
    unittest.main()
