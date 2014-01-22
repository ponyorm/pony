from __future__ import with_statement

import unittest
from pony.orm.core import *

db = Database('sqlite', ':memory:')

class Male(db.Entity):
    name = Required(unicode)
    wife = Optional('Female', column='wife')

class Female(db.Entity):
    name = Required(unicode)
    husband = Optional('Male')

db.generate_mapping(create_tables=True)

class TestOneToOne(unittest.TestCase):
    def setUp(self):
        with db_session:
            db.execute('delete from male')
            db.execute('delete from female')
            db.insert('female', id=1, name='F1')
            db.insert('female', id=2, name='F2')
            db.insert('female', id=3, name='F3')
            db.insert('male', id=1, name='M1', wife=1)
            db.insert('male', id=2, name='M2', wife=2)
            db.insert('male', id=3, name='M3', wife=None)
        db_session.__enter__()
    def tearDown(self):
        db_session.__exit__()
    def test_1(self):
        Male[3].wife = Female[3]

        self.assertEqual(Male[3]._vals_[Male.wife], Female[3])
        self.assertEqual(Female[3]._vals_[Female.husband], Male[3])
        commit()
        wives = db.select('wife from Male order by Male.id')
        self.assertEqual([1, 2, 3], wives)
    def test_2(self):
        Female[3].husband = Male[3]

        self.assertEqual(Male[3]._vals_[Male.wife], Female[3])
        self.assertEqual(Female[3]._vals_[Female.husband], Male[3])
        commit()
        wives = db.select('wife from Male order by Male.id')
        self.assertEqual([1, 2, 3], wives)
    def test_3(self):
        Male[1].wife = None

        self.assertEqual(Male[1]._vals_[Male.wife], None)
        self.assertEqual(Female[1]._vals_[Female.husband], None)
        commit()
        wives = db.select('wife from Male order by Male.id')
        self.assertEqual([None, 2, None], wives)
    def test_4(self):
        Female[1].husband = None

        self.assertEqual(Male[1]._vals_[Male.wife], None)
        self.assertEqual(Female[1]._vals_[Female.husband], None)
        commit()
        wives = db.select('wife from Male order by Male.id')
        self.assertEqual([None, 2, None], wives)
    def test_5(self):
        Male[1].wife = Female[3]

        self.assertEqual(Male[1]._vals_[Male.wife], Female[3])
        self.assertEqual(Female[1]._vals_[Female.husband], None)
        self.assertEqual(Female[3]._vals_[Female.husband], Male[1])
        commit()
        wives = db.select('wife from Male order by Male.id')
        self.assertEqual([3, 2, None], wives)
    def test_6(self):
        Female[3].husband = Male[1]

        self.assertEqual(Male[1]._vals_[Male.wife], Female[3])
        self.assertEqual(Female[1]._vals_[Female.husband], None)
        self.assertEqual(Female[3]._vals_[Female.husband], Male[1])
        commit()
        wives = db.select('wife from Male order by Male.id')
        self.assertEqual([3, 2, None], wives)
    def test_7(self):
        Male[1].wife = Female[2]

        self.assertEqual(Male[1]._vals_[Male.wife], Female[2])
        self.assertEqual(Male[2]._vals_[Male.wife], None)
        self.assertEqual(Female[1]._vals_[Female.husband], None)
        self.assertEqual(Female[2]._vals_[Female.husband], Male[1])
        commit()
        wives = db.select('wife from Male order by Male.id')
        self.assertEqual([2, None, None], wives)
    def test_8(self):
        Female[2].husband = Male[1]

        self.assertEqual(Male[1]._vals_[Male.wife], Female[2])
        self.assertEqual(Male[2]._vals_[Male.wife], None)
        self.assertEqual(Female[1]._vals_[Female.husband], None)
        self.assertEqual(Female[2]._vals_[Female.husband], Male[1])
        commit()
        wives = db.select('wife from Male order by Male.id')
        self.assertEqual([2, None, None], wives)

if __name__ == '__main__':
    unittest.main()
