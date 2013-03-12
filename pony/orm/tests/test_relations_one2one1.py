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
        rollback()
        db.execute('delete from Male')
        db.execute('delete from Female')
        db.insert('Male', id=1, name='M1', wife=1)
        db.insert('Male', id=2, name='M2', wife=2)
        db.insert('Male', id=3, name='M3', wife=None)
        db.insert('Female', id=1, name='F1')
        db.insert('Female', id=2, name='F2')
        db.insert('Female', id=3, name='F3')
        commit()
        rollback()
    def test_1(self):
        Male[3].wife = Female[3]

        self.assertEqual(Male[3]._vals_['wife'], Female[3])
        self.assertEqual(Female[3]._vals_['husband'], Male[3])
        commit()
        wives = db.select('wife from Male order by Male.id')
        self.assertEqual([1, 2, 3], wives)
    def test_2(self):
        Female[3].husband = Male[3]

        self.assertEqual(Male[3]._vals_['wife'], Female[3])
        self.assertEqual(Female[3]._vals_['husband'], Male[3])
        commit()
        wives = db.select('wife from Male order by Male.id')
        self.assertEqual([1, 2, 3], wives)
    def test_3(self):
        Male[1].wife = None

        self.assertEqual(Male[1]._vals_['wife'], None)
        self.assertEqual(Female[1]._vals_['husband'], None)
        commit()
        wives = db.select('wife from Male order by Male.id')
        self.assertEqual([None, 2, None], wives)
    def test_4(self):
        Female[1].husband = None

        self.assertEqual(Male[1]._vals_['wife'], None)
        self.assertEqual(Female[1]._vals_['husband'], None)
        commit()
        wives = db.select('wife from Male order by Male.id')
        self.assertEqual([None, 2, None], wives)
    def test_5(self):
        Male[1].wife = Female[3]

        self.assertEqual(Male[1]._vals_['wife'], Female[3])
        self.assertEqual(Female[1]._vals_['husband'], None)
        self.assertEqual(Female[3]._vals_['husband'], Male[1])
        commit()
        wives = db.select('wife from Male order by Male.id')
        self.assertEqual([3, 2, None], wives)
    def test_6(self):
        Female[3].husband = Male[1]

        self.assertEqual(Male[1]._vals_['wife'], Female[3])
        self.assertEqual(Female[1]._vals_['husband'], None)
        self.assertEqual(Female[3]._vals_['husband'], Male[1])
        commit()
        wives = db.select('wife from Male order by Male.id')
        self.assertEqual([3, 2, None], wives)
    def test_7(self):
        Male[1].wife = Female[2]

        self.assertEqual(Male[1]._vals_['wife'], Female[2])
        self.assertEqual(Male[2]._vals_['wife'], None)
        self.assertEqual(Female[1]._vals_['husband'], None)
        self.assertEqual(Female[2]._vals_['husband'], Male[1])
        commit()
        wives = db.select('wife from Male order by Male.id')
        self.assertEqual([2, None, None], wives)
    def test_8(self):
        Female[2].husband = Male[1]

        self.assertEqual(Male[1]._vals_['wife'], Female[2])
        self.assertEqual(Male[2]._vals_['wife'], None)
        self.assertEqual(Female[1]._vals_['husband'], None)
        self.assertEqual(Female[2]._vals_['husband'], Male[1])
        commit()
        wives = db.select('wife from Male order by Male.id')
        self.assertEqual([2, None, None], wives)

if __name__ == '__main__':
    unittest.main()
