from __future__ import absolute_import, print_function, division

import unittest

from pony.orm.core import *

logged_events = []

db = Database('sqlite', ':memory:')

class Person(db.Entity):
    id = PrimaryKey(int)
    name = Required(unicode)
    age = Required(int)
    def before_insert(self):
        logged_events.append('BI_' + self.name)
    def before_update(self):
        logged_events.append('BU_' + self.name)
    def before_delete(self):
        logged_events.append('BD_' + self.name)
    def after_insert(self):
        logged_events.append('AI_' + self.name)
    def after_update(self):
        logged_events.append('AU_' + self.name)
    def after_delete(self):
        logged_events.append('AD_' + self.name)

db.generate_mapping(create_tables=True)

class TestHooks(unittest.TestCase):

    def setUp(self):
        with db_session:
            db.execute('delete from Person')
            p1 = Person(id=1, name='John', age=22)
            p2 = Person(id=2, name='Mary', age=18)
            p3 = Person(id=3, name='Mike', age=25)
        logged_events[:] = []

    def tearDown(self):
        pass

    @db_session
    def test_1(self):
        p4 = Person(id=4, name='Bob', age=16)
        p5 = Person(id=5, name='Lucy', age=23)
        self.assertEqual(logged_events, [])
        db.flush()
        self.assertEqual(logged_events, ['BI_Bob', 'BI_Lucy', 'AI_Bob', 'AI_Lucy'])

    @db_session
    def test_2(self):
        p4 = Person(id=4, name='Bob', age=16)
        p1 = Person[1]  # auto-flush here
        p2 = Person[2]
        self.assertEqual(logged_events, ['BI_Bob', 'AI_Bob'])
        p2.age += 1
        p5 = Person(id=5, name='Lucy', age=23)
        db.flush()
        self.assertEqual(logged_events, ['BI_Bob', 'AI_Bob', 'BU_Mary', 'BI_Lucy', 'AU_Mary', 'AI_Lucy'])

if __name__ == '__main__':
    unittest.main()
