from __future__ import absolute_import, print_function, division

import unittest

from pony.orm.core import *
from pony.orm.tests import setup_database, teardown_database, db_params

logged_events = []

db = Database()


class Person(db.Entity):
    id = PrimaryKey(int)
    name = Required(unicode)
    age = Required(int)

    def before_insert(self):
        logged_events.append('BI_' + self.name)
        do_before_insert(self)

    def before_update(self):
        logged_events.append('BU_' + self.name)
        do_before_update(self)

    def before_delete(self):
        logged_events.append('BD_' + self.name)
        do_before_delete(self)

    def after_insert(self):
        logged_events.append('AI_' + self.name)
        do_after_insert(self)

    def after_update(self):
        logged_events.append('AU_' + self.name)
        do_after_update(self)

    def after_delete(self):
        logged_events.append('AD_' + self.name)
        do_after_delete(self)


def do_nothing(person):
    pass


def set_hooks_to_do_nothing():
    global do_before_insert, do_before_update, do_before_delete
    global do_after_insert, do_after_update, do_after_delete
    do_before_insert = do_before_update = do_before_delete = do_nothing
    do_after_insert = do_after_update = do_after_delete = do_nothing


db.bind(**db_params)
db.generate_mapping(check_tables=False)

set_hooks_to_do_nothing()


def flush_for(*objects):
    for obj in objects:
        obj.flush()


class TestHooks(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_database(db)

    @classmethod
    def tearDownClass(cls):
        teardown_database(db)

    def setUp(self):
        set_hooks_to_do_nothing()
        with db_session:
            db.execute('delete from Person')
            p1 = Person(id=1, name='John', age=22)
            p2 = Person(id=2, name='Mary', age=18)
            p3 = Person(id=3, name='Mike', age=25)
        logged_events[:] = []

    def tearDown(self):
        pass

    @db_session
    def test_1a(self):
        p4 = Person(id=4, name='Bob', age=16)
        p5 = Person(id=5, name='Lucy', age=23)
        self.assertEqual(logged_events, [])
        db.flush()
        self.assertEqual(logged_events, ['BI_Bob', 'BI_Lucy', 'AI_Bob', 'AI_Lucy'])

    @db_session
    def test_1b(self):
        p4 = Person(id=4, name='Bob', age=16)
        p5 = Person(id=5, name='Lucy', age=23)
        self.assertEqual(logged_events, [])
        flush_for(p4, p5)
        self.assertEqual(logged_events, ['BI_Bob', 'AI_Bob', 'BI_Lucy', 'AI_Lucy'])

    @db_session
    def test_2a(self):
        p4 = Person(id=4, name='Bob', age=16)
        p1 = Person[1]  # auto-flush here
        p2 = Person[2]
        self.assertEqual(logged_events, ['BI_Bob', 'AI_Bob'])
        p2.age += 1
        p5 = Person(id=5, name='Lucy', age=23)
        db.flush()
        self.assertEqual(logged_events, ['BI_Bob', 'AI_Bob', 'BU_Mary', 'BI_Lucy', 'AU_Mary', 'AI_Lucy'])

    @db_session
    def test_2b(self):
        p4 = Person(id=4, name='Bob', age=16)
        p1 = Person[1]  # auto-flush here
        p2 = Person[2]
        self.assertEqual(logged_events, ['BI_Bob', 'AI_Bob'])
        p2.age += 1
        p5 = Person(id=5, name='Lucy', age=23)
        flush_for(p4, p2, p5)
        self.assertEqual(logged_events, ['BI_Bob', 'AI_Bob', 'BU_Mary', 'AU_Mary', 'BI_Lucy', 'AI_Lucy'])

    @db_session
    def test_3(self):
        global do_before_insert
        def do_before_insert(person):
            some_person = Person.select().first()  # should not cause infinite recursion
        p4 = Person(id=4, name='Bob', age=16)
        db.flush()

    @db_session
    def test_4(self):
        global do_before_insert
        def do_before_insert(person):
            some_person = Person.select().first()  # creates nested prefetch_context
        p4 = Person(id=4, name='Bob', age=16)
        Person.select().first()


if __name__ == '__main__':
    unittest.main()
