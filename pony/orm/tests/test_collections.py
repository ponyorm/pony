from __future__ import absolute_import, print_function, division
from pony.py23compat import PY2

import unittest

from pony.orm.tests.testutils import raises_exception
from pony.orm.tests.model1 import *
from pony.orm.tests import setup_database, teardown_database


class TestCollections(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_database(db)
        populate_db()

    @classmethod
    def tearDownClass(cls):
        db.drop_all_tables(with_all_data=True)

    @db_session
    def test_setwrapper_len(self):
        g = Group.get(number='4145')
        self.assertTrue(len(g.students) == 3)

    @db_session
    def test_setwrapper_nonzero(self):
        g = Group.get(number='4145')
        self.assertTrue(bool(g.students) == True)
        self.assertTrue(len(g.students) == 3)

    @db_session
    @raises_exception(TypeError, 'Collection attribute Group.students cannot be specified as search criteria')
    def test_get_by_collection_error(self):
        Group.get(students=[])

    @db_session
    def test_collection_create_one2many_1(self):
        g = Group['3132']
        g.students.create(record=106, name='Mike', scholarship=200)
        flush()
        self.assertEqual(len(g.students), 3)
        rollback()

    @raises_exception(TypeError, "When using Group.students.create(), "
                                 "'group' attribute should not be passed explicitly")
    @db_session
    def test_collection_create_one2many_2(self):
        g = Group['3132']
        g.students.create(record=106, name='Mike', scholarship=200, group=g)

    @raises_exception(TransactionIntegrityError, "Object Student[105] cannot be stored in the database...")
    @db_session
    def test_collection_create_one2many_3(self):
        g = Group['3132']
        g.students.create(record=105, name='Mike', scholarship=200)

    @db_session
    def test_collection_create_many2many_1(self):
        g = Group['3132']
        g.subjects.create(name='Biology')
        flush()
        self.assertEqual(len(g.subjects), 3)
        rollback()

    @raises_exception(TypeError, "When using Group.subjects.create(), "
                                 "'groups' attribute should not be passed explicitly")
    @db_session
    def test_collection_create_many2many_2(self):
        g = Group['3132']
        g.subjects.create(name='Biology', groups=[g])

    @raises_exception(TransactionIntegrityError,
                      "Object Subject[u'Math'] cannot be stored in the database..." if PY2 else
                      "Object Subject['Math'] cannot be stored in the database...")
    @db_session
    def test_collection_create_many2many_3(self):
        g = Group['3132']
        g.subjects.create(name='Math')

# replace collection items when the old ones are not fully loaded
##>>> from pony.examples.orm.students01.model import *
##>>> s1 = Student[101]
##>>> g = s1.group
##>>> g.__dict__[Group.students].is_fully_loaded
##False
##>>> s2 = Student[104]
##>>> g.students = [s2]
##>>>

# replace collection items when the old ones are not loaded
##>>> from pony.examples.orm.students01.model import *
##>>> g = Group[4145]
##>>> Group.students not in g.__dict__
##True
##>>> s2 = Student[104]
##>>> g.students = [s2]


if __name__ == '__main__':
    unittest.main()
