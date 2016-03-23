'''
Postgres-specific tests
'''

import unittest

from pony.orm import *
from pony.orm.ormtypes import Json
from pony.orm.tests.testutils import raises_exception

from . import SetupTest


class JsonConcatTest(SetupTest, unittest.TestCase):

    @classmethod
    def bindDb(cls):
        cls.db = Database('postgres', user='postgres', password='postgres',
                          database='testjson', host='localhost')

    @db_session
    def setUp(self):
        info = ['description', 4, {'size': '100x50'}]
        self.E(article='A-347', info=info, extra_info={'overpriced': True})

    @db_session
    def test_field(self):
        result = select(m.info[2] | m.extra_info for m in self.M)[:]
        self.assertDictEqual(result[0], {u'overpriced': True, u'size': u'100x50'})

    @db_session
    def test_param(self):
        x = 17
        result = select(m.info[2] | {"weight": x} for m in self.M)[:]
        self.assertEqual(len(result), 1)
        self.assertDictEqual(result[0], {'weight': 17, 'size': '100x50'})

    @db_session
    def test_complex_param(self):
        x = {"weight": {'net': 17}}
        result = select(m.info[2] | x for m in self.M)[:]
        self.assertEqual(len(result), 1)
        self.assertDictEqual(result[0], {'weight': {'net': 17}, 'size': '100x50'})

    @db_session
    def test_complex_param_2(self):
        x = {'net': 17}
        result = select(m.info[2] | {"weight": x} for m in self.M)[:]
        self.assertEqual(len(result), 1)
        self.assertDictEqual(result[0], {'weight': {'net': 17}, 'size': '100x50'})

    @db_session
    def test_str_const(self):
        result = select(m.info[2] | {"weight": 17} for m in self.M)[:]
        self.assertEqual(len(result), 1)
        self.assertDictEqual(result[0], {'weight': 17, 'size': '100x50'})

    @db_session
    def test_str_param(self):
        extra = {"weight": 17}
        result = select(m.info[2] | extra for m in self.M)[:]
        self.assertEqual(len(result), 1)
        self.assertDictEqual(result[0], {'weight': 17, 'size': '100x50'})

    @raises_exception(Exception)
    @db_session
    def test_no_json_wrapper(self):
        result = select(m.info[2] | '{"weight": 17}' for m in self.M)[:]
        self.assertEqual(len(result), 1)
        self.assertDictEqual(result[0], {'weight': 17, 'size': '100x50'})


class JsonContainsTest(SetupTest, unittest.TestCase):

    @classmethod
    def bindDb(cls):
        cls.db = Database('postgres', user='postgres', password='postgres',
                          database='testjson', host='localhost')

    @db_session
    def setUp(self):
        info = ['description', 4, {'size': '100x50'}]
        self.M(article='A-347', info=info, extra_info={'overpriced': True})

    @db_session
    def test_key_in(self):
        result = select('size' in m.info[2] for m in self.M)[:]
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], True)

    @db_session
    def test_contains(self):
        result = select({"size": "100x50"} in m.info[2] for m in self.M)[:]
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], True)

    @db_session
    def test_contains_param(self):
        for size in ['100x50', '200x100']:
            result = select({"size": "%s" % size} in m.info[2] for m in self.M)[:]
            self.assertEqual(len(result), 1)
            self.assertEqual(result[0], size == '100x50')

    @db_session
    def test_list(self):
        result = select(Json(["description"]) in m.info for m in self.M)[:]
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], True)

    @db_session
    def test_contains_field(self):
        result = select({"size": "100x50"} in m.info[2] for m in self.M)[:]
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], True)

    @db_session
    def test_inverse_order(self):
        result = select(m.info[2] in {"size": "100x50", "weight": 1} for m in self.M)[:]
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], True)

    @db_session
    def test_with_concat(self):
        result = select((m.info[2] | {'weight': 1}) in {"size": "100x50", "weight": 1}
                        for m in self.M)[:]
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], True)
