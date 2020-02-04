from pony.py23compat import basestring, pickle

import unittest

from pony.orm import *
from pony.orm.tests.testutils import raises_exception, raises_if
from pony.orm.ormtypes import Json, TrackedValue, TrackedList, TrackedDict
from pony.orm.tests import setup_database, teardown_database

db = Database()


class Product(db.Entity):
    name = Required(str)
    info = Optional(Json)
    tags = Optional(Json)


class TestJson(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_database(db)

    def setUp(self):
        with db_session:
            Product.select().delete(bulk=True)
            flush()
            Product(
                name='Apple iPad Air 2',
                info={
                    'name': 'Apple iPad Air 2',
                    'display': {
                     'size': 9.7,
                     'resolution': [2048, 1536],
                     'matrix-type': 'IPS',
                     'multi-touch': True
                    },
                    'os': {
                     'type': 'iOS',
                     'version': '8'
                    },
                    'cpu': 'Apple A8X',
                    'ram': '8GB',
                    'colors': ['Gold', 'Silver', 'Space Gray'],
                    'models': [
                     {
                         'name': 'Wi-Fi',
                         'capacity': ['16GB', '64GB'],
                         'height': 240,
                         'width': 169.5,
                         'depth': 6.1,
                         'weight': 437,
                     },
                     {
                         'name': 'Wi-Fi + Cellular',
                         'capacity': ['16GB', '64GB'],
                         'height': 240,
                         'width': 169.5,
                         'depth': 6.1,
                         'weight': 444,
                     },
                    ],
                    'discontinued': False,
                    'videoUrl': None,
                    'non-ascii-attr': u'\u0442\u0435\u0441\u0442'
                },
                tags=['Tablets', 'Apple', 'Retina'])

    @classmethod
    def tearDownClass(cls):
        teardown_database(db)

    def test(self):
        with db_session:
            result = select(p for p in Product)[:]
            self.assertEqual(len(result), 1)
            p = result[0]
            p.info['os']['version'] = '9'
        with db_session:
            result = select(p for p in Product)[:]
            self.assertEqual(len(result), 1)
            p = result[0]
            self.assertEqual(p.info['os']['version'], '9')

    @db_session
    def test_query_int(self):
        val = get(p.info['display']['resolution'][0] for p in Product)
        self.assertEqual(val, 2048)

    @db_session
    def test_query_float(self):
        val = get(p.info['display']['size'] for p in Product)
        self.assertAlmostEqual(val, 9.7)

    @db_session
    def test_query_true(self):
        val = get(p.info['display']['multi-touch'] for p in Product)
        self.assertIs(val, True)

    @db_session
    def test_query_false(self):
        val = get(p.info['discontinued'] for p in Product)
        self.assertIs(val, False)

    @db_session
    def test_query_null(self):
        val = get(p.info['videoUrl'] for p in Product)
        self.assertIs(val, None)

    @db_session
    def test_query_list(self):
        val = get(p.info['colors'] for p in Product)
        self.assertListEqual(val, ['Gold', 'Silver', 'Space Gray'])
        self.assertNotIsInstance(val, TrackedValue)

    @db_session
    def test_query_dict(self):
        val = get(p.info['display'] for p in Product)
        self.assertDictEqual(val, {
            'size': 9.7,
            'resolution': [2048, 1536],
            'matrix-type': 'IPS',
            'multi-touch': True
        })
        self.assertNotIsInstance(val, TrackedValue)

    @db_session
    def test_query_json_field(self):
        val = get(p.info for p in Product)
        self.assertDictEqual(val['display'], {
            'size': 9.7,
            'resolution': [2048, 1536],
            'matrix-type': 'IPS',
            'multi-touch': True
        })
        self.assertNotIsInstance(val['display'], TrackedDict)
        val = get(p.tags for p in Product)
        self.assertListEqual(val, ['Tablets', 'Apple', 'Retina'])
        self.assertNotIsInstance(val, TrackedList)

    @db_session
    def test_get_object(self):
        p = get(p for p in Product)
        self.assertDictEqual(p.info['display'], {
            'size': 9.7,
            'resolution': [2048, 1536],
            'matrix-type': 'IPS',
            'multi-touch': True
        })
        self.assertEqual(p.info['discontinued'], False)
        self.assertEqual(p.info['videoUrl'], None)
        self.assertListEqual(p.tags, ['Tablets', 'Apple', 'Retina'])
        self.assertIsInstance(p.info, TrackedDict)
        self.assertIsInstance(p.info['display'], TrackedDict)
        self.assertIsInstance(p.info['colors'], TrackedList)
        self.assertIsInstance(p.tags, TrackedList)

    def test_set_str(self):
        with db_session:
            p = get(p for p in Product)
            p.info['os']['version'] = '9'
        with db_session:
            p = get(p for p in Product)
            self.assertEqual(p.info['os']['version'], '9')

    def test_set_int(self):
        with db_session:
            p = get(p for p in Product)
            p.info['display']['resolution'][0] += 1
        with db_session:
            p = get(p for p in Product)
            self.assertEqual(p.info['display']['resolution'][0], 2049)

    def test_set_true(self):
        with db_session:
            p = get(p for p in Product)
            p.info['discontinued'] = True
        with db_session:
            p = get(p for p in Product)
            self.assertIs(p.info['discontinued'], True)

    def test_set_false(self):
        with db_session:
            p = get(p for p in Product)
            p.info['display']['multi-touch'] = False
        with db_session:
            p = get(p for p in Product)
            self.assertIs(p.info['display']['multi-touch'], False)

    def test_set_null(self):
        with db_session:
            p = get(p for p in Product)
            p.info['display'] = None
        with db_session:
            p = get(p for p in Product)
            self.assertIs(p.info['display'], None)

    def test_set_list(self):
        with db_session:
            p = get(p for p in Product)
            p.info['colors'] = ['Pink', 'Black']
        with db_session:
            p = get(p for p in Product)
            self.assertListEqual(p.info['colors'], ['Pink', 'Black'])

    def test_list_del(self):
        with db_session:
            p = get(p for p in Product)
            del p.info['colors'][1]
        with db_session:
            p = get(p for p in Product)
            self.assertListEqual(p.info['colors'], ['Gold', 'Space Gray'])

    def test_list_append(self):
        with db_session:
            p = get(p for p in Product)
            p.info['colors'].append('White')
        with db_session:
            p = get(p for p in Product)
            self.assertListEqual(p.info['colors'], ['Gold', 'Silver', 'Space Gray', 'White'])

    def test_list_set_slice(self):
        with db_session:
            p = get(p for p in Product)
            p.info['colors'][1:] = ['White']
        with db_session:
            p = get(p for p in Product)
            self.assertListEqual(p.info['colors'], ['Gold', 'White'])

    def test_list_set_item(self):
        with db_session:
            p = get(p for p in Product)
            p.info['colors'][1] = 'White'
        with db_session:
            p = get(p for p in Product)
            self.assertListEqual(p.info['colors'], ['Gold', 'White', 'Space Gray'])

    def test_set_dict(self):
        with db_session:
            p = get(p for p in Product)
            p.info['display']['resolution'] = {'width': 2048, 'height': 1536}
        with db_session:
            p = get(p for p in Product)
            self.assertDictEqual(p.info['display']['resolution'], {'width': 2048, 'height': 1536})

    def test_dict_del(self):
        with db_session:
            p = get(p for p in Product)
            del p.info['os']['version']
        with db_session:
            p = get(p for p in Product)
            self.assertDictEqual(p.info['os'], {'type': 'iOS'})

    def test_dict_pop(self):
        with db_session:
            p = get(p for p in Product)
            p.info['os'].pop('version')
        with db_session:
            p = get(p for p in Product)
            self.assertDictEqual(p.info['os'], {'type': 'iOS'})

    def test_dict_update(self):
        with db_session:
            p = get(p for p in Product)
            p.info['os'].update(version='9')
        with db_session:
            p = get(p for p in Product)
            self.assertDictEqual(p.info['os'], {'type': 'iOS', 'version': '9'})

    def test_dict_set_item(self):
        with db_session:
            p = get(p for p in Product)
            p.info['os']['version'] = '9'
        with db_session:
            p = get(p for p in Product)
            self.assertDictEqual(p.info['os'], {'type': 'iOS', 'version': '9'})

    @db_session
    def test_set_same_value(self):
        p = get(p for p in Product)
        p.info = p.info

    @db_session
    def test_len(self):
        with raises_if(self, db.provider.dialect == 'Oracle',
                       TranslationError, 'Oracle does not provide `length` function for JSON arrays'):
            val = select(len(p.tags) for p in Product).first()
            self.assertEqual(val, 3)
            val = select(len(p.info['colors']) for p in Product).first()
            self.assertEqual(val, 3)

    @db_session
    def test_equal_str(self):
        p = get(p for p in Product if p.info['name'] == 'Apple iPad Air 2')
        self.assertTrue(p)

    @db_session
    def test_unicode_key(self):
        p = get(p for p in Product if p.info[u'name'] == 'Apple iPad Air 2')
        self.assertTrue(p)

    @db_session
    def test_equal_string_attr(self):
        p = get(p for p in Product if p.info['name'] == p.name)
        self.assertTrue(p)

    @db_session
    def test_equal_param(self):
        x = 'Apple iPad Air 2'
        p = get(p for p in Product if p.name == x)
        self.assertTrue(p)

    @db_session
    def test_composite_param(self):
        with raises_if(self, db.provider.dialect == 'Oracle',
                       TranslationError, "Oracle doesn't allow parameters in JSON paths"):
            key = 'models'
            index = 0
            val = get(p.info[key][index]['name'] for p in Product)
            self.assertEqual(val, 'Wi-Fi')

    @db_session
    def test_composite_param_in_condition(self):
        with raises_if(self, db.provider.dialect == 'Oracle',
                       TranslationError, "Oracle doesn't allow parameters in JSON paths"):
            key = 'models'
            index = 0
            p = get(p for p in Product if p.info[key][index]['name'] == 'Wi-Fi')
            self.assertIsNotNone(p)

    @db_session
    def test_equal_json_1(self):
        with raises_if(self, db.provider.dialect == 'Oracle', TranslationError,
                       "Oracle does not support comparison of json structures: "
                       "p.info['os'] == {'type':'iOS', 'version':'8'}"):
            p = get(p for p in Product if p.info['os'] == {'type': 'iOS', 'version': '8'})
            self.assertTrue(p)

    @db_session
    def test_equal_json_2(self):
        with raises_if(self, db.provider.dialect == 'Oracle', TranslationError,
                       "Oracle does not support comparison of json structures: "
                       "p.info['os'] == Json({'type':'iOS', 'version':'8'})"):
            p = get(p for p in Product if p.info['os'] == Json({'type': 'iOS', 'version': '8'}))
            self.assertTrue(p)

    @db_session
    def test_ne_json_1(self):
        with raises_if(self, db.provider.dialect == 'Oracle', TranslationError,
                       "Oracle does not support comparison of json structures: p.info['os'] != {}"):
            p = get(p for p in Product if p.info['os'] != {})
            self.assertTrue(p)
            p = get(p for p in Product if p.info['os'] != {'type': 'iOS', 'version': '8'})
            self.assertFalse(p)

    @db_session
    def test_ne_json_2(self):
        with raises_if(self, db.provider.dialect == 'Oracle', TranslationError,
                       "Oracle does not support comparison of json structures: p.info['os'] != Json({})"):
            p = get(p for p in Product if p.info['os'] != Json({}))
            self.assertTrue(p)
            p = get(p for p in Product if p.info['os'] != {'type': 'iOS', 'version': '8'})
            self.assertFalse(p)

    @db_session
    def test_equal_list_1(self):
        colors = ['Gold', 'Silver', 'Space Gray']
        with raises_if(self, db.provider.dialect == 'Oracle', TranslationError,
                       "Oracle does not support comparison of json structures: p.info['colors'] == Json(colors)"):
            p = get(p for p in Product if p.info['colors'] == Json(colors))
            self.assertTrue(p)

    @db_session
    @raises_exception(TypeError, "Incomparable types 'Json' and 'list' in expression: p.info['colors'] == ['Gold']")
    def test_equal_list_2(self):
        p = get(p for p in Product if p.info['colors'] == ['Gold'])

    @db_session
    def test_equal_list_3(self):
        with raises_if(self, db.provider.dialect == 'Oracle', TranslationError,
                       "Oracle does not support comparison of json structures: p.info['colors'] != Json(['Gold'])"):
            p = get(p for p in Product if p.info['colors'] != Json(['Gold']))
            self.assertIsNotNone(p)

    @db_session
    def test_equal_list_4(self):
        colors = ['Gold', 'Silver', 'Space Gray']
        with raises_if(self, db.provider.dialect == 'Oracle', TranslationError,
                       "Oracle does not support comparison of json structures: p.info['colors'] == Json(colors)"):
            p = get(p for p in Product if p.info['colors'] == Json(colors))
            self.assertTrue(p)

    @db_session
    @raises_exception(TypeError, "Incomparable types 'Json' and 'list' in expression: p.info['colors'] == []")
    def test_equal_empty_list_1(self):
        p = get(p for p in Product if p.info['colors'] == [])

    @db_session
    def test_equal_empty_list_2(self):
        with raises_if(self, db.provider.dialect == 'Oracle', TranslationError,
                       "Oracle does not support comparison of json structures: p.info['colors'] == Json([])"):
            p = get(p for p in Product if p.info['colors'] == Json([]))
            self.assertIsNone(p)

    @db_session
    def test_ne_list(self):
        with raises_if(self, db.provider.dialect == 'Oracle', TranslationError,
                       "Oracle does not support comparison of json structures: p.info['colors'] != Json(['Gold'])"):
            p = get(p for p in Product if p.info['colors'] != Json(['Gold']))
            self.assertTrue(p)

    @db_session
    def test_ne_empty_list(self):
        with raises_if(self, db.provider.dialect == 'Oracle', TranslationError,
                       "Oracle does not support comparison of json structures: p.info['colors'] != Json([])"):
            p = get(p for p in Product if p.info['colors'] != Json([]))
            self.assertTrue(p)

    @db_session
    def test_dbval2val(self):
        p = select(p for p in Product)[:][0]
        attr = Product.info
        val = p._vals_[attr]
        dbval = p._dbvals_[attr]
        self.assertIsInstance(dbval, basestring)
        self.assertIsInstance(val, TrackedValue)
        p.info['os']['version'] = '9'
        self.assertIs(val, p._vals_[attr])
        self.assertIs(dbval, p._dbvals_[attr])
        p.flush()
        self.assertIs(val, p._vals_[attr])
        self.assertNotEqual(dbval, p._dbvals_[attr])

    @db_session
    def test_wildcard_path_1(self):
        with raises_if(self, db.provider.dialect not in ('Oracle', 'MySQL'),
                       TranslationError, '...does not support wildcards in JSON path...'):
            names = get(p.info['models'][:]['name'] for p in Product)
            self.assertSetEqual(set(names), {'Wi-Fi', 'Wi-Fi + Cellular'})

    @db_session
    def test_wildcard_path_2(self):
        with raises_if(self, db.provider.dialect not in ('Oracle', 'MySQL'),
                       TranslationError, '...does not support wildcards in JSON path...'):
            values = get(p.info['os'][...] for p in Product)
            self.assertSetEqual(set(values), {'iOS', '8'})

    @db_session
    def test_wildcard_path_3(self):
        with raises_if(self, db.provider.dialect not in ('Oracle', 'MySQL'),
                       TranslationError, '...does not support wildcards in JSON path...'):
            names = get(p.info[...][0]['name'] for p in Product)
            self.assertSetEqual(set(names), {'Wi-Fi'})

    @db_session
    def test_wildcard_path_4(self):
        if db.provider.dialect == 'Oracle':
            raise unittest.SkipTest
        with raises_if(self, db.provider.dialect != 'MySQL',
                       TranslationError, '...does not support wildcards in JSON path...'):
            values = get(p.info[...][:][...][:] for p in Product)[:]
            self.assertSetEqual(set(values), {'16GB', '64GB'})

    @db_session
    def test_wildcard_path_with_params(self):
        if db.provider.dialect != 'Oracle':
            exc_msg = '...does not support wildcards in JSON path...'
        else:
            exc_msg = "Oracle doesn't allow parameters in JSON paths"
        with raises_if(self, db.provider.dialect != 'MySQL', TranslationError, exc_msg):
            key = 'models'
            index = 0
            values = get(p.info[key][:]['capacity'][index] for p in Product)
            self.assertListEqual(values, ['16GB', '16GB'])

    @db_session
    def test_wildcard_path_with_params_as_string(self):
        if db.provider.dialect != 'Oracle':
            exc_msg = '...does not support wildcards in JSON path...'
        else:
            exc_msg = "Oracle doesn't allow parameters in JSON paths"
        with raises_if(self, db.provider.dialect != 'MySQL', TranslationError, exc_msg):
            key = 'models'
            index = 0
            values = get("p.info[key][:]['capacity'][index] for p in Product")
            self.assertListEqual(values, ['16GB', '16GB'])

    @db_session
    def test_wildcard_path_in_condition(self):
        errors = {
            'MySQL': 'Wildcards are not allowed in json_contains()',
            'SQLite': '...does not support wildcards in JSON path...',
            'PostgreSQL': '...does not support wildcards in JSON path...'
        }
        dialect = db.provider.dialect
        with raises_if(self, dialect in errors, TranslationError, errors.get(dialect)):
            p = get(p for p in Product if '16GB' in p.info['models'][:]['capacity'])
            self.assertTrue(p)

    ##### 'key' in json

    @db_session
    def test_in_dict(self):
        obj = get(p for p in Product if 'resolution' in p.info['display'])
        self.assertTrue(obj)

    @db_session
    def test_not_in_dict(self):
        obj = get(p for p in Product if 'resolution' not in p.info['display'])
        self.assertIs(obj, None)
        obj = get(p for p in Product if 'xyz' not in p.info['display'])
        self.assertTrue(obj)

    @db_session
    def test_in_list(self):
        obj = get(p for p in Product if 'Gold' in p.info['colors'])
        self.assertTrue(obj)

    @db_session
    def test_not_in_list(self):
        obj = get(p for p in Product if 'White' not in p.info['colors'])
        self.assertTrue(obj)
        obj = get(p for p in Product if 'Gold' not in p.info['colors'])
        self.assertIs(obj, None)

    @db_session
    def test_var_in_json(self):
        with raises_if(self, db.provider.dialect == 'Oracle',
                       TypeError, "For `key in JSON` operation Oracle supports literal key values only, "
                                  "parameters are not allowed: key in p.info['colors']"):
            key = 'Gold'
            obj = get(p for p in Product if key in p.info['colors'])
            self.assertTrue(obj)

    @db_session
    def test_select_first(self):
        # query should not contain ORDER BY
        obj = select(p.info for p in Product).first()
        self.assertNotIn('order by', db.last_sql.lower())

    def test_sql_inject(self):
        # test quote in json is not causing error
        with db_session:
            p = select(p for p in Product).first()
            p.info['display']['size'] = "0' 9.7\""
        with db_session:
            p = select(p for p in Product).first()
            self.assertEqual(p.info['display']['size'], "0' 9.7\"")

    @db_session
    def test_int_compare(self):
        p = get(p for p in Product if p.info['display']['resolution'][0] == 2048)
        self.assertTrue(p)
        p = get(p for p in Product if p.info['display']['resolution'][0] != 2048)
        self.assertIsNone(p)
        p = get(p for p in Product if p.info['display']['resolution'][0] < 2048)
        self.assertIs(p, None)
        p = get(p for p in Product if p.info['display']['resolution'][0] <= 2048)
        self.assertTrue(p)
        p = get(p for p in Product if p.info['display']['resolution'][0] > 2048)
        self.assertIs(p, None)
        p = get(p for p in Product if p.info['display']['resolution'][0] >= 2048)
        self.assertTrue(p)

    @db_session
    def test_float_compare(self):
        p = get(p for p in Product if p.info['display']['size'] > 9.5)
        self.assertTrue(p)
        p = get(p for p in Product if p.info['display']['size'] < 9.8)
        self.assertTrue(p)
        p = get(p for p in Product if p.info['display']['size'] < 9.5)
        self.assertIsNone(p)
        p = get(p for p in Product if p.info['display']['size'] > 9.8)
        self.assertIsNone(p)

    @db_session
    def test_str_compare(self):
        p = get(p for p in Product if p.info['ram'] == '8GB')
        self.assertTrue(p)
        p = get(p for p in Product if p.info['ram'] != '8GB')
        self.assertIsNone(p)
        p = get(p for p in Product if p.info['ram'] < '9GB')
        self.assertTrue(p)
        p = get(p for p in Product if p.info['ram'] > '7GB')
        self.assertTrue(p)
        p = get(p for p in Product if p.info['ram'] > '9GB')
        self.assertIsNone(p)
        p = get(p for p in Product if p.info['ram'] < '7GB')
        self.assertIsNone(p)

    @db_session
    def test_bool_compare(self):
        p = get(p for p in Product if p.info['display']['multi-touch'] == True)
        self.assertTrue(p)
        p = get(p for p in Product if p.info['display']['multi-touch'] is True)
        self.assertTrue(p)
        p = get(p for p in Product if p.info['display']['multi-touch'] == False)
        self.assertIsNone(p)
        p = get(p for p in Product if p.info['display']['multi-touch'] is False)
        self.assertIsNone(p)
        p = get(p for p in Product if p.info['discontinued'] == False)
        self.assertTrue(p)
        p = get(p for p in Product if p.info['discontinued'] == True)
        self.assertIsNone(p)

    @db_session
    def test_none_compare(self):
        p = get(p for p in Product if p.info['videoUrl'] is None)
        self.assertTrue(p)
        p = get(p for p in Product if p.info['videoUrl'] is not None)
        self.assertIsNone(p)

    @db_session
    def test_none_for_nonexistent_path(self):
        p = get(p for p in Product if p.info['some_attr'] is None)
        self.assertTrue(p)

    @db_session
    def test_str_cast(self):
        p = get(coalesce(str(p.name), 'empty') for p in Product)
        last_sql = db.last_sql
        if db.provider.dialect == 'PostgreSQL':
            self.assertTrue(')::text' in last_sql)
        else:
            self.assertTrue('AS text' in db.last_sql)

    @db_session
    def test_int_cast(self):
        p = get(coalesce(int(p.info['os']['version']), 0) for p in Product)
        last_sql = db.last_sql
        if db.provider.dialect == 'PostgreSQL':
            self.assertTrue(')::int' in last_sql)
        else:
            self.assertTrue('as integer' in last_sql)


    def test_nonzero(self):
        with db_session:
            delete(p for p in Product)
            Product(name='P1', info=dict(id=1, val=True))
            Product(name='P2', info=dict(id=2, val=False))
            Product(name='P3', info=dict(id=3, val=0))
            Product(name='P4', info=dict(id=4, val=1))
            Product(name='P5', info=dict(id=5, val=''))
            Product(name='P6', info=dict(id=6, val='x'))
            Product(name='P7', info=dict(id=7, val=[]))
            Product(name='P8', info=dict(id=8, val=[1, 2, 3]))
            Product(name='P9', info=dict(id=9, val={}))
            Product(name='P10', info=dict(id=10, val={'a': 'b'}))
            Product(name='P11', info=dict(id=11))
            Product(name='P12', info=dict(id=12, val='True'))
            Product(name='P13', info=dict(id=13, val='False'))
            Product(name='P14', info=dict(id=14, val='0'))
            Product(name='P15', info=dict(id=15, val='1'))
            Product(name='P16', info=dict(id=16, val='""'))
            Product(name='P17', info=dict(id=17, val='[]'))
            Product(name='P18', info=dict(id=18, val='{}'))

        with db_session:
            val = select(p.info['id'] for p in Product if not p.info['val'])
            self.assertEqual(tuple(sorted(val)), (2, 3, 5, 7, 9, 11))

    @db_session
    def test_optimistic_check(self):
        p1 = Product.select().first()
        p1.info['foo'] = 'bar'
        flush()
        p1.name = 'name2'
        flush()
        p1.name = 'name3'
        flush()

    @db_session
    def test_avg(self):
        result = select(avg(p.info['display']['size']) for p in Product).first()
        self.assertAlmostEqual(result, 9.7)

    def test_pickle(self):
        with db_session:
            p1 = Product.select().first()
            data = pickle.dumps(p1)
        with db_session:
            p1 = pickle.loads(data)
            p1.name = 'name2'
            flush()
            rollback()
