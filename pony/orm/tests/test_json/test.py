# *uses fixtures*

import unittest

from pony.orm import *
from pony.orm.tests.testutils import raises_exception
from pony.orm.ormtypes import Json, TrackedValue, TrackedList, TrackedDict

from contextlib import contextmanager

import pony.fixtures
from ponytest import with_cli_args



def no_json1_fixture(cls):
    if cls.db_provider != 'sqlite':
        raise unittest.SkipTest

    cls.no_json1 = True

    @contextmanager
    def mgr():
        json1_available = cls.db.provider.json1_available
        cls.db.provider.json1_available = False
        try:
            yield
        finally:
            cls.db.provider.json1_available = json1_available

    return mgr()


no_json1_fixture.class_scoped = True

import click


@contextmanager
def empty_mgr(*args, **kw):
    yield


@with_cli_args
@click.option('--json1', flag_value=True, default=None)
@click.option('--no-json1', 'json1', flag_value=False)
def json1_cli(json1):
    if json1 is None or json1 is True:
        yield empty_mgr
    if json1 is None or json1 is False:
        yield no_json1_fixture



class JsonTest(unittest.TestCase):
    in_db_session = False

    @classmethod
    def make_entities(cls):
        class E(cls.db.Entity):
            article = Required(str)
            info = Optional(ormtypes.Json)
            extra_info = Optional(ormtypes.Json)
            zero = Optional(int)
            DESCRIPTION = Optional(str, default='description')

        class F(cls.db.Entity):
            info = Optional(ormtypes.Json)

        cls.M = cls.E = cls.db.E

    from ponytest import pony_fixtures
    pony_fixtures = list(pony_fixtures) + [json1_cli]

    @db_session
    def setUp(self):
        self.db.execute('delete from %s' % self.db.E._table_)
        self.db.execute('delete from %s' % self.db.F._table_)

        info = [
            'description',
            4,
            {'size': '100x50'},
            ['item1', 'item2', 'smth', 'else'],
        ]
        extra_info = {'info': ['warranty 1 year', '2 weeks testing']}
        self.db.E(article='A-347', info=info, extra_info=extra_info)


    def test_int(self):
        Merchandise = self.M
        with db_session:
            qs = select(b.info[1] for b in Merchandise)[:]
            self.assertEqual(qs[0], 4)

    def test(self):
        Merchandise = self.M
        with db_session:
            qs = select(b for b in Merchandise)[:]
            self.assertEqual(len(qs), 1)
            o = qs[0]
            o.info[2]['weight'] = '3 kg'
        with db_session:
            qs = select(b for b in Merchandise)[:]
            self.assertEqual(len(qs), 1)
            o = qs[0]
            self.assertEqual(o.info[2]['weight'], '3 kg')

    def test_sqlite_sql_inject(self):
        # py_json_extract
        with db_session:
            o = select(m for m in self.M).first()
            o.info = {'text' : "3 ' kg"}
        with db_session:
            o = select(m.info['text'] for m in self.M).first()
            # test quote in json is not causing error

    def test_set_list(self):
        with db_session:
            qs = select(m for m in self.M)[:]
            self.assertEqual(len(qs), 1)
            o = qs[0]
            o.info[2] = ['some', 'list']
        with db_session:
            val = select(m.info[2] for m in self.M).first()
            self.assertListEqual(val, ['some', 'list'])

    def test_getitem_int(self):
        Merchandise = self.M
        with db_session:
            qs = select(b.info[0] for b in Merchandise)[:]
            self.assertEqual(qs[0], 'description')

    def test_getitem_str(self):
        Merchandise = self.M
        with db_session:
            qs = select(b.info[2]['size'] for b in Merchandise)[:]
            self.assertEqual(qs[0], '100x50')

    @db_session
    def test_delete_str(self):
        if self.db_provider == 'oracle' or getattr(self, 'no_json1', False):
            raise unittest.SkipTest
        def g():
            for m in self.M:
                yield m.info[2] - 'size'
        val = select(g()).first()
        self.assertDictEqual(val, {})

    @raises_exception(TypeError) # only constants are supported
    @db_session
    def test_delete_field(self):
        if self.db_provider == 'oracle' or getattr(self, 'no_json1', False):
            raise unittest.SkipTest
        qs = select(m.info - m.zero for m in self.M)[:]
        self.assertEqual(len(qs), 1)
        val = qs[0]
        self.assertEqual(val[0], 4)

    @db_session
    def test_delete_path(self):
        if self.db_provider== 'oracle' or getattr(self, 'no_json1', False):
            raise unittest.SkipTest
        val = select(m.info - [2, 'size'] for m in self.M).first()
        self.assertDictEqual(val[2], {})

    # JSON length

    @db_session
    def test_len(self):
        if self.db_provider == 'oracle' or getattr(self, 'no_json1', False):
            raise unittest.SkipTest
        g = (len(m.info) for m in self.M)
        val = select(g).first()
        self.assertEqual(val, 4)

    @db_session
    def test_item_len(self):
        if self.db_provider == 'oracle' or getattr(self, 'no_json1', False):
            raise unittest.SkipTest
        g = (len(m.info[3]) for m in self.M)
        val = select(g).first()
        self.assertEqual(val, 4)

    # Tracked attribute

    def test_tracked_attr(self):
        with db_session:
            val = select(m for m in self.M).first()
            val.info = val.extra_info['info']
            self.assertIsInstance(val.info, TrackedValue)
        with db_session:
            o = select(m for m in self.M).first()
            self.assertListEqual(o.info, o.extra_info['info'])

    @db_session
    def test_tracked_attr_type(self):
        val = select(m.extra_info['info'] for m in self.M).first()
        self.assertEqual(type(val), list)
        o = select(m for m in self.M).first()
        self.assertEqual(type(o.extra_info), TrackedDict)
        self.assertEqual(type(o.extra_info['info']), TrackedList)

    def test_tracked_del(self):
        with db_session:
            d = select(m for m in self.M).first()
            del d.info[2]['size']
        with db_session:
            d = select(m.info[2] for m in self.M).first()
            self.assertDictEqual(d, {})

    # # Json equality

    @db_session
    def test_equal_str(self):
        g = (m.info[1] for m in self.M if m.info[0] == 'description')
        val = select(g).first()
        self.assertTrue(val)

    @db_session
    def test_equal_string_attr(self):
        if self.db_provider == 'oracle':
            raise unittest.SkipTest
        g = (m.info[1] for m in self.M if m.info[0] == m.DESCRIPTION)
        val = select(g).first()
        self.assertTrue(val)

    @db_session
    def test_equal_param(self):
        if self.db_provider == 'oracle':
            raise unittest.SkipTest
        x = 'description'
        g = (m.info[1] for m in self.M if m.info[0] == x)
        val = select(g).first()
        self.assertTrue(val)

    @db_session
    def test_computed_param(self):
        index = 2
        key = 'size'
        qs = select(b.info[index][key] for b in self.db.E)[:]
        self.assertEqual(qs[0], '100x50')


    @db_session
    def test_equal_json(self):
        if self.db_provider == 'oracle':
            raise unittest.SkipTest
        g = (m.info[2] for m in self.M if m.info[2] == {"size":"100x50"})
        val = select(g).first()
        self.assertTrue(val)

    @db_session
    def test_ne_json(self):
        if self.db_provider == 'oracle':
            raise unittest.SkipTest
        g = (m.info[2] for m in self.M if m.info[2] != {"size":"200x50"})
        val = select(g).first()
        self.assertTrue(val)
        g = (m.info[2] for m in self.M if m.info[2] != {"size":"100x50"})
        val = select(g).first()
        self.assertFalse(val)

    def test_equal_attr(self):
        if self.db_provider == 'oracle':
            raise unittest.SkipTest
        with db_session:
            e = select(e for e in self.db.E).first()
            f = self.db.F(info=e.info[2])
        with db_session:
            g = (e.info[2]
                for e in self.db.E for f in self.db.F
                if e.info[2] == f.info)
            val = select(g).first()
            self.assertTrue(val)

    @db_session
    def test_equal_list(self):
        if self.db_provider == 'oracle':
            raise unittest.SkipTest
        li = ['item1', 'item2', 'smth', 'else']
        self.assertTrue(
            get(m for m in self.M if m.info[3] == Json(li))
        )

    @db_session
    def test_dbval2val(self):
        with db_session:
            obj = select(e for e in self.E)[:][0]
            self.assertIsInstance(obj.info, TrackedValue)
            obj.info[3][0] = 'trash'
        with db_session:
            obj = select(e for e in self.E)[:][0]
            dbval = obj._dbvals_[self.E.info]
            val = obj._vals_[self.E.info]
            self.assertIn('trash', str(dbval))
            self.assertIsInstance(dbval, str)
            self.assertIsInstance(val, TrackedValue)

    @db_session
    def test_starred_path1(self):
        if self.db_provider not in ['mysql', 'oracle']:
            raise unittest.SkipTest('* in path is not supported by %s' % self.db_provider)
        g = select(e.info[:][...] for e in self.E)
        for val in g[:]:
            self.assertListEqual(val, ['100x50'])

    @db_session
    def test_starred_gen_as_string(self):
        if self.db_provider not in ['mysql', 'oracle']:
            raise unittest.SkipTest('* in path is not supported by %s' % self.db_provider)
        g = select('e.info[:][...] for e in self.E')
        for val in g[:]:
            self.assertListEqual(val, ['100x50'])

    @db_session
    def test_starred_path2(self):
        if self.db_provider not in ['mysql', 'oracle']:
            raise unittest.SkipTest('* in path is not supported by %s' % self.db_provider)
        g = select(e.extra_info[...][0] for e in self.E)
        for val in g[:]:
            self.assertListEqual(val, ['warranty 1 year'])

    ##### 'key' in json

    @db_session
    def test_in_dict(self):
        obj = select(
            m.info[2]['size'] for m in self.M if 'size' in m.info[2]
        ).first()
        self.assertTrue(obj)

    @db_session
    def test_not_in_dict(self):
        obj = select(
            m.info for m in self.M if 'size' not in m.info[2]
        ).first()
        self.assertEqual(obj, None)
        obj = select(
            m.info for m in self.M if 'siz' not in m.info[2]
        ).first()
        self.assertTrue(obj)

    @db_session
    def test_in_list(self):
        obj = select(
            m.info[3] for m in self.M if 'item1' in m.info[3]
        ).first()
        self.assertTrue(obj)
        obj = select(
            m.info for m in self.M if 'description' in m.info
        ).first()
        self.assertTrue(obj)

    @db_session
    def test_not_in_list(self):
        obj = select(
            m.info[3] for m in self.M if 'item1' not in m.info[3]
        ).first()
        self.assertEqual(obj, None)
        obj = select(
            m.info[3] for m in self.M if 'ite' not in m.info[3]
        ).first()
        self.assertIn('item1', obj)

    @db_session
    def test_var_in_json(self):
        if self.db_provider in ('mysql', 'oracle'):
            if_implemented = lambda: self.assertRaises(NotImplementedError)
        else:
            @contextmanager
            def if_implemented():
                yield
        with if_implemented():
            key = 'item1'
            obj = select(
                m.info[3] for m in self.M if key in m.info[3]
            ).first()
            self.assertTrue(obj)

    @db_session
    def test_get_json_attr(self):
        ''' query should not contain distinct
        '''
        if self.db_provider != 'oracle':
            raise unittest.SkipTest
        obj = get(
            m.info for m in self.M
        )
        self.assertTrue(obj)

    @db_session
    def test_select_first(self):
        ''' query shoud not contain ORDER BY
        '''
        if self.db_provider != 'oracle':
            raise unittest.SkipTest
        obj = select(
            m.info for m in self.M
        ).first()
        self.assertTrue(obj)

    def test_in_json_regexp(self):
        if self.db_provider != 'oracle':
            raise unittest.SkipTest
        import re
        from pony.orm.dbproviders.oracle import search_in_json_list_regexp
        regexp = search_in_json_list_regexp('item')
        pos = [
            '["item"]',
            '[0, "item"]',
            '[{}, "item", []]',
            '[{"a": 1}, "item", []]',
            '[false, "item", "erg"]',
        ]
        for s in pos:
            self.assertTrue(re.search(regexp, s))
        neg = [
            '[["item"]]',
            '[{"item": 0]]',
            '["1 item", "item 1"]',
            '[0, " "]',
            '[]',
        ]
        for s in neg:
            self.assertFalse(re.search(regexp, s))


class TestDataTypes(unittest.TestCase):

    in_db_session = False

    from ponytest import pony_fixtures
    pony_fixtures = list(pony_fixtures) + [json1_cli]

    @classmethod
    def make_entities(cls):
        class Data(cls.db.Entity):
            data = Optional(Json)

    @db_session
    def setUp(self):
        self.db.execute('delete from %s' % self.db.Data._table_)


    def test_int(self):

        db = self.db
        with db_session:
            db.Data(data={'val': 1})

        with db_session:
            obj = get(d for d in db.Data if d.data['val'] == 1)
            self.assertEqual(obj.data['val'], 1)

    def test_compare_int(self):
        db = self.db
        with db_session:
            db.Data(data={'val': 3})

        with db_session:
            self.assertTrue(
                get(d for d in db.Data if d.data['val'] > 2)
            )
            self.assertTrue(
                get(d for d in db.Data if d.data['val'] < 4)
            )

    def test_str(self):
        db = self.db
        with db_session:
            db.Data(data={'val': "1"})

        with db_session:
            obj = get(d for d in db.Data if d.data['val'] == '1')
            self.assertTrue(obj)

    def test_none(self):
        db = self.db
        with db_session:
            db.Data()

        with db_session:
            data = get(d for d in db.Data if d.data is None)
            self.assertTrue(data)

    # def test_is_null(self):
    #     db = self.db
    #     with db_session:
    #         db.Data(data={'val': None})

    #     with db_session:
    #         data = get(d for d in db.Data if d.data['val'] is None)
    #         self.assertTrue(data)

    # def test_eq_null(self):
    #     db = self.db
    #     with db_session:
    #         db.Data(data={'val': None})

    #     with db_session:
    #         data = get(d for d in db.Data if d.data['val'] == None)
    #         self.assertTrue(data)

    def test_bool(self):
        with db_session:
            self.db.Data(data={'val': True, 'id': 1})
            self.db.Data(data={'val': False, 'id': 2})

        with db_session:
            val = get(
                d.data['id'] for d in self.db.Data
                if d.data['val'] == False
            )
            self.assertEqual(val, 2)
            val = get(
                d.data['id'] for d in self.db.Data
                if d.data['val'] == True
            )
            self.assertEqual(val, 1)

    def test_nonzero(self):
        with db_session:
            self.db.Data(data={'val': True, 'id': 1})
            self.db.Data(data={'val': False, 'id': 2})
            self.db.Data(data={'val': 0, 'id': 3})
            self.db.Data(data={'val': '', 'id': 4})
            self.db.Data(data={'id': 5})

        if self.db_provider == 'oracle':
            assert_raises = lambda: self.assertRaises(NotImplementedError)
        else:
            @contextmanager
            def assert_raises():
                yield

        with db_session, assert_raises():
            val = get(
                d.data['id'] for d in self.db.Data
                if d.data['val']
            )
            self.assertEqual(val, 1)


    def test_float(self):
        with db_session:
            self.db.Data(data={'val': 3.14})

        with db_session:
            val = get(d.data['val'] for d in self.db.Data)
            self.assertIsInstance(val, float)

    def test_compare_float(self):
        with db_session:
            self.db.Data(data={'val': 3.14})
        with db_session:
            val = get(
                d.data['val'] for d in self.db.Data
                if d.data['val'] < 3.15
            )
            self.assertIsInstance(val, float)



# from ._postgres import JsonConcatTest #, JsonContainsTest # TODO


class TestSqliteFallback(unittest.TestCase):

    from ponytest import pony_fixtures
    pony_fixtures = list(pony_fixtures) + [
        [no_json1_fixture]
    ]

    @classmethod
    def make_entities(cls):
        class Person(cls.db.Entity):
            name = Required(str)
            data = Optional(Json)


    def setUp(self):
        self.db.execute('delete from %s' % self.db.Person._table_)


    def test(self):
        Person = self.db.Person
        with db_session:
            Person(name='John')
            Person(name='Mike', data=dict(a=1,b=2))
        with db_session:
            p = Person[1]
            p.data = dict(c=[2, 3, 4], d='d')
            p = Person[2]
            p.data['c'] = [1, 2, 3]

            qs = select(p for p in Person if p.data['c'][1] == 2)
            self.assertEqual(qs.count(), 1)


    def test_cmp(self):
        Person = self.db.Person
        with db_session:
            Person(name='Mike', data=[4])
        with db_session:
            qs = select(p for p in Person if p.data[0] < 5)
            self.assertEqual(qs.count(), 1)
            qs = select(p for p in Person if p.data[0] > 3)
            self.assertEqual(qs.count(), 1)