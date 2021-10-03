from pony.py23compat import PY2

import unittest
from pony.orm.tests.testutils import *
from pony.orm.tests import db_params, setup_database, teardown_database

from pony.orm import *

db = Database()


class Foo(db.Entity):
    id = PrimaryKey(int)
    a = Required(int)
    b = Required(int)
    c = Required(int)
    array1 = Required(IntArray, index=True)
    array2 = Required(FloatArray)
    array3 = Required(StrArray)
    array4 = Optional(IntArray)
    array5 = Optional(IntArray, nullable=True)


class Test(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if db_params['provider'] not in ('sqlite', 'postgres'):
            raise unittest.SkipTest('Arrays are only available for SQLite and PostgreSQL')

        setup_database(db)
        with db_session:
            Foo(id=1, a=1, b=3, c=-2, array1=[10, 20, 30, 40, 50], array2=[1.1, 2.2, 3.3, 4.4, 5.5],
                array3=['foo', 'bar'])

    @classmethod
    def tearDownClass(cls):
        teardown_database(db)

    @db_session
    def test_1(self):
        foo = select(f for f in Foo if 10 in f.array1)[:]
        self.assertEqual([Foo[1]], foo)

    @db_session
    def test_2(self):
        foo = select(f for f in Foo if [10, 20, 50] in f.array1)[:]
        self.assertEqual([Foo[1]], foo)

    @db_session
    def test_2a(self):
        foo = select(f for f in Foo if [] in f.array1)[:]
        self.assertEqual([Foo[1]], foo)

    @db_session
    def test_3(self):
        x = [10, 20, 50]
        foo = select(f for f in Foo if x in f.array1)[:]
        self.assertEqual([Foo[1]], foo)

    @db_session
    def test_4(self):
        foo = select(f for f in Foo if 1.1 in f.array2)[:]
        self.assertEqual([Foo[1]], foo)

    err_msg = "Cannot store 'int' item in array of " + ("'unicode'" if PY2 else "'str'")

    @raises_exception(TypeError, err_msg)
    @db_session
    def test_5(self):
        foo = Foo.select().first()
        foo.array3.append(123)

    @raises_exception(TypeError, err_msg)
    @db_session
    def test_6(self):
        foo = Foo.select().first()
        foo.array3[0] = 123

    @raises_exception(TypeError, err_msg)
    @db_session
    def test_7(self):
        foo = Foo.select().first()
        foo.array3.extend(['str', 123, 'str'])

    @db_session
    def test_8(self):
        foo = Foo.select().first()
        foo.array3.extend(['str1', 'str2'])

    @db_session
    def test_9(self):
        foos = select(f.array2[0] for f in Foo)[:]
        self.assertEqual([1.1], foos)

    @db_session
    def test_10(self):
        foos = select(f.array1[1:-1] for f in Foo)[:]
        self.assertEqual([20, 30, 40], foos[0])

    @db_session
    def test_11(self):
        foo = Foo.select().first()
        foo.array4.append(1)
        self.assertEqual([1], foo.array4)

    @raises_exception(AttributeError, "'NoneType' object has no attribute 'append'")
    @db_session
    def test_12(self):
        foo = Foo.select().first()
        foo.array5.append(1)

    @db_session
    def test_13(self):
        x = [10, 20, 30, 40, 50]
        ids = select(f.id for f in Foo if x == f.array1)[:]
        self.assertEqual(ids, [1])

    @db_session
    def test_14(self):
        val = select(f.array1[0] for f in Foo).first()
        self.assertEqual(val, 10)

    @db_session
    def test_15(self):
        val = select(f.array1[2] for f in Foo).first()
        self.assertEqual(val, 30)

    @db_session
    def test_16(self):
        val = select(f.array1[-1] for f in Foo).first()
        self.assertEqual(val, 50)

    @db_session
    def test_17(self):
        val = select(f.array1[-2] for f in Foo).first()
        self.assertEqual(val, 40)

    @db_session
    def test_18(self):
        x = 2
        val = select(f.array1[x] for f in Foo).first()
        self.assertEqual(val, 30)

    @db_session
    def test_19(self):
        val = select(f.array1[f.a] for f in Foo).first()
        self.assertEqual(val, 20)

    @db_session
    def test_20(self):
        val = select(f.array1[f.c] for f in Foo).first()
        self.assertEqual(val, 40)

    @db_session
    def test_21(self):
        array = select(f.array1[2:4] for f in Foo).first()
        self.assertEqual(array, [30, 40])

    @db_session
    def test_22(self):
        array = select(f.array1[1:-2] for f in Foo).first()
        self.assertEqual(array, [20, 30])

    @db_session
    def test_23(self):
        array = select(f.array1[10:-10] for f in Foo).first()
        self.assertEqual(array, [])

    @db_session
    def test_24(self):
        x = 2
        array = select(f.array1[x:4] for f in Foo).first()
        self.assertEqual(array, [30, 40])

    @db_session
    def test_25(self):
        y = 4
        array = select(f.array1[2:y] for f in Foo).first()
        self.assertEqual(array, [30, 40])

    @db_session
    def test_26(self):
        x, y = 2, 4
        array = select(f.array1[x:y] for f in Foo).first()
        self.assertEqual(array, [30, 40])

    @db_session
    def test_27(self):
        x, y = 1, -2
        array = select(f.array1[x:y] for f in Foo).first()
        self.assertEqual(array, [20, 30])

    @db_session
    def test_28(self):
        x = 1
        array = select(f.array1[x:f.b] for f in Foo).first()
        self.assertEqual(array, [20, 30])

    @db_session
    def test_29(self):
        array = select(f.array1[f.a:f.c] for f in Foo).first()
        self.assertEqual(array, [20, 30])

    @db_session
    def test_30(self):
        array = select(f.array1[:3] for f in Foo).first()
        self.assertEqual(array, [10, 20, 30])

    @db_session
    def test_31(self):
        array = select(f.array1[2:] for f in Foo).first()
        self.assertEqual(array, [30, 40, 50])

    @db_session
    def test_32(self):
        array = select(f.array1[:f.b] for f in Foo).first()
        self.assertEqual(array, [10, 20, 30])

    @db_session
    def test_33(self):
        array = select(f.array1[:f.c] for f in Foo).first()
        self.assertEqual(array, [10, 20, 30])

    @db_session
    def test_34(self):
        array = select(f.array1[f.c:] for f in Foo).first()
        self.assertEqual(array, [40, 50])

    @db_session
    def test_35(self):
        foo = Foo.select().first()
        self.assertTrue(10 in foo.array1)
        self.assertTrue(1000 not in foo.array1)
        self.assertTrue([10, 20] in foo.array1)
        self.assertTrue([20, 10] in foo.array1)
        self.assertTrue([10, 1000] not in foo.array1)
        self.assertTrue([] in foo.array1)
        self.assertTrue('bar' in foo.array3)
        self.assertTrue('baz' not in foo.array3)
        self.assertTrue(['foo', 'bar'] in foo.array3)
        self.assertTrue(['bar', 'foo'] in foo.array3)
        self.assertTrue(['baz', 'bar'] not in foo.array3)
        self.assertTrue([] in foo.array3)

    @db_session
    def test_36(self):
        items = []
        result = select(foo for foo in Foo if foo in items)[:]
        self.assertEqual(result, [])

    @db_session
    def test_37(self):
        f1 = Foo[1]
        items = [f1]
        result = select(foo for foo in Foo if foo in items)[:]
        self.assertEqual(result, [f1])

    @db_session
    def test_38(self):
        items = []
        result = select(foo for foo in Foo if foo.id in items)[:]
        self.assertEqual(result, [])

    @db_session
    def test_39(self):
        items = [1]
        result = select(foo.id for foo in Foo if foo.id in items)[:]
        self.assertEqual(result, [1])
