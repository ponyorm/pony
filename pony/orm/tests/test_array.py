from pony.py23compat import PY2

import unittest
from pony.orm.tests.testutils import *

from pony.orm import *

db = Database('sqlite', ':memory:')

class Foo(db.Entity):
    array1 = Required(IntArray, index=True)
    array2 = Required(FloatArray)
    array3 = Required(StrArray)
    array4 = Optional(IntArray)
    array5 = Optional(IntArray, nullable=True)

db.generate_mapping(create_tables=True)


with db_session:
    Foo(array1=[1, 2, 3, 4, 5], array2=[1.1, 2.2, 3.3, 4.4, 5.5], array3=['foo', 'bar'])

class Test(unittest.TestCase):
    @db_session
    def test_1(self):
        foo = select(f for f in Foo if 1 in f.array1)[:]
        self.assertEqual([Foo[1]], foo)

    @db_session
    def test_2(self):
        foo = select(f for f in Foo if [1, 2, 5] in f.array1)[:]
        self.assertEqual([Foo[1]], foo)

    @db_session
    def test_3(self):
        x = [1, 2, 5]
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
        self.assertEqual([2, 3, 4], foos[0])

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
