from __future__ import absolute_import, print_function, division

import unittest

from pony.orm.core import *
from pony.orm.tests.testutils import *
from pony.orm.tests import db_params, setup_database, teardown_database


class TestKeys(unittest.TestCase):
    def tearDown(self):
        teardown_database(self.db)

    def test_keys1(self):
        db = self.db = Database(**db_params)
        class Entity1(db.Entity):
            a = PrimaryKey(int)
            b = Required(str)
        self.assertEqual(Entity1._pk_attrs_, (Entity1.a,))
        self.assertEqual(Entity1._pk_is_composite_, False)
        self.assertEqual(Entity1._pk_, Entity1.a)
        self.assertEqual(Entity1._keys_, [])
        self.assertEqual(Entity1._simple_keys_, [])
        self.assertEqual(Entity1._composite_keys_, [])

    def test_keys2(self):
        db = self.db = Database(**db_params)
        class Entity1(db.Entity):
            a = Required(int)
            b = Required(str)
            PrimaryKey(a, b)
        self.assertEqual(Entity1._pk_attrs_, (Entity1.a, Entity1.b))
        self.assertEqual(Entity1._pk_is_composite_, True)
        self.assertEqual(Entity1._pk_, (Entity1.a, Entity1.b))
        self.assertEqual(Entity1._keys_, [])
        self.assertEqual(Entity1._simple_keys_, [])
        self.assertEqual(Entity1._composite_keys_, [])

    @raises_exception(ERDiagramError, 'Only one primary key can be defined in each entity class')
    def test_keys3(self):
        db = self.db = Database(**db_params)
        class Entity1(db.Entity):
            a = PrimaryKey(int)
            b = PrimaryKey(int)

    @raises_exception(ERDiagramError, 'Only one primary key can be defined in each entity class')
    def test_keys4(self):
        db = self.db = Database(**db_params)
        class Entity1(db.Entity):
            a = PrimaryKey(int)
            b = Required(int)
            c = Required(int)
            PrimaryKey(b, c)

    def test_unique1(self):
        db = self.db = Database(**db_params)
        class Entity1(db.Entity):
            a = PrimaryKey(int)
            b = Required(int, unique=True)
        self.assertEqual(Entity1._keys_, [(Entity1.b,)])
        self.assertEqual(Entity1._simple_keys_, [Entity1.b])
        self.assertEqual(Entity1._composite_keys_, [])

    def test_unique2(self):
        db = self.db = Database(**db_params)
        class Entity1(db.Entity):
            a = PrimaryKey(int)
            b = Optional(int, unique=True)
        self.assertEqual(Entity1._keys_, [(Entity1.b,)])
        self.assertEqual(Entity1._simple_keys_, [Entity1.b])
        self.assertEqual(Entity1._composite_keys_, [])

    def test_unique2_1(self):
        db = self.db = Database(**db_params)
        class Entity1(db.Entity):
            a = PrimaryKey(int)
            b = Optional(int)
            c = Required(int)
            composite_key(b, c)
        self.assertEqual(Entity1._keys_, [(Entity1.b, Entity1.c)])
        self.assertEqual(Entity1._simple_keys_, [])
        self.assertEqual(Entity1._composite_keys_, [(Entity1.b, Entity1.c)])

    @raises_exception(TypeError, 'composite_key() must receive at least two attributes as arguments')
    def test_unique3(self):
        db = self.db = Database(**db_params)
        class Entity1(db.Entity):
            a = PrimaryKey(int)
            composite_key()

    @raises_exception(TypeError, 'composite_key() arguments must be attributes. Got: 123')
    def test_unique4(self):
        db = self.db = Database(**db_params)
        class Entity1(db.Entity):
            a = PrimaryKey(int)
            composite_key(123, 456)

    @raises_exception(TypeError, "composite_key() arguments must be attributes. Got: %r" % int)
    def test_unique5(self):
        db = self.db = Database(**db_params)
        class Entity1(db.Entity):
            a = PrimaryKey(int)
            composite_key(int, a)

    @raises_exception(TypeError, 'Set attribute Entity1.b cannot be part of unique index')
    def test_unique6(self):
        db = self.db = Database(**db_params)
        class Entity1(db.Entity):
            a = Required(int)
            b = Set('Entity2')
            composite_key(a, b)

    @raises_exception(TypeError, "'unique' option cannot be set for attribute Entity1.b because it is collection")
    def test_unique7(self):
        db = self.db = Database(**db_params)
        class Entity1(db.Entity):
            a = PrimaryKey(int)
            b = Set('Entity2', unique=True)

    @raises_exception(TypeError, 'Optional attribute Entity1.b cannot be part of primary key')
    def test_unique8(self):
        db = self.db = Database(**db_params)
        class Entity1(db.Entity):
            a = Required(int)
            b = Optional(int)
            PrimaryKey(a, b)

    @raises_exception(TypeError, 'PrimaryKey attribute Entity1.a cannot be of type float')
    def test_float_pk(self):
        db = self.db = Database(**db_params)
        class Entity1(db.Entity):
            a = PrimaryKey(float)

    @raises_exception(TypeError, 'Attribute Entity1.b of type float cannot be part of primary key')
    def test_float_composite_pk(self):
        db = self.db = Database(**db_params)
        class Entity1(db.Entity):
            a = Required(int)
            b = Required(float)
            PrimaryKey(a, b)

    @raises_exception(TypeError, 'Attribute Entity1.b of type float cannot be part of unique index')
    def test_float_composite_key(self):
        db = self.db = Database(**db_params)
        class Entity1(db.Entity):
            a = Required(int)
            b = Required(float)
            composite_key(a, b)

    @raises_exception(TypeError, 'Unique attribute Entity1.a cannot be of type float')
    def test_float_unique(self):
        db = self.db = Database(**db_params)
        class Entity1(db.Entity):
            a = Required(float, unique=True)

    @raises_exception(TypeError, 'PrimaryKey attribute Entity1.a cannot be volatile')
    def test_volatile_pk(self):
        db = self.db = Database(**db_params)
        class Entity1(db.Entity):
            a = PrimaryKey(int, volatile=True)

    @raises_exception(TypeError, 'Volatile attribute Entity1.b cannot be part of primary key')
    def test_volatile_composite_pk(self):
        db = self.db = Database(**db_params)
        class Entity1(db.Entity):
            a = Required(int)
            b = Required(int, volatile=True)
            PrimaryKey(a, b)

    def test_composite_key_update(self):
        db = self.db = Database()
        class Entity1(db.Entity):
            s = Set('Entity3')
        class Entity2(db.Entity):
            s = Set('Entity3')
        class Entity3(db.Entity):
            a = Required(Entity1)
            b = Required(Entity2)
            composite_key(a, b)
        setup_database(db)

        with db_session:
            x = Entity1(id=1)
            y = Entity2(id=1)
            z = Entity3(id=1, a=x, b=y)
        with db_session:
            z = Entity3[1]
            self.assertEqual(z.a, Entity1[1])
            self.assertEqual(z.b, Entity2[1])
        with db_session:
            z = Entity3[1]
            w = Entity1(id=2)
            z.a = w
        with db_session:
            z = Entity3[1]
            self.assertEqual(z.a, Entity1[2])
            self.assertEqual(z.b, Entity2[1])

if __name__ == '__main__':
    unittest.main()
