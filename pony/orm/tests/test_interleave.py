from __future__ import absolute_import, print_function, division

import unittest

from pony.orm.core import *
from pony.orm.tests.testutils import raises_exception
from pony.orm.tests import db_params, only_for

@only_for(providers=['cockroach'])
class TestDiag(unittest.TestCase):
    @raises_exception(TypeError, '`interleave` option cannot be specified for Set attribute Foo.x')
    def test_1(self):
        db = Database()
        class Foo(db.Entity):
            x = Set('Bar', interleave=True)
        class Bar(db.Entity):
            y = Required('Foo')

    @raises_exception(TypeError, "`interleave` option value should be True, False or None. Got: 'yes'")
    def test_2(self):
        db = Database()
        class Foo(db.Entity):
            x = Required('Bar', interleave='yes')
        class Bar(db.Entity):
            y = Set('Foo')

    @raises_exception(TypeError, 'only one attribute may be marked as interleave. Got: Foo.x, Foo.y')
    def test_3(self):
        db = Database()
        class Foo(db.Entity):
            x = Required(int, interleave=True)
            y = Required(int, interleave=True)

    @raises_exception(TypeError, 'Interleave attribute should be part of relationship. Got: Foo.x')
    def test_4(self):
        db = Database()
        class Foo(db.Entity):
            x = Required(int, interleave=True)

    def test_5(self):
        db = Database(**db_params)
        class Bar(db.Entity):
            y = Set('Foo')

        class Foo(db.Entity):
            x = Required('Bar', interleave=True)
            id = Required(int)
            PrimaryKey(x, id)

        db.generate_mapping(create_tables=True)
        s = ') INTERLEAVE IN PARENT "bar" ("x")'
        self.assertIn(s, db.schema.tables['foo'].get_create_command())
        db.drop_all_tables()


if __name__ == '__main__':
    unittest.main()