from __future__ import absolute_import, print_function, division

import unittest
from pony.orm.sqlsymbols import *
from pony.orm.sqlbuilding import SQLBuilder
from pony.orm.dbapiprovider import DBAPIProvider
from pony.orm.tests.testutils import TestPool


class TestFormatStyles(unittest.TestCase):
    def setUp(self):
        self.key1 = 'KEY1'
        self.key2 = 'KEY2'
        self.provider = DBAPIProvider(pony_pool_mockup=TestPool(None))
        self.ast = [ SELECT, [ ALL, [COLUMN, None, 'A']], [ FROM, [None, TABLE, 'T1']],
                     [ WHERE, [ EQ, [COLUMN, None, 'B'], [ PARAM, self.key1 ] ],
                              [ EQ, [COLUMN, None, 'C'], [ PARAM, self.key2 ] ],
                              [ EQ, [COLUMN, None, 'D'], [ PARAM, self.key2 ] ],
                              [ EQ, [COLUMN, None, 'E'], [ PARAM, self.key1 ] ]
                     ]
                   ]
    def test_qmark(self):
        self.provider.paramstyle = 'qmark'
        b = SQLBuilder(self.provider, self.ast)
        self.assertEqual(b.sql, 'SELECT "A"\n'
                                'FROM "T1"\n'
                                'WHERE "B" = ?\n  AND "C" = ?\n  AND "D" = ?\n  AND "E" = ?')
        self.assertEqual(b.layout, [self.key1, self.key2, self.key2, self.key1])
    def test_numeric(self):
        self.provider.paramstyle = 'numeric'
        b = SQLBuilder(self.provider, self.ast)
        self.assertEqual(b.sql, 'SELECT "A"\n'
                                'FROM "T1"\n'
                                'WHERE "B" = :1\n  AND "C" = :2\n  AND "D" = :2\n  AND "E" = :1')
        self.assertEqual(b.layout, [self.key1, self.key2, self.key2, self.key1])
    def test_named(self):
        self.provider.paramstyle = 'named'
        b = SQLBuilder(self.provider, self.ast)
        self.assertEqual(b.sql, 'SELECT "A"\n'
                                'FROM "T1"\n'
                                'WHERE "B" = :p1\n  AND "C" = :p2\n  AND "D" = :p2\n  AND "E" = :p1')
        self.assertEqual(b.layout, [self.key1, self.key2, self.key2, self.key1])
    def test_format(self):
        self.provider.paramstyle = 'format'
        b = SQLBuilder(self.provider, self.ast)
        self.assertEqual(b.sql, 'SELECT "A"\n'
                                'FROM "T1"\n'
                                'WHERE "B" = %s\n  AND "C" = %s\n  AND "D" = %s\n  AND "E" = %s')
        self.assertEqual(b.layout, [self.key1, self.key2, self.key2, self.key1])
    def test_pyformat(self):
        self.provider.paramstyle = 'pyformat'
        b = SQLBuilder(self.provider, self.ast)
        self.assertEqual(b.sql, 'SELECT "A"\n'
                                'FROM "T1"\n'
                                'WHERE "B" = %(p1)s\n  AND "C" = %(p2)s\n  AND "D" = %(p2)s\n  AND "E" = %(p1)s')
        self.assertEqual(b.layout, [self.key1, self.key2, self.key2, self.key1])


if __name__ == "__main__":
    unittest.main()
