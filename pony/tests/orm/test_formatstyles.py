import unittest
from pony.sqlsymbols import *
from pony.dbapiprovider import SQLBuilder

class TestFormatStyles(unittest.TestCase):
    def setUp(self):
        self.key1 = object()
        self.key2 = object()
        self.ast = [ SELECT, [ ALL, [COLUMN, None, 'A']], [ FROM, [None, TABLE, 'T1']],
                     [ WHERE, [ AND, [ EQ, [COLUMN, None, 'B'], [ PARAM, self.key1 ] ],
                                     [ EQ, [COLUMN, None, 'C'], [ PARAM, self.key2 ] ],
                                     [ EQ, [COLUMN, None, 'D'], [ PARAM, self.key2 ] ],
                                     [ EQ, [COLUMN, None, 'E'], [ PARAM, self.key1 ] ]
                                ]]]
    def test_qmark(self):
        b = SQLBuilder(self.ast, 'qmark')
        self.assertEqual(b.sql, 'SELECT "A"\n'
                                'FROM "T1"\n'
                                'WHERE ("B" = ?)\n  AND ("C" = ?)\n  AND ("D" = ?)\n  AND ("E" = ?)\n')
        self.assertEqual(b.layout, (self.key1, self.key2, self.key2, self.key1))
    def test_numeric(self):
        b = SQLBuilder(self.ast, 'numeric')
        self.assertEqual(b.sql, 'SELECT "A"\n'
                                'FROM "T1"\n'
                                'WHERE ("B" = :1)\n  AND ("C" = :2)\n  AND ("D" = :2)\n  AND ("E" = :1)\n')
        self.assertEqual(b.layout, (self.key1, self.key2))
    def test_named(self):
        b = SQLBuilder(self.ast, 'named')
        self.assertEqual(b.sql, 'SELECT "A"\n'
                                'FROM "T1"\n'
                                'WHERE ("B" = :p1)\n  AND ("C" = :p2)\n  AND ("D" = :p2)\n  AND ("E" = :p1)\n')
        self.assertEqual(b.layout, (self.key1, self.key2))
    def test_format(self):
        b = SQLBuilder(self.ast, 'format')
        self.assertEqual(b.sql, 'SELECT "A"\n'
                                'FROM "T1"\n'
                                'WHERE ("B" = %s)\n  AND ("C" = %s)\n  AND ("D" = %s)\n  AND ("E" = %s)\n')
        self.assertEqual(b.layout, (self.key1, self.key2, self.key2, self.key1))
    def test_pyformat(self):
        b = SQLBuilder(self.ast, 'pyformat')
        self.assertEqual(b.sql, 'SELECT "A"\n'
                                'FROM "T1"\n'
                                'WHERE ("B" = %(p1)s)\n  AND ("C" = %(p2)s)\n  AND ("D" = %(p2)s)\n  AND ("E" = %(p1)s)\n')
        self.assertEqual(b.layout, (self.key1, self.key2))
                         

if __name__ == "__main__":
    unittest.main()