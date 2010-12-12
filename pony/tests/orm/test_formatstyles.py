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
                                'WHERE (("B" = ?) AND ("C" = ?) AND ("D" = ?) AND ("E" = ?))\n')
        self.assertEqual(b.layout, (self.key1, self.key2, self.key2, self.key1))
    def test_numeric(self):
        b = SQLBuilder(self.ast, 'numeric')
        self.assertEqual(b.sql, 'SELECT "A"\n'
                                'FROM "T1"\n'
                                'WHERE (("B" = :1) AND ("C" = :2) AND ("D" = :2) AND ("E" = :1))\n')
        self.assertEqual(b.layout, (self.key1, self.key2))
    def test_named(self):
        b = SQLBuilder(self.ast, 'named')
        self.assertEqual(b.sql, 'SELECT "A"\n'
                                'FROM "T1"\n'
                                'WHERE (("B" = :p1) AND ("C" = :p2) AND ("D" = :p2) AND ("E" = :p1))\n')
        self.assertEqual(b.layout, (self.key1, self.key2))
    def test_format(self):
        b = SQLBuilder(self.ast, 'format')
        self.assertEqual(b.sql, 'SELECT "A"\n'
                                'FROM "T1"\n'
                                'WHERE (("B" = %s) AND ("C" = %s) AND ("D" = %s) AND ("E" = %s))\n')
        self.assertEqual(b.layout, (self.key1, self.key2, self.key2, self.key1))
    def test_pyformat(self):
        b = SQLBuilder(self.ast, 'pyformat')
        self.assertEqual(b.sql, 'SELECT "A"\n'
                                'FROM "T1"\n'
                                'WHERE (("B" = %(p1)s) AND ("C" = %(p2)s) AND ("D" = %(p2)s) AND ("E" = %(p1)s))\n')
        self.assertEqual(b.layout, (self.key1, self.key2))
                         

if __name__ == "__main__":
    unittest.main()