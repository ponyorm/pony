import unittest
from pony.db import Database
from pony.sqlsymbols import *

class TestSQLAST(unittest.TestCase):
    def setUp(self):
        self.db = Database('sqlite', ':memory:')
        conn = self.db.get_connection()
        conn.executescript("""
        create table if not exists T1(
            a integer primary key,
            b varchar(20) not null
            );
        insert or ignore into T1 values(1, 'abc');
        """)
    def tearDown(self):
        self.db.release()
    def test_alias(self):
        sql_ast = [SELECT, [ALL, [COLUMN, "Group", "a"]],
                           [FROM, ["Group", TABLE, "T1" ]]]
        cursor = self.db._exec_ast(sql_ast)
    def test_alias2(self):
        sql_ast = [SELECT, [ALL, [COLUMN, None, "a"]],
                            [FROM, [None, TABLE, "T1"]]]
        cursor = self.db._exec_ast(sql_ast)

if __name__ == "__main__":
    unittest.main()
  