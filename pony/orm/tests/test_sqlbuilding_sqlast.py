from __future__ import with_statement

import unittest
from pony.orm.core import Database, db_session
from pony.orm.sqlsymbols import *

class TestSQLAST(unittest.TestCase):
    def setUp(self):
        self.db = Database('sqlite', ':memory:')
        with db_session:
            conn = self.db.get_connection()
            conn.executescript("""
            create table if not exists T1(
                a integer primary key,
                b varchar(20) not null
                );
            insert or ignore into T1 values(1, 'abc');
            """)
    @db_session
    def test_alias(self):
        sql_ast = [SELECT, [ALL, [COLUMN, "Group", "a"]],
                           [FROM, ["Group", TABLE, "T1" ]]]
        sql, adapter = self.db._ast2sql(sql_ast)
        cursor = self.db._exec_sql(sql)
    @db_session
    def test_alias2(self):
        sql_ast = [SELECT, [ALL, [COLUMN, None, "a"]],
                            [FROM, [None, TABLE, "T1"]]]
        sql, adapter = self.db._ast2sql(sql_ast)
        cursor = self.db._exec_sql(sql)

if __name__ == "__main__":
    unittest.main()
