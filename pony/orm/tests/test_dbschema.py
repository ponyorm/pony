import unittest
from typing import Any

from pony.orm.core import Database, DBSchemaError
from pony.orm.dbschema import DBSchema, Table

NO_CONVERTER: Any = None
EMPTY_TABLE_NAME: Any = ()


class TestQualifiedTableNames(unittest.TestCase):
    def setUp(self):
        db = Database()
        db.bind("sqlite", ":memory:")
        self.provider = db.provider
        assert self.provider is not None
        self.schema = DBSchema(self.provider)

    def test_table_already_exists(self):
        Table(("main", "foo"), self.schema)
        with self.assertRaisesRegex(
            DBSchemaError,
            r"^Table \('main', 'foo'\) already exists in database schema$",
        ):
            Table(("main", "foo"), self.schema)

    def test_table_name_already_in_use(self):
        self.schema.names[("main", "foo")] = Table(("main", "bar"), self.schema)
        with self.assertRaisesRegex(
            DBSchemaError,
            r"^Table \('main', 'foo'\) cannot be created, name is already in use$",
        ):
            Table(("main", "foo"), self.schema)

    def test_primary_key_already_defined(self):
        table = Table(("main", "foo"), self.schema)
        column1 = table.add_column("id1", "INTEGER", NO_CONVERTER, True)
        column2 = table.add_column("id2", "INTEGER", NO_CONVERTER, True)
        table.add_index(None, (column1,), is_pk=True)
        with self.assertRaisesRegex(
            DBSchemaError,
            r"^Primary key for table \('main', 'foo'\) is already defined$",
        ):
            table.add_index(None, (column2,), is_pk=True)

    def test_empty_table_name(self):
        assert self.provider is not None
        with self.assertRaisesRegex(TypeError, r"^Invalid table name: \(\)$"):
            self.provider.split_table_name(EMPTY_TABLE_NAME)


if __name__ == "__main__":
    unittest.main()
