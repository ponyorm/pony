import unittest

from pony.orm.core import *


class TestOnConnect(unittest.TestCase):
    def test_provider_as_first_argument(self):
        db = Database()
        self.assertEqual(db.on_connect("sqlite").provider, "sqlite")

    def test_provider_as_keyword_argument(self):
        db = Database()
        self.assertEqual(db.on_connect(provider="sqlite").provider, "sqlite")

    def test_unknown_provider(self):
        db = Database()
        with self.assertRaisesRegex(BindingError, "Unknown provider foo"):
            db.on_connect("foo")

    def test_functions_are_called_for_matching_providers_only(self):
        db = Database()
        called = []

        @db.on_connect
        def for_any_provider(database, connection):
            called.append("any")

        @db.on_connect("sqlite")
        def for_sqlite(database, connection):
            called.append("sqlite")

        @db.on_connect(provider="sqlite")
        def for_sqlite_keyword(database, connection):
            called.append("sqlite keyword")

        @db.on_connect("postgres")
        def for_postgres(database, connection):
            called.append("postgres")

        db.bind("sqlite", ":memory:")
        db.generate_mapping(check_tables=False)
        with db_session:
            db.execute("select 1")
        self.assertEqual(sorted(called), ["any", "sqlite", "sqlite keyword"])


if __name__ == "__main__":
    unittest.main()
