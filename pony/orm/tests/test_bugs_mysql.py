"""
Regression tests for MySQL-specific bugs.

These tests currently fail and serve as acceptance criteria for the fixes:
  - BLOB/TEXT columns used as primary keys or in unique indexes (MySQL error 1170)
  - DATE - DATE subtraction using TIMEDIFF instead of DATEDIFF
"""
import sys
import unittest
from datetime import date, timedelta

from pony import orm
from pony.orm import *
from pony.orm.tests import setup_database, teardown_database, db_params


class TestMySQLBlobPrimaryKey(unittest.TestCase):
    """MySQL requires a key-length prefix for BLOB/TEXT columns in indexes (error 1170).
    The ORM should emit the appropriate prefix automatically."""

    def setUp(self):
        self.db = orm.Database()

        class Item(self.db.Entity):
            data = orm.PrimaryKey(orm.buffer)

        setup_database(self.db)

    def tearDown(self):
        teardown_database(self.db)

    def test_buffer_primary_key_roundtrip(self):
        """Insert and retrieve an entity whose primary key is a BLOB."""
        Item = self.db.Item
        buf = orm.buffer(b'hello world')
        with db_session:
            Item(data=buf)
        with db_session:
            item = Item[buf]
            self.assertEqual(item.data, buf)


class TestMySQLBlobUniqueIndex(unittest.TestCase):
    """Same key-length requirement applies when a BLOB/TEXT column has a unique index."""

    def setUp(self):
        self.db = orm.Database()

        class Item(self.db.Entity):
            id = orm.PrimaryKey(int)
            data = orm.Optional(orm.buffer, unique=True)

        setup_database(self.db)

    def tearDown(self):
        teardown_database(self.db)

    def test_buffer_unique_roundtrip(self):
        """Insert two rows with distinct BLOB values and look one up by value."""
        Item = self.db.Item
        buf1 = orm.buffer(b'value1')
        buf2 = orm.buffer(b'value2')
        with db_session:
            Item(id=1, data=buf1)
            Item(id=2, data=buf2)
        with db_session:
            item = Item.get(data=buf1)
            self.assertIsNotNone(item)
            self.assertEqual(item.id, 1)


@unittest.skipIf(sys.version_info >= (3, 13), "Python 3.13 decompiler not supported")
class TestMySQLDateSubtraction(unittest.TestCase):
    """MySQL DATE_DIFF translates to TIMEDIFF, which returns a TIME value.
    Date-minus-date comparisons need DATEDIFF (returns integer days) instead."""

    def setUp(self):
        self.db = orm.Database()

        class Event(self.db.Entity):
            name = orm.Required(str)
            start = orm.Required(date)
            end = orm.Required(date)

        setup_database(self.db)

        with db_session:
            Event(name='short', start=date(2024, 1, 1), end=date(2024, 1,  3))  # 2 days
            Event(name='long',  start=date(2024, 1, 1), end=date(2024, 1, 15))  # 14 days

    def tearDown(self):
        teardown_database(self.db)

    @db_session
    def test_date_diff_greater_than(self):
        Event = self.db.Event
        result = select(e.name for e in Event if e.end - e.start > timedelta(days=7))[:]
        self.assertEqual(result, ['long'])

    @db_session
    def test_date_diff_less_than(self):
        Event = self.db.Event
        result = select(e.name for e in Event if e.end - e.start < timedelta(days=7))[:]
        self.assertEqual(result, ['short'])

    @db_session
    def test_date_diff_equals(self):
        Event = self.db.Event
        result = select(e.name for e in Event if e.end - e.start == timedelta(days=2))[:]
        self.assertEqual(result, ['short'])


if __name__ == '__main__':
    unittest.main()
