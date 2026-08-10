import asyncio
import os
import sys
import tempfile
import time
import unittest

if sys.version_info < (3, 10):
    raise unittest.SkipTest("pony.orm.asyncio requires Python 3.10 or newer")

import pony.orm.asyncio as orm_async
from pony.orm import Optional, PrimaryKey, Required, Set
from pony.orm.asyncio import (
    AsyncImplicitIOError,
    AsyncLazyLoadError,
    AsyncSessionConcurrencyError,
    Database,
    db_session,
    flush,
)
from pony.orm.asyncio.providers import _AsyncPGPool

try:
    import aiosqlite
except ImportError:
    aiosqlite = None


class TestAsyncPublicAPI(unittest.TestCase):
    def test_exports_match_sync_api_names(self):
        self.assertEqual(Database.__name__, "Database")
        self.assertIs(orm_async.Database, Database)
        self.assertIs(orm_async.db_session, db_session)
        self.assertIn("Database", orm_async.__all__)
        self.assertIn("db_session", orm_async.__all__)
        self.assertNotIn("AsyncDatabase", orm_async.__all__)
        self.assertNotIn("async_db_session", orm_async.__all__)
        self.assertNotIn("AsyncQuery", orm_async.__all__)
        self.assertNotIn("AsyncResult", orm_async.__all__)
        self.assertFalse(hasattr(orm_async, "AsyncDatabase"))
        self.assertFalse(hasattr(orm_async, "async_db_session"))


class TestAsyncPostgresTransport(unittest.IsolatedAsyncioTestCase):
    async def test_materialized_results_and_rowcounts(self):
        class Attribute:
            def __init__(self, name):
                self.name = name

        class Record(tuple):
            def keys(self):
                return ("id", "name")

        class Statement:
            def __init__(self, rows):
                self.rows = rows

            async def fetch(self, *values):
                return self.rows

            def get_attributes(self):
                return (Attribute("id"), Attribute("name"))

        class Connection:
            def __init__(self):
                self.executed = []

            async def prepare(self, sql):
                self.executed.append(("prepare", sql))
                return Statement((Record((1, "one")),))

            async def execute(self, sql, *values):
                self.executed.append(("execute", sql, values))
                return "UPDATE 3"

        pool = _AsyncPGPool(database=None, driver=None, args=(), kwargs={})
        connection = Connection()
        selected = await pool.execute(
            connection,
            "SELECT id, name FROM item WHERE id = %(id)s",
            {"id": 1},
        )
        self.assertEqual(selected.rows, ((1, "one"),))
        self.assertEqual(selected.generated_id, 1)
        self.assertEqual(selected.description[1][0], "name")

        updated = await pool.execute(connection, "UPDATE item SET name = %s", ("two",))
        self.assertEqual(updated.rowcount, 3)
        self.assertEqual(connection.executed[-1][0], "execute")


@unittest.skipUnless(aiosqlite is not None, "aiosqlite is not installed")
class TestAsyncSQLite(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.db = db = Database()

        class Parent(db.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            note = Optional(str, lazy=True)
            children = Set("Child")

        class Child(db.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            parent = Required(Parent)

        self.Parent = Parent
        self.Child = Child
        await db.bind(provider="sqlite", filename=":memory:")
        await db.generate_mapping(create_tables=True)

    async def asyncTearDown(self):
        await self.db.disconnect()

    async def test_crud_queries_aggregates_and_raw_sql(self):
        Parent = self.Parent
        async with db_session:
            parent = Parent(name="Alice", note="one")
            await flush()
            self.assertEqual(parent.id, 1)
            self.assertIs(await Parent.get(id=1), parent)
            self.assertEqual(
                [item.name for item in await Parent.select().fetch()], ["Alice"]
            )
            self.assertEqual(await Parent.select().count(), 1)
            self.assertEqual(await self.db.select("name from Parent"), ["Alice"])

        async with db_session:
            self.assertEqual((await Parent.get(name="Alice")).id, 1)

    async def test_query_magic_methods_never_do_io(self):
        async with db_session:
            query = self.Parent.select()
            operations = (
                lambda: iter(query),
                lambda: len(query),
                lambda: bool(query),
                lambda: query[0],
                lambda: query[:1],
            )
            for operation in operations:
                with self.assertRaises(AsyncImplicitIOError):
                    operation()

    async def test_explicit_lazy_and_collection_loading(self):
        Parent, Child = self.Parent, self.Child
        async with db_session:
            parent = Parent(name="parent", note="lazy")
            Child(name="child", parent=parent)

        async with db_session:
            parent = await Parent.get(name="parent")
            with self.assertRaises(AsyncLazyLoadError):
                _ = parent.note
            await parent.load("note")
            self.assertEqual(parent.note, "lazy")

            children = parent.children
            with self.assertRaises(AsyncLazyLoadError):
                list(children)
            await children.load()
            self.assertEqual([child.name for child in children], ["child"])
            self.assertEqual(await children.count(), 1)

        async with db_session:
            parent = (await Parent.select().prefetch(Parent.children).fetch())[0]
            self.assertEqual([child.name for child in parent.children], ["child"])

    async def test_rollback_nested_session_and_retry(self):
        Parent = self.Parent
        with self.assertRaises(ValueError):
            async with db_session:
                Parent(name="rolled back")
                async with db_session:
                    raise ValueError("stop")

        async with db_session:
            self.assertFalse(await Parent.select(name="rolled back").exists())

        with self.assertRaises(ValueError):
            async with db_session(allowed_exceptions=(ValueError,)):
                Parent(name="allowed")
                raise ValueError("commit this transaction")
        async with db_session:
            self.assertTrue(await Parent.select(name="allowed").exists())

        attempts = 0

        @db_session(retry=1, retry_exceptions=(RuntimeError,))
        async def retrying_operation():
            nonlocal attempts
            attempts += 1
            Parent(name="retry")
            if attempts == 1:
                raise RuntimeError("retry transaction")

        await retrying_operation()
        self.assertEqual(attempts, 2)
        async with db_session:
            self.assertEqual(await Parent.select(name="retry").count(), 1)

    async def test_task_ownership_and_parallel_sessions(self):
        Parent = self.Parent
        async with db_session:
            parent = Parent(name="shared")
            await flush()

            async def inherited_session():
                await Parent.get(id=parent.id)

            with self.assertRaises(AsyncSessionConcurrencyError):
                await asyncio.create_task(inherited_session())

        async def independent_lookup():
            async with db_session:
                return (await Parent.get(name="shared")).id

        self.assertEqual(
            await asyncio.gather(independent_lookup(), independent_lookup()), [1, 1]
        )

    async def test_event_loop_remains_responsive_during_driver_io(self):
        # aiosqlite performs the SQL function in its connection worker.  Pony must
        # not block the event-loop thread while waiting for it.
        await self.db.disconnect()
        self.db = db = Database()

        @db.on_connect(provider="sqlite")
        def add_delay_function(_database, connection):
            return connection.create_function("pony_delay", 1, time.sleep)

        await db.bind(provider="sqlite", filename=":memory:")
        ticks = 0

        async def ticker():
            nonlocal ticks
            for _i in range(5):
                await asyncio.sleep(0.02)
                ticks += 1

        ticker_task = asyncio.create_task(ticker())
        async with db_session:
            await db.select("pony_delay(0.15)")
        await ticker_task
        self.assertEqual(ticks, 5)

    async def test_cancellation_rolls_back_and_releases_connection(self):
        await self.db.disconnect()
        self.db = db = Database()

        @db.on_connect(provider="sqlite")
        def add_delay_function(_database, connection):
            return connection.create_function("pony_delay", 1, time.sleep)

        await db.bind(provider="sqlite", filename=":memory:")

        async def cancelled_query():
            async with db_session:
                owner = asyncio.current_task()
                asyncio.get_running_loop().call_later(0.02, owner.cancel)
                await db.select("pony_delay(0.15)")

        with self.assertRaises(asyncio.CancelledError):
            await cancelled_query()

        async with db_session:
            self.assertEqual(await db.select("1"), [1])

    async def test_sqlite_file_mode_and_async_on_connect(self):
        await self.db.disconnect()
        callback_count = 0
        temporary = tempfile.NamedTemporaryFile(
            suffix=".sqlite", dir=os.getcwd(), delete=False
        )
        filename = temporary.name
        temporary.close()
        try:
            self.db = db = Database()

            @db.on_connect(provider="sqlite")
            async def configure_connection(_database, connection):
                nonlocal callback_count
                callback_count += 1
                await connection.execute("PRAGMA busy_timeout = 1000")

            class Item(db.Entity):
                name = Required(str)

            await db.bind(
                provider="sqlite",
                filename=filename,
                create_db=True,
                max_pool_size=2,
            )
            await db.generate_mapping(create_tables=True)
            async with db_session:
                Item(name="file")
            async with db_session:
                self.assertEqual((await Item.get(name="file")).name, "file")
            self.assertGreaterEqual(callback_count, 1)
        finally:
            await self.db.disconnect()
            self.db = Database()
            os.remove(filename)


try:
    import asyncpg
except ImportError:
    asyncpg = None


@unittest.skipUnless(
    asyncpg is not None and os.environ.get("PONY_ASYNC_POSTGRES_DSN"),
    "PONY_ASYNC_POSTGRES_DSN is not configured",
)
class TestAsyncPostgreSQL(unittest.IsolatedAsyncioTestCase):
    async def test_crud_and_rollback(self):
        db = Database()

        class AsyncPonyTestItem(db.Entity):
            _table_ = "pony_async_test_item"
            name = Required(str)

        await db.bind("postgres", os.environ["PONY_ASYNC_POSTGRES_DSN"])
        try:
            await db.generate_mapping(create_tables=True)
            async with db_session:
                AsyncPonyTestItem(name="postgres")
            async with db_session:
                item = await AsyncPonyTestItem.get(name="postgres")
                self.assertEqual(item.name, "postgres")

            with self.assertRaises(RuntimeError):
                async with db_session:
                    AsyncPonyTestItem(name="rollback")
                    raise RuntimeError("rollback")
            async with db_session:
                self.assertFalse(
                    await AsyncPonyTestItem.select(name="rollback").exists()
                )
            await db.drop_all_tables(with_all_data=True)
        finally:
            await db.disconnect()


if __name__ == "__main__":
    unittest.main()
