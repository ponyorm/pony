# Asyncio support

Pony's opt-in asyncio API supports Python 3.10–3.14 and uses `aiosqlite` for
SQLite or `asyncpg` for PostgreSQL. Importing the regular `pony.orm` API does not
import or require either driver.

The ORM compiler, identity map, Unit of Work and hydration run as synchronous
CPU code in the event-loop thread. At each database operation Pony suspends that
stack and awaits the native driver directly. No executor thread is occupied
while PostgreSQL is processing a query.

Install the transport you need (run both commands if the application uses both):

```console
python -m pip install "pony[async-sqlite]"
python -m pip install "pony[async-postgres]"
```

## Basic usage

Connection and mapping initialization are deliberately explicit:

```python
import asyncio

from pony.orm import PrimaryKey, Required
from pony.orm.asyncio import Database, db_session, select

db = Database()


class Person(db.Entity):
    id = PrimaryKey(int, auto=True)
    name = Required(str)


async def main():
    await db.bind(provider="sqlite", filename=":memory:")
    await db.generate_mapping(create_tables=True)

    async with db_session:
        Person(name="Ada")                  # no I/O yet

    async with db_session:
        people = await select(p for p in Person).fetch()
        ada = await Person.get(name="Ada")
        print(people, ada)

    await db.disconnect()


asyncio.run(main())
```

For PostgreSQL, pass normal `asyncpg.create_pool()` connection options:

```python
await db.bind(
    provider="postgres",
    host="localhost",
    database="app",
    user="app",
    password="secret",
    max_pool_size=10,
    max_waiters=40,
    connect_timeout=10,
    acquire_timeout=5,
    query_timeout=30,
    shutdown_timeout=20,
)
```

`pony.orm.asyncio.Database` never accepts connection arguments in its
constructor. SQLite
`:memory:` uses one retained, serialized connection. File databases use a bounded
pool; configure it with `max_pool_size`.

The connection pool is bounded. Once all connections and `max_waiters` queue
slots are occupied, a new operation raises `AsyncPoolQueueFull`. Waiting longer
than `acquire_timeout` raises `AsyncPoolTimeout`, and a query exceeding
`query_timeout` raises `AsyncQueryTimeout`.

## Explicit I/O rules

Query construction stays synchronous. Terminal methods are awaited:

```python
query = Person.select(lambda p: p.name.startswith("A"))
rows = await query.fetch(limit=100)
one = await query.get()
first = await query.first()
present = await query.exists()
count = await query.count()
```

Iteration, `len(query)`, `bool(query)`, indexing, and slicing raise
`AsyncImplicitIOError`; they never hide an I/O operation. Use `fetch(limit=...,
offset=...)`, `first()`, or `exists()` as indicated by the exception.

Already-loaded scalar fields and relationships remain synchronously readable.
Unloaded data must be requested explicitly:

```python
await person.load("biography")
await person.orders.load()
```

Implicit access raises `AsyncLazyLoadError`. `prefetch()` is still a synchronous
query modifier, and its work is included in `await query.fetch()`.

Creating entities and assigning values which are already available stay
synchronous. Flushing, committing, rolling back, deleting entities, and raw SQL
are awaitable:

```python
from pony.orm.asyncio import commit, flush, rollback

await flush()
values = await db.select("name FROM Person")
await commit()
```

## Sessions, retries, and cancellation

`db_session` supports both forms:

```python
async with db_session(serializable=True):
    ...


@db_session(retry=3)
async def update_data():
    ...
```

PostgreSQL transaction characteristics are explicit:

```python
async with db_session(
    isolation="serializable",
    readonly=True,
    deferrable=True,
    transaction_timeout=20,
):
    ...
```

`isolation` accepts `read_committed`, `repeatable_read`, or `serializable`.
`serializable=True` remains a shorthand. A whole-transaction deadline raises
`AsyncTransactionTimeout` after the mandatory rollback has completed.

Nested sessions follow the synchronous transaction compatibility rules. A
session belongs to exactly one `asyncio.Task`. Since `create_task()` copies
context variables, attempting to use an inherited session in the child raises
`AsyncSessionConcurrencyError`; open a new `db_session` in that task.
Independent tasks can use independent sessions concurrently.

On normal exit Pony flushes and commits. Exceptions and cancellation cause a
rollback. Connection cleanup is completed before the original exception or
`CancelledError` is re-raised.

Both synchronous callbacks which return an awaitable and `async def` callbacks
are accepted by `db.on_connect`. The callback receives the native driver
connection and completes before that connection is made available to a session.

## Operations and observability

`await db.disconnect()` stops new acquisitions, waits up to `shutdown_timeout`
for checked-out connections, then terminates remaining PostgreSQL connections or
closes SQLite connections. `await db.health_check()` executes a real query;
`await db.readiness_check()` additionally checks pool admission state.

`db.async_stats` returns query, cancellation, retry, timeout, active-transaction
and pool counters. Pool data includes current occupancy, waiters and cumulative
acquire wait time. `db.recent_queries` contains SQL text and duration, never
parameter values. Configure `slow_query_threshold` in `bind()` for warning logs,
and pass `query_observer=callback` or call `db.add_query_observer(callback)` to
integrate tracing. Observers may be synchronous or asynchronous.
SQL parameter values are also hidden from debug output by default; use
`db_session(sql_debug=True, show_values=True)` only in a safe development
environment when values are genuinely required.

Recommended starting values are one connection per concurrent database-bound
request up to the database's safe connection budget, `max_waiters` between two
and four times the pool size, a short acquire timeout, and a query timeout below
the request deadline. Measure and tune these values for the deployed workload.

SQLite `:memory:` and `:sharedmemory:` deliberately use a single connection.
File-backed SQLite can use a larger pool, but SQLite still serializes writes;
high-contention multi-writer services should use PostgreSQL.

## Supported matrix

| Component | Supported |
| --- | --- |
| Python | 3.10–3.14 |
| greenlet | 3.5.4–3.x |
| aiosqlite | 0.22.1–0.x |
| asyncpg | 0.31–0.x |
| PostgreSQL | 9.5–18 (the asyncpg support range) |
| SQLite | the SQLite library bundled with supported Python versions |

Terminal query results are materialized before hydration. Async server-side
streaming and Flask/Bottle integration are not included. MySQL, Oracle, and
CockroachDB remain available only through the synchronous API.

## Migrating synchronous code

Use a separate `pony.orm.asyncio.Database`; sync and async entities or sessions
cannot be mixed. Typical replacements are:

| Synchronous | Asyncio |
| --- | --- |
| `from pony.orm import Database, db_session` | `from pony.orm.asyncio import Database, db_session` |
| `Database(...)` | `db = Database(); await db.bind(...)` |
| `db.generate_mapping()` | `await db.generate_mapping()` |
| `with db_session:` | `async with db_session:` |
| `list(query)` or `query[:]` | `await query.fetch()` |
| `Entity.get(...)` | `await Entity.get(...)` |
| lazy attribute access | `await entity.load(...)` first |
| `commit()` / `rollback()` | `await commit()` / `await rollback()` |

When both APIs are needed in one module, qualify them through their namespaces:

```python
import pony.orm as orm
import pony.orm.asyncio as aio

sync_db = orm.Database()
async_db = aio.Database()
```
