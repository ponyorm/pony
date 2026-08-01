"""Small standalone example for Pony's asyncio API."""

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
        Person(name="Ada")
        Person(name="Grace")

    async with db_session:
        people = await select(person for person in Person).fetch()
        print([person.name for person in people])

    await db.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
