import unittest

from pony.orm import *
from pony import orm

import pony.orm.tests.fixtures

class TestJoin(unittest.TestCase):

    exclude_fixtures = {'test': ['clear_tables']}

    @classmethod
    def setUpClass(cls):
        db = cls.db = Database('sqlite', ':memory:')

        class Genre(db.Entity):
            name = orm.Optional(str) # TODO primary key
            artists = orm.Set('Artist')
            favorite = orm.Optional(bool)
            index = orm.Optional(int)

        class Hobby(db.Entity):
            name = orm.Required(str)
            artists = orm.Set('Artist')

        class Artist(db.Entity):
            name = orm.Required(str)
            age = orm.Optional(int)
            hobbies = orm.Set(Hobby)
            genres = orm.Set(Genre)

        db.generate_mapping(create_tables=True)

        with orm.db_session:
            pop = Genre(name='pop')
            rock = Genre(name='rock')
            Artist(name='Sia', age=40, genres=[pop, rock])
            Artist(name='Lady GaGa', age=30, genres=[pop])

        pony.options.INNER_JOIN_SYNTAX = True

    @db_session
    def test_join_1(self):
        result = select(g.id for g in self.db.Genre for a in g.artists if a.name.startswith('S'))[:]
        self.assertEqual(self.db.last_sql, """SELECT DISTINCT "g"."id"
FROM "genre" "g"
  INNER JOIN "artist_genres" "t-1"
    ON "g"."id" = "t-1"."genre"
  INNER JOIN "artist" "a"
    ON "t-1"."artist" = "a"."id"
WHERE "a"."name" LIKE 'S%'""")

    @db_session
    def test_join_2(self):
        result = select(g.id for g in self.db.Genre for a in self.db.Artist
                        if JOIN(a in g.artists) and a.name.startswith('S'))[:]
        self.assertEqual(self.db.last_sql, """SELECT DISTINCT "g"."id"
FROM "genre" "g"
  INNER JOIN "artist_genres" "t-1"
    ON "g"."id" = "t-1"."genre", "artist" "a"
WHERE "t-1"."artist" = "a"."id"
  AND "a"."name" LIKE 'S%'""")


    @db_session
    def test_join_3(self):
        result = select(g.id for g in self.db.Genre for x in self.db.Artist for a in self.db.Artist
                        if JOIN(a in g.artists) and a.name.startswith('S') and g.id == x.id)[:]
        self.assertEqual(self.db.last_sql, '''SELECT DISTINCT "g"."id"
FROM "genre" "g"
  INNER JOIN "artist_genres" "t-1"
    ON "g"."id" = "t-1"."genre", "artist" "x", "artist" "a"
WHERE "t-1"."artist" = "a"."id"
  AND "a"."name" LIKE 'S%'
  AND "g"."id" = "x"."id"''')

    @db_session
    def test_join_4(self):
        result = select(g.id for g in self.db.Genre for a in self.db.Artist for x in self.db.Artist
                        if JOIN(a in g.artists) and a.name.startswith('S') and g.id == x.id)[:]
        self.assertEqual(self.db.last_sql, '''SELECT DISTINCT "g"."id"
FROM "genre" "g"
  INNER JOIN "artist_genres" "t-1"
    ON "g"."id" = "t-1"."genre", "artist" "a", "artist" "x"
WHERE "t-1"."artist" = "a"."id"
  AND "a"."name" LIKE 'S%'
  AND "g"."id" = "x"."id"''')

if __name__ == '__main__':
    unittest.main()
