import unittest

from pony.orm import *
from pony import orm
from pony.orm.tests import setup_database, teardown_database, only_for

db = Database()


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

pony.options.INNER_JOIN_SYNTAX = True


@only_for('sqlite')
class TestJoin(unittest.TestCase):
    exclude_fixtures = {'test': ['clear_tables']}
    @classmethod
    def setUpClass(cls):
        setup_database(db)

        with orm.db_session:
            pop = Genre(name='pop')
            rock = Genre(name='rock')
            Artist(name='Sia', age=40, genres=[pop, rock])
            Artist(name='Lady GaGa', age=30, genres=[pop])

    @classmethod
    def tearDownClass(cls):
        teardown_database(db)

    @db_session
    def test_join_1(self):
        result = select(g.id for g in db.Genre for a in g.artists if a.name.startswith('S'))[:]
        self.assertEqual(db.last_sql, """SELECT DISTINCT "g"."id"
FROM "Genre" "g"
  INNER JOIN "Artist_Genre" "t-1"
    ON "g"."id" = "t-1"."genre"
  INNER JOIN "Artist" "a"
    ON "t-1"."artist" = "a"."id"
WHERE "a"."name" LIKE 'S%'""")

    @db_session
    def test_join_2(self):
        result = select(g.id for g in db.Genre for a in db.Artist
                        if JOIN(a in g.artists) and a.name.startswith('S'))[:]
        self.assertEqual(db.last_sql, """SELECT DISTINCT "g"."id"
FROM "Genre" "g"
  INNER JOIN "Artist_Genre" "t-1"
    ON "g"."id" = "t-1"."genre", "Artist" "a"
WHERE "t-1"."artist" = "a"."id"
  AND "a"."name" LIKE 'S%'""")


    @db_session
    def test_join_3(self):
        result = select(g.id for g in db.Genre for x in db.Artist for a in db.Artist
                        if JOIN(a in g.artists) and a.name.startswith('S') and g.id == x.id)[:]
        self.assertEqual(db.last_sql, '''SELECT DISTINCT "g"."id"
FROM "Genre" "g"
  INNER JOIN "Artist_Genre" "t-1"
    ON "g"."id" = "t-1"."genre", "Artist" "x", "Artist" "a"
WHERE "t-1"."artist" = "a"."id"
  AND "a"."name" LIKE 'S%'
  AND "g"."id" = "x"."id"''')

    @db_session
    def test_join_4(self):
        result = select(g.id for g in db.Genre for a in db.Artist for x in db.Artist
                        if JOIN(a in g.artists) and a.name.startswith('S') and g.id == x.id)[:]
        self.assertEqual(db.last_sql, '''SELECT DISTINCT "g"."id"
FROM "Genre" "g"
  INNER JOIN "Artist_Genre" "t-1"
    ON "g"."id" = "t-1"."genre", "Artist" "a", "Artist" "x"
WHERE "t-1"."artist" = "a"."id"
  AND "a"."name" LIKE 'S%'
  AND "g"."id" = "x"."id"''')

if __name__ == '__main__':
    unittest.main()
