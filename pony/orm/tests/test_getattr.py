from pony.py23compat import basestring

import unittest

from pony.orm import *
from pony import orm
from pony.utils import cached_property
from pony.orm.tests.testutils import raises_exception
from pony.orm.tests import db_params, setup_database, teardown_database

class Test(unittest.TestCase):

    @cached_property
    def db(self):
        return orm.Database()

    def setUp(self):
        db = self.db

        class Genre(db.Entity):
            name = orm.Required(str)
            artists = orm.Set('Artist')

        class Hobby(db.Entity):
            name = orm.Required(str)
            artists = orm.Set('Artist')

        class Artist(db.Entity):
            name = orm.Required(str)
            age = orm.Optional(int)
            hobbies = orm.Set(Hobby)
            genres = orm.Set(Genre)

        setup_database(db)

        with orm.db_session:
            pop = Genre(name='Pop')
            Artist(name='Sia', age=40, genres=[pop])
            Hobby(name='Swimming')

        pony.options.INNER_JOIN_SYNTAX = True

    def tearDown(self):
        teardown_database(self.db)

    @db_session
    def test_no_caching(self):
        for attr_name, attr_type in zip(['name', 'age'], [basestring, int]):
            val = select(getattr(x, attr_name) for x in self.db.Artist).first()
            self.assertIsInstance(val, attr_type)

    @db_session
    def test_simple(self):
        val = select(getattr(x, 'age') for x in self.db.Artist).first()
        self.assertIsInstance(val, int)

    @db_session
    def test_expr(self):
        val = select(getattr(x, ''.join(['ag', 'e'])) for x in self.db.Artist).first()
        self.assertIsInstance(val, int)
    
    @db_session
    def test_external(self):
        class data:
            id = 1
        val = select(x.id for x in self.db.Artist if x.id >= getattr(data, 'id')).first()
        self.assertIsNotNone(val)
    
    @db_session
    def test_related(self):
        val = select(getattr(x.genres, 'name') for x in self.db.Artist).first()
        self.assertIsNotNone(val)
    
    @db_session
    def test_not_instance_iter(self):
        val = select(getattr(x.name, 'startswith')('S') for x in self.db.Artist).first()
        self.assertTrue(val)
    
    @raises_exception(TranslationError, 'Expression `getattr(x, x.name)` cannot be translated into SQL '
                                        'because x.name will be different for each row')
    @db_session
    def test_not_external(self):
        select(getattr(x, x.name) for x in self.db.Artist)

    @raises_exception(TypeError, 'In `getattr(x, 1)` second argument should be a string. Got: 1')
    @db_session
    def test_not_string(self):
        select(getattr(x, 1) for x in self.db.Artist)

    @raises_exception(TypeError, 'In `getattr(x, name)` second argument should be a string. Got: 1')
    @db_session
    def test_not_string(self):
        name = 1
        select(getattr(x, name) for x in self.db.Artist)

    @db_session
    def test_lambda_1(self):
        for name, value in [('name', 'Sia'), ('age', 40), ('name', 'Sia')]:
            result = self.db.Artist.select(lambda a: getattr(a, name) == value)
            self.assertEqual(set(obj.name for obj in result), {'Sia'})

    @db_session
    def test_lambda_2(self):
        for entity, name, value in [
                (self.db.Genre, 'name', 'Pop'),
                (self.db.Artist, 'age', 40),
                (self.db.Hobby, 'name', 'Swimming'),
        ]:
            result = entity.select(lambda a: getattr(a, name) == value)
            self.assertEqual(set(result[:]), {entity.select().first()})
