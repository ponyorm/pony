from pony.py23compat import basestring

import unittest

from pony.orm import *
from pony import orm
from pony.utils import cached_property
from pony.orm.tests.testutils import raises_exception

class Test(unittest.TestCase):

    @cached_property
    def db(self):
        return orm.Database('sqlite', ':memory:')

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

        db.generate_mapping(check_tables=True, create_tables=True)

        with orm.db_session:
            pop = Genre(name='pop')
            Artist(name='Sia', age=40, genres=[pop])

        pony.options.INNER_JOIN_SYNTAX = True
    
    @db_session
    def test_no_caching(self):
        for attr, type in zip(['name', 'age'], [basestring, int]):
            val = select(getattr(x, attr) for x in self.db.Artist).first()
            self.assertIsInstance(val, type)
    
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
    
    @db_session
    @raises_exception(TypeError, '`x.name` should be either external expression or constant.')
    def test_not_external(self):
        select(getattr(x, x.name) for x in self.db.Artist)

    @raises_exception(TypeError, 'getattr(x, 1): attribute name must be string. Got: 1')
    @db_session
    def test_not_string(self):
        select(getattr(x, 1) for x in self.db.Artist)


    @raises_exception(TypeError, 'getattr(x, name): attribute name must be string. Got: 1')
    @db_session
    def test_not_string(self):
        name = 1
        select(getattr(x, name) for x in self.db.Artist)


