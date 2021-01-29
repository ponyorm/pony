#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division

__author__ = 'luckydonald'

import unittest
from enum import IntEnum, Enum

from pony.orm.core import *
from pony.orm.tests.testutils import *
from pony.orm.tests import setup_database, teardown_database

class Fruits(IntEnum):
    APPLE = 0
    MANGO = 42
    BANANA = -7
    PEAR = 69
    CUCUMBER = 4458
# end class


class TrafficLightState(str, Enum):
    RED = '#f00'
    YELLOW = '#FFFF00'
    GREEN = '#00ff00'
    OFF = '#000'
# end class


class TestEnumCreation(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # setup_database(db)
        # with db_session:
        #     d1 = Department(number=1)
        # # end with
        pass
    # end def

    @classmethod
    def tearDownClass(cls):
        # teardown_database(db)
        pass
    # end def

    def setUp(self):
        # db_session.__enter__()
        self.db = Database()
        pass
    # end def

    def tearDown(self):
        teardown_database(self.db)
        # db_session.__exit__()
    # end def

    def test__table_creation__int_enum(self):
        db = self.db

        class FavoriteFruit(db.Entity):
            id = PrimaryKey(int, auto=True)
            user = Required(str)
            fruit = Required(Fruits, size=16)
        # end class

        setup_database(db)
    # end def

    def test__table_creation__str_enum(self):
        db = self.db

        class TrafficLight(db.Entity):
            id = PrimaryKey(int, auto=True)
            state = Required(TrafficLightState)
        # end class

        setup_database(db)
    # end def
# end class


class TestEnumInsertion(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # setup_database(db)
        # with db_session:
        #     d1 = Department(number=1)
        # # end with
        pass

    # end def

    @classmethod
    def tearDownClass(cls):
        # teardown_database(db)
        pass
    # end def

    def setUp(self):
        db = self.db = Database()

        with db_session:
            class FavoriteFruit(db.Entity):
                id = PrimaryKey(int, auto=True)
                user = Required(str)
                fruit = Required(Fruits, size=16)
            # end class

            class TrafficLight(db.Entity):
                id = PrimaryKey(int, auto=True)
                state = Required(TrafficLightState)
            # end class

            # as those are not global, keep them around
            self.FavoriteFruit = FavoriteFruit
            self.TrafficLight = TrafficLight
        # end with
        setup_database(db)
    # end def

    def tearDown(self):
        teardown_database(self.db)
    # end def

    def test_0(self):
        """ Just make sure the setup did work """
        db = self.db
        self.assertEqual(self.FavoriteFruit, db.FavoriteFruit)
        self.assertEqual(self.TrafficLight, db.TrafficLight)
    # end def

    def test__insert__int_enum(self):
        FavoriteFruit = self.FavoriteFruit
        with db_session:
            FavoriteFruit(user="Me", fruit=Fruits.MANGO)
            FavoriteFruit(user="Me", fruit=Fruits.BANANA)
        # end with
    # end def

    def test__insert__str_enum(self):
        TrafficLight = self.TrafficLight
        with db_session:
            TrafficLight(state=TrafficLightState.YELLOW)
            TrafficLight(state=TrafficLightState.RED)
        # end with
    # end def

    def test__select__int_enum(self):
        FavoriteFruit = self.FavoriteFruit
        with db_session:
            select(ff for ff in FavoriteFruit if ff.fruit == Fruits.PEAR)
            select(ff for ff in FavoriteFruit if ff.fruit != Fruits.PEAR)
        # end with
    # end def

    def test__select__str_enum(self):
        TrafficLight = self.TrafficLight
        with db_session:
            select(tl for tl in TrafficLight if tl.state == TrafficLightState.GREEN)
            select(tl for tl in TrafficLight if tl.state != TrafficLightState.GREEN)
        # end with
    # end def
# end class


if __name__ == "__main__":
    unittest.main()
# end if


"""
class TestProxy(unittest.TestCase):
    def setUp(self):
        setup_database(db)
        with db_session:
            c1 = Country(id=1, name='Russia')
            c2 = Country(id=2, name='Japan')
            Person(id=1, name='Alexander Nevskiy', country=c1)
            Person(id=2, name='Raikou Minamoto', country=c2)
            Person(id=3, name='Ibaraki Douji', country=c2)
        # end with
    # end def

    def tearDown(self):
        teardown_database(db)
    # end def

    def test_1(self):
        with db_session:
            p = make_proxy(Person[2])
        # end with

        with db_session:
            x1 = db.local_stats[None].db_count  # number of queries
            # it is possible to access p attributes in a new db_session
            name = p.name
            country = p.country
            x2 = db.local_stats[None].db_count
        # end with

        # p.name and p.country are loaded with a single query
        self.assertEqual(x1, x2 - 1)
    # end def

    def test_group_by_having(self):
        result = set(select((s.age, sum(s.scholarship)) for s in Student if sum(s.scholarship) < 300))
        self.assertEqual(result, {(20, 0), (21, 200)})
        self.assertNotIn('distinct', db.last_sql.lower())
    # end def

    def test_aggregation_no_group_by_1(self):
        result = set(select(sum(s.scholarship) for s in Student if s.age < 23))
        self.assertEqual(result, {200})
        self.assertNotIn('distinct', db.last_sql.lower())
    # end def

    def test_aggregation_no_group_by_2(self):
        result = set(select((sum(s.scholarship), min(s.scholarship)) for s in Student if s.age < 23))
        self.assertEqual(result, {(200, 0)})
        self.assertNotIn('distinct', db.last_sql.lower())
    # end def

    def test_aggregation_no_group_by_3(self):
        result = set(select((sum(s.scholarship), min(s.scholarship))
                            for s in Student for g in Group
                            if s.group == g and g.dept.number == 1))
        self.assertEqual(result, {(400, 0)})
        self.assertNotIn('distinct', db.last_sql.lower())
    # end def
# end class
"""
