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
# end class


if __name__ == "__main__":
    unittest.main()
# end if
