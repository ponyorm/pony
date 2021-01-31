#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division

__author__ = 'luckydonald'

import unittest
from enum import IntEnum, Enum, IntFlag, auto

from pony.orm.core import *
from pony.orm.dbapiprovider import EnumConverter
from pony.orm.tests.testutils import *
from pony.orm.tests import setup_database, teardown_database

"""
Unittests for the EnumConverter
"""


class Fruits(IntEnum):
    APPLE = 0
    MANGO = 42
    BANANA = -7
    PEAR = 69
    CUCUMBER = 4458
# end class


class LightState(str, Enum):
    RED = '#f00'
    YELLOW = '#FFFF00'
    GREEN = '#00ff00'
    OFF = '#000'
# end class


class BinaryStuff(IntFlag):
    ONE = auto()
    TWO = auto()
    FOUR = auto()
    EIGHT = auto()
    SIXTEEN = auto()
    THIRTY_TWO = auto()
    SIXTY_FOUR = auto()
    ONE_HUNDRED_AND_TWENTY_EIGHT = auto()
    TWO_HUNDRED_AND_FIFTY_SIX = auto()
    FIVE_HUNDRED_AND_TWELVE = auto()
    ONE_THOUSAND_AND_TWENTY_FOUR = auto()
    TWO_THOUSAND_AND_FORTY_EIGHT = auto()
    FOUR_THOUSAND_AND_NINETY_SIX = auto()
    EIGHT_THOUSAND_ONE_HUNDRED_AND_NINETY_TWO = auto()
    SIXTEEN_THOUSAND_THREE_HUNDRED_AND_EIGHTY_FOUR = auto()
    THIRTY_TWO_THOUSAND_SEVEN_HUNDRED_AND_SIXTY_EIGHT = auto()
    SIXTY_FIVE_THOUSAND_FIVE_HUNDRED_AND_THIRTY_SIX = auto()
    ONE_HUNDRED_AND_THIRTY_ONE_THOUSAND_AND_SEVENTY_TWO = auto()
    TWO_HUNDRED_AND_SIXTY_TWO_THOUSAND_ONE_HUNDRED_AND_FORTY_FOUR = auto()
    FIVE_HUNDRED_AND_TWENTY_FOUR_THOUSAND_TWO_HUNDRED_AND_EIGHTY_EIGHT = auto()
    ONE_MILLION_FORTY_EIGHT_THOUSAND_FIVE_HUNDRED_AND_SEVENTY_SIX = auto()
    TWO_MILLION_NINETY_SEVEN_THOUSAND_ONE_HUNDRED_AND_FIFTY_TWO = auto()
    FOUR_MILLION_ONE_HUNDRED_AND_NINETY_FOUR_THOUSAND_THREE_HUNDRED_AND_FOUR = auto()
    EIGHT_MILLION_THREE_HUNDRED_AND_EIGHTY_EIGHT_THOUSAND_SIX_HUNDRED_AND_EIGHT = auto()
    SIXTEEN_MILLION_SEVEN_HUNDRED_AND_SEVENTY_SEVEN_THOUSAND_TWO_HUNDRED_AND_SIXTEEN = auto()
    THIRTY_THREE_MILLION_FIVE_HUNDRED_AND_FIFTY_FOUR_THOUSAND_FOUR_HUNDRED_AND_THIRTY_TWO = auto()
    SIXTY_SEVEN_MILLION_ONE_HUNDRED_AND_EIGHT_THOUSAND_EIGHT_HUNDRED_AND_SIXTY_FOUR = auto()
    ONE_HUNDRED_AND_THIRTY_FOUR_MILLION_TWO_HUNDRED_AND_SEVENTEEN_THOUSAND_SEVEN_HUNDRED_AND_TWENTY_EIGHT = auto()
    TWO_HUNDRED_AND_SIXTY_EIGHT_MILLION_FOUR_HUNDRED_AND_THIRTY_FIVE_THOUSAND_FOUR_HUNDRED_AND_FIFTY_SIX = auto()
    FIVE_HUNDRED_AND_THIRTY_SIX_MILLION_EIGHT_HUNDRED_AND_SEVENTY_THOUSAND_NINE_HUNDRED_AND_TWELVE = auto()
    ONE_BILLION_SEVENTY_THREE_MILLION_SEVEN_HUNDRED_AND_FORTY_ONE_THOUSAND_EIGHT_HUNDRED_AND_TWENTY_FOUR = auto()
    TWO_BILLION_ONE_HUNDRED_AND_FORTY_SEVEN_MILLION_FOUR_HUNDRED_AND_EIGHTY_THREE_THOUSAND_SIX_HUNDRED_AND_FORTY_EIGHT = auto()
    FOUR_BILLION_TWO_HUNDRED_AND_NINETY_FOUR_MILLION_NINE_HUNDRED_AND_SIXTY_SEVEN_THOUSAND_TWO_HUNDRED_AND_NINETY_SIX = auto()
    EIGHT_BILLION_FIVE_HUNDRED_AND_EIGHTY_NINE_MILLION_NINE_HUNDRED_AND_THIRTY_FOUR_THOUSAND_FIVE_HUNDRED_AND_NINETY_TWO = auto()
    SEVENTEEN_BILLION_ONE_HUNDRED_AND_SEVENTY_NINE_MILLION_EIGHT_HUNDRED_AND_SIXTY_NINE_THOUSAND_ONE_HUNDRED_AND_EIGHTY_FOUR = auto()
    THIRTY_FOUR_BILLION_THREE_HUNDRED_AND_FIFTY_NINE_MILLION_SEVEN_HUNDRED_AND_THIRTY_EIGHT_THOUSAND_THREE_HUNDRED_AND_SIXTY_EIGHT = auto()
    SIXTY_EIGHT_BILLION_SEVEN_HUNDRED_AND_NINETEEN_MILLION_FOUR_HUNDRED_AND_SEVENTY_SIX_THOUSAND_SEVEN_HUNDRED_AND_THIRTY_SIX = auto()
    ONE_HUNDRED_AND_THIRTY_SEVEN_BILLION_FOUR_HUNDRED_AND_THIRTY_EIGHT_MILLION_NINE_HUNDRED_AND_FIFTY_THREE_THOUSAND_FOUR_HUNDRED_AND_SEVENTY_TWO = auto()
    TWO_HUNDRED_AND_SEVENTY_FOUR_BILLION_EIGHT_HUNDRED_AND_SEVENTY_SEVEN_MILLION_NINE_HUNDRED_AND_SIX_THOUSAND_NINE_HUNDRED_AND_FORTY_FOUR = auto()
    FIVE_HUNDRED_AND_FORTY_NINE_BILLION_SEVEN_HUNDRED_AND_FIFTY_FIVE_MILLION_EIGHT_HUNDRED_AND_THIRTEEN_THOUSAND_EIGHT_HUNDRED_AND_EIGHTY_EIGHT = auto()
    ONE_TRILLION_NINETY_NINE_BILLION_FIVE_HUNDRED_AND_ELEVEN_MILLION_SIX_HUNDRED_AND_TWENTY_SEVEN_THOUSAND_SEVEN_HUNDRED_AND_SEVENTY_SIX = auto()
    TWO_TRILLION_ONE_HUNDRED_AND_NINETY_NINE_BILLION_TWENTY_THREE_MILLION_TWO_HUNDRED_AND_FIFTY_FIVE_THOUSAND_FIVE_HUNDRED_AND_FIFTY_TWO = auto()
    FOUR_TRILLION_THREE_HUNDRED_AND_NINETY_EIGHT_BILLION_FORTY_SIX_MILLION_FIVE_HUNDRED_AND_ELEVEN_THOUSAND_ONE_HUNDRED_AND_FOUR = auto()
    EIGHT_TRILLION_SEVEN_HUNDRED_AND_NINETY_SIX_BILLION_NINETY_THREE_MILLION_TWENTY_TWO_THOUSAND_TWO_HUNDRED_AND_EIGHT = auto()
    SEVENTEEN_TRILLION_FIVE_HUNDRED_AND_NINETY_TWO_BILLION_ONE_HUNDRED_AND_EIGHTY_SIX_MILLION_FORTY_FOUR_THOUSAND_FOUR_HUNDRED_AND_SIXTEEN = auto()
    THIRTY_FIVE_TRILLION_ONE_HUNDRED_AND_EIGHTY_FOUR_BILLION_THREE_HUNDRED_AND_SEVENTY_TWO_MILLION_EIGHTY_EIGHT_THOUSAND_EIGHT_HUNDRED_AND_THIRTY_TWO = auto()
    SEVENTY_TRILLION_THREE_HUNDRED_AND_SIXTY_EIGHT_BILLION_SEVEN_HUNDRED_AND_FORTY_FOUR_MILLION_ONE_HUNDRED_AND_SEVENTY_SEVEN_THOUSAND_SIX_HUNDRED_AND_SIXTY_FOUR = auto()
    ONE_HUNDRED_AND_FORTY_TRILLION_SEVEN_HUNDRED_AND_THIRTY_SEVEN_BILLION_FOUR_HUNDRED_AND_EIGHTY_EIGHT_MILLION_THREE_HUNDRED_AND_FIFTY_FIVE_THOUSAND_THREE_HUNDRED_AND_TWENTY_EIGHT = auto()
    TWO_HUNDRED_AND_EIGHTY_ONE_TRILLION_FOUR_HUNDRED_AND_SEVENTY_FOUR_BILLION_NINE_HUNDRED_AND_SEVENTY_SIX_MILLION_SEVEN_HUNDRED_AND_TEN_THOUSAND_SIX_HUNDRED_AND_FIFTY_SIX = auto()
    FIVE_HUNDRED_AND_SIXTY_TWO_TRILLION_NINE_HUNDRED_AND_FORTY_NINE_BILLION_NINE_HUNDRED_AND_FIFTY_THREE_MILLION_FOUR_HUNDRED_AND_TWENTY_ONE_THOUSAND_THREE_HUNDRED_AND_TWELVE = auto()
    ONE_QUADRILLION_ONE_HUNDRED_AND_TWENTY_FIVE_TRILLION_EIGHT_HUNDRED_AND_NINETY_NINE_BILLION_NINE_HUNDRED_AND_SIX_MILLION_EIGHT_HUNDRED_AND_FORTY_TWO_THOUSAND_SIX_HUNDRED_AND_TWENTY_FOUR = auto()
    TWO_QUADRILLION_TWO_HUNDRED_AND_FIFTY_ONE_TRILLION_SEVEN_HUNDRED_AND_NINETY_NINE_BILLION_EIGHT_HUNDRED_AND_THIRTEEN_MILLION_SIX_HUNDRED_AND_EIGHTY_FIVE_THOUSAND_TWO_HUNDRED_AND_FORTY_EIGHT = auto()
    FOUR_QUADRILLION_FIVE_HUNDRED_AND_THREE_TRILLION_FIVE_HUNDRED_AND_NINETY_NINE_BILLION_SIX_HUNDRED_AND_TWENTY_SEVEN_MILLION_THREE_HUNDRED_AND_SEVENTY_THOUSAND_FOUR_HUNDRED_AND_NINETY_SIX = auto()
    NINE_QUADRILLION_SEVEN_TRILLION_ONE_HUNDRED_AND_NINETY_NINE_BILLION_TWO_HUNDRED_AND_FIFTY_FOUR_MILLION_SEVEN_HUNDRED_AND_FORTY_THOUSAND_NINE_HUNDRED_AND_NINETY_TWO = auto()
    EIGHTEEN_QUADRILLION_FOURTEEN_TRILLION_THREE_HUNDRED_AND_NINETY_EIGHT_BILLION_FIVE_HUNDRED_AND_NINE_MILLION_FOUR_HUNDRED_AND_EIGHTY_ONE_THOUSAND_NINE_HUNDRED_AND_EIGHTY_FOUR = auto()
    THIRTY_SIX_QUADRILLION_TWENTY_EIGHT_TRILLION_SEVEN_HUNDRED_AND_NINETY_SEVEN_BILLION_EIGHTEEN_MILLION_NINE_HUNDRED_AND_SIXTY_THREE_THOUSAND_NINE_HUNDRED_AND_SIXTY_EIGHT = auto()
    SEVENTY_TWO_QUADRILLION_FIFTY_SEVEN_TRILLION_FIVE_HUNDRED_AND_NINETY_FOUR_BILLION_THIRTY_SEVEN_MILLION_NINE_HUNDRED_AND_TWENTY_SEVEN_THOUSAND_NINE_HUNDRED_AND_THIRTY_SIX = auto()
    ONE_HUNDRED_AND_FORTY_FOUR_QUADRILLION_ONE_HUNDRED_AND_FIFTEEN_TRILLION_ONE_HUNDRED_AND_EIGHTY_EIGHT_BILLION_SEVENTY_FIVE_MILLION_EIGHT_HUNDRED_AND_FIFTY_FIVE_THOUSAND_EIGHT_HUNDRED_AND_SEVENTY_TWO = auto()
    TWO_HUNDRED_AND_EIGHTY_EIGHT_QUADRILLION_TWO_HUNDRED_AND_THIRTY_TRILLION_THREE_HUNDRED_AND_SEVENTY_SIX_BILLION_ONE_HUNDRED_AND_FIFTY_ONE_MILLION_SEVEN_HUNDRED_AND_ELEVEN_THOUSAND_SEVEN_HUNDRED_AND_FORTY_FOUR = auto()
    FIVE_HUNDRED_AND_SEVENTY_SIX_QUADRILLION_FOUR_HUNDRED_AND_SIXTY_TRILLION_SEVEN_HUNDRED_AND_FIFTY_TWO_BILLION_THREE_HUNDRED_AND_THREE_MILLION_FOUR_HUNDRED_AND_TWENTY_THREE_THOUSAND_FOUR_HUNDRED_AND_EIGHTY_EIGHT = auto()
    ONE_QUINTILLION_ONE_HUNDRED_AND_FIFTY_TWO_QUADRILLION_NINE_HUNDRED_AND_TWENTY_ONE_TRILLION_FIVE_HUNDRED_AND_FOUR_BILLION_SIX_HUNDRED_AND_SIX_MILLION_EIGHT_HUNDRED_AND_FORTY_SIX_THOUSAND_NINE_HUNDRED_AND_SEVENTY_SIX = auto()
    TWO_QUINTILLION_THREE_HUNDRED_AND_FIVE_QUADRILLION_EIGHT_HUNDRED_AND_FORTY_THREE_TRILLION_NINE_BILLION_TWO_HUNDRED_AND_THIRTEEN_MILLION_SIX_HUNDRED_AND_NINETY_THREE_THOUSAND_NINE_HUNDRED_AND_FIFTY_TWO = auto()
    FOUR_QUINTILLION_SIX_HUNDRED_AND_ELEVEN_QUADRILLION_SIX_HUNDRED_AND_EIGHTY_SIX_TRILLION_EIGHTEEN_BILLION_FOUR_HUNDRED_AND_TWENTY_SEVEN_MILLION_THREE_HUNDRED_AND_EIGHTY_SEVEN_THOUSAND_NINE_HUNDRED_AND_FOUR = auto()

    ZERO = 0
    # The next one would fail for databases which can't do unsigned 64-bit/8-byte.
    # NINE_QUINTILLION_TWO_HUNDRED_AND_TWENTY_THREE_QUADRILLION_THREE_HUNDRED_AND_SEVENTY_TWO_TRILLION_THIRTY_SIX_BILLION_EIGHT_HUNDRED_AND_FIFTY_FOUR_MILLION_SEVEN_HUNDRED_AND_SEVENTY_FIVE_THOUSAND_EIGHT_HUNDRED_AND_EIGHT = auto()
# end class


class Emptiness(int, Enum):
    pass
# end class


class TestEnumCreation(unittest.TestCase):
    """
    Simple tests for table creation.
    Creating a table, but not yet writing or reading data.
    """
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
        """
        Table creation for an integer Enum
        """
        db = self.db

        class FavoriteFruit(db.Entity):
            id = PrimaryKey(int, auto=True)
            user = Required(str)
            fruit = Required(Fruits, size=16)
        # end class

        setup_database(db)
    # end def

    def test__table_creation__str_enum(self):
        """
        Table creation for a string Enum
        """
        db = self.db

        class TrafficLight(db.Entity):
            id = PrimaryKey(int, auto=True)
            state = Required(LightState)
        # end class

        setup_database(db)
    # end def

    def test__table_creation__flag_enum(self):
        """
        Table creation for a string Enum
        """
        db = self.db

        # noinspection PyUnusedLocal
        class CountingManually(db.Entity):
            id = PrimaryKey(int, auto=True)
            power_of_two = Required(BinaryStuff)
        # end class

        setup_database(db)
    # end def
# end class


class TestEnumInsertion(unittest.TestCase):
    """
    More advanced tests already writing to the database,
    but not yet reading.
    """
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
        rollback()

        with db_session:
            class FavoriteFruit(db.Entity):
                id = PrimaryKey(int, auto=True)
                user = Required(str)
                fruit = Required(Fruits, size=16)
            # end class

            class TrafficLight(db.Entity):
                id = PrimaryKey(int, auto=True)
                state = Required(LightState)
            # end class

            class CountingManually(db.Entity):
                id = PrimaryKey(int, auto=True)
                power_of_two = Required(BinaryStuff)
            # end class

            # as those are not global, keep them around
            self.FavoriteFruit = FavoriteFruit
            self.TrafficLight = TrafficLight
            self.CountingManually = CountingManually
        # end with
        setup_database(db)
    # end def

    def tearDown(self):
        rollback()
        teardown_database(self.db)
    # end def

    def test_0(self):
        """ Just make sure the setup did work """
        db = self.db
        self.assertEqual(self.FavoriteFruit, db.FavoriteFruit)
        self.assertEqual(self.TrafficLight, db.TrafficLight)
    # end def

    def test__insert__int_enum(self):
        """
        Inserting integer Enums into the database
        """
        # noinspection PyPep8Naming
        FavoriteFruit = self.FavoriteFruit
        with db_session:
            FavoriteFruit(user="Me", fruit=Fruits.MANGO)
            FavoriteFruit(user="You", fruit=Fruits.BANANA)
        # end with
    # end def

    def test__insert__str_enum(self):
        """
        Inserting string Enums into the database
        """
        TrafficLight = self.TrafficLight
        with db_session:
            TrafficLight(state=LightState.YELLOW)
            TrafficLight(state=LightState.RED)
        # end with
    # end def

    def test__insert__int_flag_enum(self):
        """
        Inserting string Enums into the database
        """
        # noinspection PyPep8Naming
        CountingManually = self.CountingManually
        with db_session:
            CountingManually(power_of_two=BinaryStuff.TWO_HUNDRED_AND_SIXTY_EIGHT_MILLION_FOUR_HUNDRED_AND_THIRTY_FIVE_THOUSAND_FOUR_HUNDRED_AND_FIFTY_SIX)
            CountingManually(power_of_two=BinaryStuff.FOUR_QUINTILLION_SIX_HUNDRED_AND_ELEVEN_QUADRILLION_SIX_HUNDRED_AND_EIGHTY_SIX_TRILLION_EIGHTEEN_BILLION_FOUR_HUNDRED_AND_TWENTY_SEVEN_MILLION_THREE_HUNDRED_AND_EIGHTY_SEVEN_THOUSAND_NINE_HUNDRED_AND_FOUR)
        # end with
    # end def

    def test__select__int_enum(self):
        """
        Integer Enums in an select statement
        """
        # noinspection PyPep8Naming
        FavoriteFruit = self.FavoriteFruit
        with db_session:
            select(ff for ff in FavoriteFruit if ff.fruit == Fruits.PEAR)
            select(ff for ff in FavoriteFruit if ff.fruit != Fruits.PEAR)
        # end with
    # end def

    def test__select__str_enum(self):
        """
        String Enums in an select statement
        """
        # noinspection PyPep8Naming
        TrafficLight = self.TrafficLight
        with db_session:
            select(tl for tl in TrafficLight if tl.state == LightState.GREEN)
            select(tl for tl in TrafficLight if tl.state != LightState.GREEN)
        # end with
    # end def

    def test__select__int_flag_enum(self):
        """
        String Enums in an select statement
        """
        # noinspection PyPep8Naming
        CountingManually = self.CountingManually
        with db_session:
            select(cm for cm in CountingManually if cm.power_of_two == BinaryStuff.FOUR)
            select(cm for cm in CountingManually if cm.power_of_two != BinaryStuff.SIXTY_FOUR)
        # end with
    # end def
# end def


# noinspection DuplicatedCode
class TestEnumLoad(unittest.TestCase):
    def setUp(self):
        db = self.db = Database()
        rollback()

        class FavoriteFruit(db.Entity):
            id = PrimaryKey(int, auto=True)
            user = Required(str)
            fruit = Required(Fruits, size=16)
        # end class

        class TrafficLight(db.Entity):
            id = PrimaryKey(int, auto=True)
            state = Required(LightState)
        # end class

        class CountingManually(db.Entity):
            id = PrimaryKey(int, auto=True)
            power_of_two = Required(BinaryStuff)
        # end class

        # as those are not global, keep them around
        self.FavoriteFruit = FavoriteFruit
        self.TrafficLight = TrafficLight
        self.CountingManually = CountingManually
        setup_database(db)

        with db_session:
            self.ff1 = FavoriteFruit(user="Me", fruit=Fruits.MANGO)
            self.ff2 = FavoriteFruit(user="You", fruit=Fruits.BANANA)

            self.tl1 = TrafficLight(state=LightState.RED)
            self.tl2 = TrafficLight(state=LightState.GREEN)

            self.cm1 = CountingManually(power_of_two=BinaryStuff.ONE)
            self.cm2 = CountingManually(power_of_two=BinaryStuff.TWO)
        # end with
    # end def

    def tearDown(self):
        teardown_database(self.db)
    # end def

    def test__load__int_enum(self):
        """
        Retrieving integer Enums from the database
        """
        # noinspection PyPep8Naming
        FavoriteFruit = self.FavoriteFruit
        with db_session:
            ff1 = FavoriteFruit.get(user="Me")
            ff2 = FavoriteFruit.get(fruit=Fruits.BANANA)
            ff0 = FavoriteFruit.get(fruit=Fruits.CUCUMBER)
        # end with

        self.assertIsNone(ff0, msg="Requesting a value not in the database must return None")

        self.assertEqual(self.ff1.id, ff1.id, msg="ID must be the same as the one inserted to the database")
        self.assertEqual(self.ff2.id, ff2.id, msg="ID must be the same as the one inserted to the database")

        self.assertEqual(self.ff1.user, ff1.user, msg="User must be the same as the one inserted to the database")
        self.assertEqual(self.ff2.user, ff2.user, msg="User must be the same as the one inserted to the database")

        self.assertEqual(self.ff1.fruit, ff1.fruit, msg="Fruit (enum) must be the same as the one inserted to the database")
        self.assertEqual(self.ff2.fruit, ff2.fruit, msg="Fruit (enum) must be the same as the one inserted to the database")

        self.assertIsInstance(self.ff1.fruit, Enum, msg="Original must be Enum")
        self.assertIsInstance(self.ff2.fruit, Enum, msg="Original must be Enum")

        self.assertIsInstance(self.ff1.fruit, Fruits, msg="Original must be Fruits Enum")
        self.assertIsInstance(self.ff2.fruit, Fruits, msg="Original must be Fruits Enum")

        self.assertIsInstance(ff1.fruit, Enum, msg="Loaded one must be Enum")
        self.assertIsInstance(ff2.fruit, Enum, msg="Loaded one must be Enum")

        self.assertIsInstance(ff1.fruit, Fruits, msg="Loaded one must be Fruits Enum")
        self.assertIsInstance(ff2.fruit, Fruits, msg="Loaded one must be Fruits Enum")
    # end def

    def test__load__str_enum(self):
        """
        Retrieving string Enums from the database
        """
        # noinspection PyPep8Naming
        TrafficLight = self.TrafficLight
        with db_session:
            tl1 = TrafficLight.get(state=LightState.RED)
            tl2 = TrafficLight.get(state=LightState.GREEN)
            tl0 = TrafficLight.get(state=LightState.OFF)
        # end with

        self.assertIsNone(tl0, msg="Requesting a value not in the database must return None")

        self.assertEqual(self.tl1.id, tl1.id, msg="ID must be the same as the one inserted to the database")
        self.assertEqual(self.tl2.id, tl2.id, msg="ID must be the same as the one inserted to the database")

        self.assertEqual(self.tl1.state, tl1.state, msg="State (enum) must be the same as the one inserted to the database")
        self.assertEqual(self.tl2.state, tl2.state, msg="State (enum) must be the same as the one inserted to the database")

        self.assertIsInstance(self.tl1.state, Enum, msg="Original must be Enum")
        self.assertIsInstance(self.tl2.state, Enum, msg="Original must be Enum")

        self.assertIsInstance(self.tl1.state, LightState, msg="Original must be LightState Enum")
        self.assertIsInstance(self.tl2.state, LightState, msg="Original must be LightState Enum")

        self.assertIsInstance(tl1.state, Enum, msg="Loaded one must be Enum")
        self.assertIsInstance(tl2.state, Enum, msg="Loaded one must be Enum")

        self.assertIsInstance(tl1.state, LightState, msg="Loaded one must be LightState Enum")
        self.assertIsInstance(tl2.state, LightState, msg="Loaded one must be LightState Enum")
    # end def

    def test__load__int_flag_enum(self):
        """
        Retrieving IntFlag Enums from the database
        """
        # noinspection PyPep8Naming
        CountingManually = self.CountingManually
        with db_session:
            cm1 = CountingManually.get(power_of_two=BinaryStuff.ONE)
            cm2 = CountingManually.get(power_of_two=BinaryStuff.TWO)
            cm0 = CountingManually.get(power_of_two=BinaryStuff.ZERO)
        # end with

        self.assertIsNone(cm0, msg="Requesting a value not in the database must return None")

        self.assertEqual(self.cm1.id, cm1.id, msg="ID must be the same as the one inserted to the database")
        self.assertEqual(self.cm2.id, cm2.id, msg="ID must be the same as the one inserted to the database")

        self.assertEqual(self.cm1.power_of_two, cm1.power_of_two, msg="State (enum) must be the same as the one inserted to the database")
        self.assertEqual(self.cm2.power_of_two, cm2.power_of_two, msg="State (enum) must be the same as the one inserted to the database")

        self.assertIsInstance(self.cm1.power_of_two, Enum, msg="Original must be Enum")
        self.assertIsInstance(self.cm2.power_of_two, Enum, msg="Original must be Enum")

        self.assertIsInstance(self.cm1.power_of_two, BinaryStuff, msg="Original must be BinaryStuff Enum")
        self.assertIsInstance(self.cm2.power_of_two, BinaryStuff, msg="Original must be BinaryStuff Enum")

        self.assertIsInstance(cm1.power_of_two, Enum, msg="Loaded one must be Enum")
        self.assertIsInstance(cm2.power_of_two, Enum, msg="Loaded one must be Enum")

        self.assertIsInstance(cm1.power_of_two, BinaryStuff, msg="Loaded one must be BinaryStuff Enum")
        self.assertIsInstance(cm2.power_of_two, BinaryStuff, msg="Loaded one must be BinaryStuff Enum")
    # end def

    def test__to_json__int_enum(self):
        """
        to_json() of integer Enums
        """
        self.assertEqual(Fruits.MANGO.value, +42, msg="Just to be sure the number of the enum is correct in the unittests; needed below")
        self.assertEqual(Fruits.BANANA.value, -7, msg="Just to be sure the number of the enum is correct in the unittests; needed below")

        dict1 = self.ff1.to_dict()
        dict2 = self.ff2.to_dict()

        self.assertDictEqual(dict1, {"id": 1, "user": "Me", "fruit": +42})
        self.assertDictEqual(dict2, {"id": 2, "user": "You", "fruit": -7})
    # end def

    def test__to_json__str_enum(self):
        """
        to_json() of string Enums
        """
        self.assertEqual(LightState.RED.value, '#f00', msg="Just to be sure the value of the enum is correct in the unittests; needed below")
        self.assertEqual(LightState.GREEN.value, '#00ff00', msg="Just to be sure the value of the enum is correct in the unittests; needed below")

        dict1 = self.tl1.to_dict()
        dict2 = self.tl2.to_dict()

        self.assertDictEqual(dict1, {"id": 1, "state": '#f00'})
        self.assertDictEqual(dict2, {"id": 2, "state": '#00ff00'})
    # end def

    def test__to_json__int_flag_enum(self):
        """
        to_json() of string Enums
        """
        # self.cm1 = CountingManually(power_of_two=BinaryStuff.ONE)

        self.assertEqual(BinaryStuff.ONE.value, 1, msg="Just to be sure the number of the enum is correct in the unittests; needed below")
        self.assertEqual(BinaryStuff.TWO.value, 2, msg="Just to be sure the number of the enum is correct in the unittests; needed below")

        dict1 = self.cm1.to_dict()
        dict2 = self.cm2.to_dict()

        self.assertDictEqual(dict1, {"id": 1, "power_of_two": 1})
        self.assertDictEqual(dict2, {"id": 2, "power_of_two": 2})
    # end def
# end class


class TestEnumDefaults(unittest.TestCase):
    """
    Test for kwargs modifications
    """

    def test___prepare_int_kwargs__default(self):
        """
        Default sane parameters
        """
        input_enum = Fruits
        input_kwargs = {}
        expected_kwargs = {"min": -7, "max": 4458, "unsigned": False, "size": 16}

        output_kwargs = EnumConverter._prepare_int_kwargs(input_enum, input_kwargs, uint64_support=True, attr='the_best_field')

        self.assertDictEqual(output_kwargs, expected_kwargs, msg="Should result in the expected kwargs.")
    # end def

    def test___prepare_int_kwargs__min_smaller_kept(self):
        """
        Keep smaller min -> extreme one wins
        """
        input_enum = Fruits
        input_kwargs = {"min": -42}
        expected_kwargs = {"min": -42, "max": 4458, "unsigned": False, "size": 16}

        output_kwargs = EnumConverter._prepare_int_kwargs(input_enum, input_kwargs, uint64_support=True, attr='the_best_field')

        self.assertDictEqual(output_kwargs, expected_kwargs, msg="Should result in the expected kwargs.")
    # end def

    def test___prepare_int_kwargs__min_bigger_error(self):
        """
        needing a smaller min should cause an exception
        """
        input_enum = Fruits
        input_kwargs = {"min": -1}

        with self.assertRaises(TypeError, msg="should fail") as e_context:
            EnumConverter._prepare_int_kwargs(input_enum, input_kwargs, uint64_support=True, attr='the_best_field')
        # end with
        expected_msg = "Enum option <Fruits.BANANA: -7> with numeric value -7 would not fit within the given min=-1 limit (attribute the_best_field)."
        self.assertEquals(expected_msg, str(e_context.exception))
    # end def

    def test___prepare_int_kwargs__max_bigger_kept(self):
        """
        Keep bigger max -> extreme one wins
        """
        input_enum = Fruits
        input_kwargs = {"max": 4459}
        expected_kwargs = {"min": -7, "max": 4459, "unsigned": False, "size": 16}

        output_kwargs = EnumConverter._prepare_int_kwargs(input_enum, input_kwargs, uint64_support=True, attr='the_best_field')

        self.assertDictEqual(output_kwargs, expected_kwargs, msg="Should result in the expected kwargs.")
    # end def

    def test___prepare_int_kwargs__max_smaller_error(self):
        """
        needing a bigger max should cause an exception
        """
        input_enum = Fruits
        input_kwargs = {"max": 42}

        with self.assertRaises(TypeError, msg="should fail") as e_context:
            EnumConverter._prepare_int_kwargs(input_enum, input_kwargs, uint64_support=True, attr='the_best_field')
        # end with
        expected_msg = "Enum option <Fruits.CUCUMBER: 4458> with numeric value 4458 would not fit within the given max=42 limit (attribute the_best_field)."
        self.assertEquals(expected_msg, str(e_context.exception))
    # end def

    def test___prepare_int_kwargs__size_bigger_kept(self):
        """
        Keep bigger size -> extreme one wins
        """
        input_enum = Fruits
        input_kwargs = {"size": 24}
        expected_kwargs = {"min": -7, "max": 4458, "unsigned": False, "size": 24}

        output_kwargs = EnumConverter._prepare_int_kwargs(input_enum, input_kwargs, uint64_support=True, attr='the_best_field')

        self.assertDictEqual(output_kwargs, expected_kwargs, msg="Should result in the expected kwargs.")
    # end def

    def test___prepare_int_kwargs__size_smaller_error(self):
        """
        needing a bigger size should cause an exception
        """
        input_enum = Fruits
        input_kwargs = {"size": 8}

        with self.assertRaises(TypeError, msg="should fail") as e_context:
            EnumConverter._prepare_int_kwargs(input_enum, input_kwargs, uint64_support=True, attr='the_best_field')
        # end with
        expected_msg = "Enum option <Fruits.CUCUMBER: 4458> with numeric value 4458 cannot fit the signed size 8 with range [-32768 - 32767]. Needs to be at least of size 16. (attribute the_best_field)."
        self.assertEquals(expected_msg, str(e_context.exception))
    # end def

    def test___prepare_int_kwargs__empty_enum_causes_error(self):
        """
        Empty enums are not allowed
        """
        input_enum = Emptiness
        input_kwargs = {}

        with self.assertRaises(TypeError, msg="should fail") as e_context:
            EnumConverter._prepare_int_kwargs(input_enum, input_kwargs, uint64_support=True, attr='the_best_field')
        # end with
        expected_msg = "Enum <enum 'Emptiness'> (of attribute the_best_field) has no values defined."
        self.assertEquals(expected_msg, str(e_context.exception))
    # end def

    def test___prepare_str_kwargs__default(self):
        """
        Default sane parameters
        """
        input_enum = LightState
        input_kwargs = {}
        expected_kwargs = {"max_len": 7, "autostrip": False}

        output_kwargs = EnumConverter._prepare_str_kwargs(input_enum, input_kwargs, attr='the_best_field')

        self.assertDictEqual(output_kwargs, expected_kwargs, msg="Should result in the expected kwargs.")
    # end def

    def test___prepare_str_kwargs__length_longer_kept(self):
        """
        That a longer manual max_len is not overwritten.
        """
        input_enum = LightState
        input_kwargs = {"max_len": 123}
        expected_kwargs = {"max_len": 123, "autostrip": False}

        output_kwargs = EnumConverter._prepare_str_kwargs(input_enum, input_kwargs, attr='the_best_field')

        self.assertDictEqual(output_kwargs, expected_kwargs, msg="Should result in the expected kwargs.")
    # end def

    def test___prepare_str_kwargs__autostrip_off_is_okey(self):
        """
        Setting the autostrip to off should be valid.
        """
        input_enum = LightState
        input_kwargs = {"autostrip": False}
        expected_kwargs = {"max_len": 7, "autostrip": False}

        output_kwargs = EnumConverter._prepare_str_kwargs(input_enum, input_kwargs, attr='the_best_field')

        self.assertDictEqual(output_kwargs, expected_kwargs, msg="Should result in the expected kwargs.")
    # end def
# end class


if __name__ == "__main__":
    unittest.main()
# end if
