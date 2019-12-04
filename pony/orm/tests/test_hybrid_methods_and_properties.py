import unittest

from pony.orm import *
from pony.orm.tests.testutils import *
from pony.orm.tests import setup_database, teardown_database

db = Database()

sep = ' '


class Person(db.Entity):
    id = PrimaryKey(int)
    first_name = Required(str)
    last_name = Required(str)
    favorite_color = Optional(str)
    cars = Set(lambda: Car)

    @property
    def full_name(self):
        return self.first_name + sep + self.last_name

    @property
    def full_name_2(self):
        return concat(self.first_name, sep, self.last_name)  # tests using of function `concat` from external scope

    @property
    def has_car(self):
        return not self.cars.is_empty()

    def cars_by_color1(self, color):
        return select(car for car in self.cars if car.color == color)

    def cars_by_color2(self, color):
        return self.cars.select(lambda car: car.color == color)

    @property
    def cars_price(self):
        return sum(c.price for c in self.cars)

    @property
    def incorrect_full_name(self):
        return self.first_name + ' ' + p.last_name  # p is FakePerson instance here

    @classmethod
    def find_by_full_name(cls, full_name):
        return cls.select(lambda p: p.full_name_2 == full_name)

    def complex_method(self):
        result = ''
        for i in range(10):
            result += str(i)
        return result

    def simple_method(self):
        return self.complex_method()


class FakePerson(object):
    pass


p = FakePerson()
p.last_name = '***'


class Car(db.Entity):
    brand = Required(str)
    model = Required(str)
    owner = Optional(Person)
    year = Required(int)
    price = Required(int)
    color = Required(str)


def simple_func(person):
    return person.full_name


def complex_func(person):
    return person.complex_method()



class TestHybridsAndProperties(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_database(db)
        with db_session:
            p1 = Person(id=1, first_name='Alexander', last_name='Kozlovsky', favorite_color='white')
            p2 = Person(id=2, first_name='Alexei', last_name='Malashkevich', favorite_color='green')
            p3 = Person(id=3, first_name='Vitaliy', last_name='Abetkin')
            p4 = Person(id=4, first_name='Alexander', last_name='Tischenko', favorite_color='blue')

            c1 = Car(id=1, brand='Peugeot', model='306', owner=p1, year=2006, price=14000, color='red')
            c2 = Car(id=2, brand='Honda', model='Accord', owner=p1, year=2007, price=13850, color='white')
            c3 = Car(id=3, brand='Nissan', model='Skyline', owner=p2, year=2008, price=29900, color='black')
            c4 = Car(id=4, brand='Volkswagen', model='Passat', owner=p1, year=2012, price=9400, color='blue')
            c5 = Car(id=5, brand='Koenigsegg', model='CCXR', owner=p4, year=2016, price=4850000, color='white')
            c6 = Car(id=6, brand='Lada', model='Kalina', owner=p4, year=2015, price=5000, color='white')

    @classmethod
    def tearDownClass(cls):
        teardown_database(db)

    @db_session
    def test1(self):
        persons = select(p.full_name for p in Person if p.has_car)[:]
        self.assertEqual(set(persons), {'Alexander Kozlovsky', 'Alexei Malashkevich', 'Alexander Tischenko'})

    @db_session
    def test2(self):
        cars_prices = select(p.cars_price for p in Person)[:]
        self.assertEqual(set(cars_prices), {0, 29900, 37250, 4855000})

    @db_session
    def test3(self):
        persons = select(p.full_name for p in Person if p.cars_price > 100000)[:]
        self.assertEqual(set(persons), {'Alexander Tischenko'})

    @db_session
    def test4(self):
        persons = select(p.full_name for p in Person if not p.cars_price)[:]
        self.assertEqual(set(persons), {'Vitaliy Abetkin'})

    @db_session
    def test5(self):
        persons = select(p.full_name for p in Person if exists(c for c in p.cars_by_color2('white') if c.price > 10000))[:]
        self.assertEqual(set(persons), {'Alexander Kozlovsky', 'Alexander Tischenko'})

    @db_session
    def test6(self):
        persons = select(p.full_name for p in Person if exists(c for c in p.cars_by_color1('white') if c.price > 10000))[:]
        self.assertEqual(set(persons), {'Alexander Kozlovsky', 'Alexander Tischenko'})

    @db_session
    def test7(self):
        c1 = Car[1]
        persons = select(p.full_name for p in Person if c1 in p.cars_by_color2('red'))[:]
        self.assertEqual(set(persons), {'Alexander Kozlovsky'})

    @db_session
    def test8(self):
        c1 = Car[1]
        persons = select(p.full_name for p in Person if c1 in p.cars_by_color1('red'))[:]
        self.assertEqual(set(persons), {'Alexander Kozlovsky'})

    @db_session
    def test9(self):
        persons = select(p.full_name for p in Person if p.cars_by_color1(p.favorite_color))[:]
        self.assertEqual(set(persons), {'Alexander Kozlovsky'})

    @db_session
    def test10(self):
        persons = select(p.full_name for p in Person if not p.cars_by_color1(p.favorite_color))[:]
        self.assertEqual(set(persons), {'Alexander Tischenko', 'Alexei Malashkevich', 'Vitaliy Abetkin'})

    @db_session
    def test11(self):
        persons = select(p.full_name for p in Person if p.cars_by_color2(p.favorite_color))[:]
        self.assertEqual(set(persons), {'Alexander Kozlovsky'})

    @db_session
    def test12(self):
        persons = select(p.full_name for p in Person if not p.cars_by_color2(p.favorite_color))[:]
        self.assertEqual(set(persons), {'Alexander Tischenko', 'Alexei Malashkevich', 'Vitaliy Abetkin'})

    @db_session
    def test13(self):
        persons = select(p.full_name for p in Person if count(p.cars_by_color1('white')) > 1)
        self.assertEqual(set(persons), {'Alexander Tischenko'})

    @db_session
    def test14(self):
        # This test checks if accessing function-specific globals works correctly
        persons = select(p.incorrect_full_name for p in Person if p.has_car)[:]
        self.assertEqual(set(persons), {'Alexander ***', 'Alexei ***', 'Alexander ***'})

    @db_session
    def test15(self):
        # Test repeated use of the same generator with hybrid method/property that uses funciton from external scope
        result = Person.find_by_full_name('Alexander Kozlovsky')
        self.assertEqual(set(obj.last_name for obj in result), {'Kozlovsky'})
        result = Person.find_by_full_name('Alexander Kozlovsky')
        self.assertEqual(set(obj.last_name for obj in result), {'Kozlovsky'})
        result = Person.find_by_full_name('Alexander Tischenko')
        self.assertEqual(set(obj.last_name for obj in result), {'Tischenko'})

    @db_session
    def test16(self):
        result = Person.select(lambda p: p.full_name == 'Alexander Kozlovsky')
        self.assertEqual(set(p.id for p in result), {1})

    @db_session
    def test17(self):
        global sep
        sep = '.'
        try:
            result = Person.select(lambda p: p.full_name == 'Alexander.Kozlovsky')
            self.assertEqual(set(p.id for p in result), {1})
        finally:
            sep = ' '

    @db_session
    def test18(self):
        result = Person.select().filter(lambda p: p.full_name == 'Alexander Kozlovsky')
        self.assertEqual(set(p.id for p in result), {1})

    @db_session
    def test19(self):
        global sep
        sep = '.'
        try:
            result = Person.select().filter(lambda p: p.full_name == 'Alexander.Kozlovsky')
            self.assertEqual(set(p.id for p in result), {1})
        finally:
            sep = ' '

    @db_session
    @raises_exception(TranslationError, 'p.complex_method(...) is too complex to decompile')
    def test_20(self):
        q = select(p.complex_method() for p in Person)[:]

    @db_session
    @raises_exception(TranslationError, 'p.to_dict(...) is too complex to decompile')
    def test_21(self):
        q = select(p.to_dict() for p in Person)[:]

    @db_session
    @raises_exception(TranslationError, 'self.complex_method(...) is too complex to decompile (inside Person.simple_method)')
    def test_22(self):
        q = select(p.simple_method() for p in Person)[:]

    @db_session
    def test_23(self):
        q = select(simple_func(p) for p in Person)[:]

    @db_session
    @raises_exception(TranslationError, 'person.complex_method(...) is too complex to decompile (inside complex_func)')
    def test_24(self):
        q = select(complex_func(p) for p in Person)[:]


if __name__ == '__main__':
    unittest.main()
