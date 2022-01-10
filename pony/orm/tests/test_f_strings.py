import unittest
from pony.orm.core import *
from pony.orm.tests.testutils import *
from pony.orm.tests import setup_database, teardown_database


db = Database()

class Person(db.Entity):
    first_name = Required(str)
    last_name = Required(str)
    age = Optional(int)
    value = Required(float)


class TestFString(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_database(db)
        with db_session:
            Person(id=1, first_name='Alexander', last_name='Tischenko', age=23, value=1.4)
            Person(id=2, first_name='Alexander', last_name='Kozlovskiy', age=42, value=1.2)
            Person(id=3, first_name='Arthur', last_name='Pendragon', age=54, value=1.33)
            Person(id=4, first_name='Okita', last_name='Souji', age=15, value=2.1)
            Person(id=5, first_name='Musashi', last_name='Miyamoto', age=None, value=0.9)
            Person(id=6, first_name='Jeanne', last_name="d'Arc", age=30, value=43.212)

    @classmethod
    def tearDownClass(cls):
        teardown_database(db)

    def setUp(self):
        rollback()
        db_session.__enter__()
    def tearDown(self):
        rollback()
        db_session.__exit__()

    def test_1(self):
        x = 'Alexander'
        y = 'Tischenko'

        q = select(p.id for p in Person if p.first_name + ' ' + p.last_name == f'{x} {y}')
        self.assertEqual(set(q), {1})

        q2 = select("""p.id for p in Person if p.first_name + ' ' + p.last_name == f'{x} {y}'""")
        self.assertEqual(set(q2), {1})

    def test_2(self):
        q = select(p.id for p in Person if f'{p.first_name} {p.last_name}' == 'Alexander Tischenko')
        self.assertEqual(set(q), {1})

        q2 = select("""p.id for p in Person if f'{p.first_name} {p.last_name}' == 'Alexander Tischenko'""")
        self.assertEqual(set(q2), {1})

    def test_3(self):
        x = 'Great'

        q = select(f'{p.first_name!s} the {x}' for p in Person if p.id == 1)
        self.assertEqual(set(q), {'Alexander the Great'})

        q2 = select("""f'{p.first_name!s} the {x}' for p in Person if p.id == 1""")
        self.assertEqual(set(q2), {'Alexander the Great'})

    def test_4(self):
        q = select(f'{p.first_name} {p.age}' for p in Person if p.id == 1)
        self.assertEqual(set(q), {'Alexander 23'})

        q2 = select("""f'{p.first_name} {p.age}' for p in Person if p.id == 1""")
        self.assertEqual(set(q2), {'Alexander 23'})

    def test_5(self):
        q = select(f'{p.first_name} {p.age}' for p in Person if p.id == 1)
        self.assertEqual(set(q), {'Alexander 23'})

        q2 = select("""f'{p.first_name} {p.age}' for p in Person if p.id == 1""")
        self.assertEqual(set(q2), {'Alexander 23'})

    @raises_exception(NotImplementedError, 'You cannot set width and precision for f-string expression in query')
    def test_6a(self):
        width = 3
        precision = 4
        q = select(p.id for p in Person if f'{p.value:{width}.{precision}}')[:]

    @raises_exception(NotImplementedError, 'You cannot set width and precision for f-string expression in query')
    def test_6b(self):
        width = 3
        precision = 4
        q = select("""p.id for p in Person if f'{p.value:{width}.{precision}}'""")[:]
        q.show()

    @raises_exception(NotImplementedError, 'You cannot specify conversion type for f-string expression in query')
    def test_7a(self):
        width = 3
        precision = 4
        q = select(p.id for p in Person if f'{p.value!r}')[:]

    @raises_exception(NotImplementedError, 'You cannot specify conversion type for f-string expression in query')
    def test_7b(self):
        width = 3
        precision = 4
        q = select("""p.id for p in Person if f'{p.value!r}'""")[:]
        q.show()

    def test_8(self):
        x = 'Tischenko'
        q = select(p.first_name + f"{' ' + x}" for p in Person if p.id == 1)
        self.assertEqual(set(q), {'Alexander Tischenko'})

        q2 = select("""p.first_name + f"{' ' + x}" for p in Person if p.id == 1""")
        self.assertEqual(set(q2), {'Alexander Tischenko'})
