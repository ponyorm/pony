import unittest
from binascii import unhexlify

from pony.orm.core import *
from pony.orm.tests import only_for
from pony.orm.tests.testutils import *

db = Database("sqlite", ":memory:")


class Person(db.Entity):
    name = Required(str)
    age = Optional(int)
    image = Optional(buffer)


db.generate_mapping(create_tables=True)

with db_session:
    p1 = Person(name="John", age=20, image=unhexlify("abcdef"))
    p2 = Person(name="Иван")  # u'\u0418\u0432\u0430\u043d'


@only_for("sqlite")
class TestUnicode(unittest.TestCase):
    @db_session
    def test1(self):
        names = select(p.name for p in Person).order_by(lambda: p.id)[:]  # type: ignore # deprecated syntax
        self.assertEqual(names, ["John", "Иван"])

    @db_session
    def test2(self):
        names = select(p.name.upper() for p in Person).order_by(lambda: p.id)[:]  # type: ignore # deprecated syntax
        self.assertEqual(names, ["JOHN", "ИВАН"])  # u'\u0418\u0412\u0410\u041d'

    @db_session
    def test3(self):
        names = select(p.name.lower() for p in Person).order_by(lambda: p.id)[:]  # type: ignore # deprecated syntax
        self.assertEqual(names, ["john", "иван"])  # u'\u0438\u0432\u0430\u043d'

    @db_session
    def test4(self):
        ages = db.select("select py_upper(age) from person")
        self.assertEqual(ages, ["20", None])

    @db_session
    def test5(self):
        ages = db.select("select py_lower(age) from person")
        self.assertEqual(ages, ["20", None])

    @db_session
    def test6(self):
        ages = db.select("select py_upper(image) from person")
        self.assertEqual(ages, ["ABCDEF", None])

    @db_session
    def test7(self):
        ages = db.select("select py_lower(image) from person")
        self.assertEqual(ages, ["abcdef", None])


if __name__ == "__main__":
    unittest.main()
