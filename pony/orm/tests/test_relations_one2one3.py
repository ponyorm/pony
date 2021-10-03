from __future__ import absolute_import, print_function, division

import unittest

from pony.orm.core import *
from pony.orm.tests.testutils import *
from pony.orm.tests import setup_database, teardown_database, only_for


@only_for('sqlite')
class TestOneToOne3(unittest.TestCase):
    def setUp(self):
        self.db = Database()

        class Person(self.db.Entity):
            name = Required(unicode)
            passport = Optional("Passport", cascade_delete=True)

        class Passport(self.db.Entity):
            code = Required(unicode)
            person = Required("Person")

        setup_database(self.db)

        with db_session:
            p1 = Person(name='John')
            Passport(code='123', person=p1)

    def tearDown(self):
        teardown_database(self.db)

    @db_session
    def test_1(self):
        obj = select(p for p in self.db.Person if p.passport.id).first()
        self.assertEqual(obj.name, 'John')
        self.assertEqual(obj.passport.code, '123')

    @db_session
    def test_2(self):
        select(p for p in self.db.Person if p.passport is None)[:]
        sql = self.db.last_sql
        self.assertEqual(sql, '''SELECT "p"."id", "p"."name"
FROM "Person" "p"
  LEFT JOIN "Passport" "passport"
    ON "p"."id" = "passport"."person"
WHERE "passport"."id" IS NULL''')

    @db_session
    def test_3(self):
        select(p for p in self.db.Person if not p.passport)[:]
        sql = self.db.last_sql
        self.assertEqual(sql, '''SELECT "p"."id", "p"."name"
FROM "Person" "p"
  LEFT JOIN "Passport" "passport"
    ON "p"."id" = "passport"."person"
WHERE "passport"."id" IS NULL''')

    @db_session
    def test_4(self):
        select(p for p in self.db.Person if p.passport)[:]
        sql = self.db.last_sql
        self.assertEqual(sql, '''SELECT "p"."id", "p"."name"
FROM "Person" "p"
  LEFT JOIN "Passport" "passport"
    ON "p"."id" = "passport"."person"
WHERE "passport"."id" IS NOT NULL''')

    @db_session
    def test_5(self):
        p = self.db.Person.get(name='John')
        p.delete()
        flush()
        sql = self.db.last_sql
        self.assertEqual(sql, '''DELETE FROM "Person"
WHERE "id" = ?
  AND "name" = ?''')

    @raises_exception(ConstraintError, 'Cannot unlink Passport[1] from previous Person[1] object, because Passport.person attribute is required')
    @db_session
    def test_6(self):
        p = self.db.Person.get(name='John')
        self.db.Passport(code='456', person=p)

    @raises_exception(ConstraintError, 'Cannot unlink Passport[1] from previous Person[1] object, because Passport.person attribute is required')
    @db_session
    def test7(self):
        p2 = self.db.Person(name='Mike')
        pas2 = self.db.Passport(code='456', person=p2)
        commit()
        p1 = self.db.Person.get(name='John')
        pas2.person = p1

if __name__ == '__main__':
    unittest.main()
