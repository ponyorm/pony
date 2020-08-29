from __future__ import absolute_import, print_function, division

import unittest

from pony.orm.core import *
from pony.orm.tests.testutils import *
from pony.orm.tests import setup_database, teardown_database

db = Database()

class Person(db.Entity):
    name = Required(unicode)
    passport = Optional("Passport")

class Passport(db.Entity):
    code = Required(unicode)
    person = Required("Person")

class TestOneToOne4(unittest.TestCase):
    def setUp(self):
        setup_database(db)
        with db_session:
            p1 = Person(id=1, name='John')
            Passport(id=1, code='123', person=p1)

    def tearDown(self):
        teardown_database(db)

    @raises_exception(ConstraintError, 'Cannot unlink Passport[1] from previous Person[1] object, because Passport.person attribute is required')
    @db_session
    def test1(self):
        p2 = Person(id=2, name='Mike')
        pas2 = Passport(id=2, code='456', person=p2)
        commit()
        p1 = Person.get(name='John')
        pas2.person = p1

if __name__ == '__main__':
    unittest.main()
