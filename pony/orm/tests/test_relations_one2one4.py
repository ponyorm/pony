from __future__ import absolute_import, print_function, division

import unittest

from pony.orm.core import *
from pony.orm.tests.testutils import *

class TestOneToOne4(unittest.TestCase):
    def setUp(self):
        self.db = Database('sqlite', ':memory:')

        class Person(self.db.Entity):
            name = Required(unicode)
            passport = Optional("Passport")

        class Passport(self.db.Entity):
            code = Required(unicode)
            person = Required("Person")

        self.db.generate_mapping(create_tables=True)

        with db_session:
            p1 = Person(name='John')
            Passport(code='123', person=p1)

    def tearDown(self):
        self.db = None

    @raises_exception(ConstraintError, 'Cannot unlink Passport[1] from previous Person[1] object, because Passport.person attribute is required')
    @db_session
    def test1(self):
        p2 = self.db.Person(name='Mike')
        pas2 = self.db.Passport(code='456', person=p2)
        commit()
        p1 = self.db.Person.get(name='John')
        pas2.person = p1

if __name__ == '__main__':
    unittest.main()
