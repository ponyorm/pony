# coding: utf-8

from __future__ import absolute_import, print_function, division

import unittest

from pony.orm.core import *
from pony.orm.tests.testutils import *

db = Database('sqlite', ':memory:')

class Person(db.Entity):
    name = Required(unicode)

db.generate_mapping(create_tables=True)

with db_session:
    p1 = Person(name='John')
    p2 = Person(name=u'Иван')  # u'\u0418\u0432\u0430\u043d'

class TestUnicode(unittest.TestCase):
    @db_session
    def test1(self):
        names = select(p.name for p in Person).order_by(1)[:]
        self.assertEqual(names, ['John', u'Иван'])

    @db_session
    def test2(self):
        names = select(p.name.upper() for p in Person).order_by(1)[:]
        self.assertEqual(names, ['JOHN', u'ИВАН'])  # u'\u0418\u0412\u0410\u041d'

if __name__ == '__main__':
    unittest.main()
