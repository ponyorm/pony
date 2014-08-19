from __future__ import absolute_import, print_function, division

import unittest

from pony.orm.core import *
from pony.orm.tests.testutils import *

class TestOneToOne3(unittest.TestCase):
    def setUp(self):
        self.db = Database('sqlite', ':memory:')

        class CommonItem(self.db.Entity):
            url = Required(unicode)
            act_item1 = Optional("ActItem1")

        class ActItem1(self.db.Entity):
            regnum = Required(unicode)
            common_item = Required("CommonItem")

        self.db.generate_mapping(create_tables=True)

        with db_session:
            c1 = CommonItem(url='http://example.com')
            a1 = ActItem1(regnum='r1', common_item=c1)

    def tearDown(self):
        self.db = None

    def test_1(self):
        with db_session:
            obj = select(r for r in self.db.CommonItem if r.act_item1.id).first()
            self.assertEqual(obj.url, 'http://example.com')
            self.assertEqual(obj.act_item1.regnum, 'r1')

    def test_2(self):
        with db_session:
            select(r for r in self.db.CommonItem if r.act_item1 is None)[:]
            sql = self.db.last_sql
            self.assertEqual(sql, '''SELECT "r"."id", "r"."url"
FROM "CommonItem" "r"
  LEFT JOIN "ActItem1" "actitem1-1"
    ON "r"."id" = "actitem1-1"."common_item"
WHERE "actitem1-1"."id" IS NULL''')

    def test_3(self):
        with db_session:
            select(r for r in self.db.CommonItem if not r.act_item1)[:]
            sql = self.db.last_sql
            self.assertEqual(sql, '''SELECT "r"."id", "r"."url"
FROM "CommonItem" "r"
  LEFT JOIN "ActItem1" "actitem1-1"
    ON "r"."id" = "actitem1-1"."common_item"
WHERE "actitem1-1"."id" IS NULL''')

    def test_4(self):
        with db_session:
            select(r for r in self.db.CommonItem if r.act_item1)[:]
            sql = self.db.last_sql
            self.assertEqual(sql, '''SELECT "r"."id", "r"."url"
FROM "CommonItem" "r"
  LEFT JOIN "ActItem1" "actitem1-1"
    ON "r"."id" = "actitem1-1"."common_item"
WHERE "actitem1-1"."id" IS NOT NULL''')

if __name__ == '__main__':
    unittest.main()
