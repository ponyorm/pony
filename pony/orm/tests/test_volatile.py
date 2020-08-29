import sys, unittest

from pony.orm import *
from pony.orm.tests.testutils import *
from pony.orm.tests import setup_database, teardown_database


class TestVolatile(unittest.TestCase):
    def setUp(self):
        db = self.db = Database()

        class Item(self.db.Entity):
            name = Required(str)
            index = Required(int, volatile=True)

        setup_database(db)

        with db_session:
            Item(id=1, name='A', index=1)
            Item(id=2, name='B', index=2)
            Item(id=3, name='C', index=3)

    def tearDown(self):
        teardown_database(self.db)

    @db_session
    def test_1(self):
        db = self.db
        Item = db.Item
        db.execute('update "item" set "index" = "index" + 1')
        items = Item.select(lambda item: item.index > 0).order_by(Item.id)[:]
        a, b, c = items
        self.assertEqual(a.index, 2)
        self.assertEqual(b.index, 3)
        self.assertEqual(c.index, 4)
        c.index = 1
        items = Item.select()[:]  # force re-read from the database
        self.assertEqual(c.index, 1)
        self.assertEqual(a.index, 2)
        self.assertEqual(b.index, 3)


    @db_session
    def test_2(self):
        Item = self.db.Item
        item = Item[1]
        item.name = 'X'
        item.flush()
        self.assertEqual(item.index, 1)

if __name__ == '__main__':
    unittest.main()
