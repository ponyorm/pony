import sys, unittest

from pony.orm import *
from pony.orm.tests.testutils import *

class TestVolatile(unittest.TestCase):
    def setUp(self):
        db = self.db = Database('sqlite', ':memory:')

        class Item(self.db.Entity):
            name = Required(str)
            index = Required(int, volatile=True)

        db.generate_mapping(create_tables=True)

        with db_session:
            Item(name='A', index=1)
            Item(name='B', index=2)
            Item(name='C', index=3)

    @db_session
    def test_1(self):
        db = self.db
        Item = db.Item
        db.execute('update "Item" set "index" = "index" + 1')
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
