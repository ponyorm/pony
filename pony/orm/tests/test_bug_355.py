import unittest

from pony import orm
from pony.py23compat import buffer

class Test(unittest.TestCase):
    def test_1(self):
        db = orm.Database('sqlite', ':memory:')

        class Buf(db.Entity):
            pk = orm.PrimaryKey(buffer)

        db.generate_mapping(create_tables=True)

        x = buffer(b'123')

        with orm.db_session:
            Buf(pk=x)
            orm.commit()

        with orm.db_session:
            t = Buf[x]

