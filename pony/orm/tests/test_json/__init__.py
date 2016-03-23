
from pony.orm import *

class SetupTest(object):

    E = NotImplemented

    @classmethod
    def setUpClass(cls):
        cls.bindDb()
        cls.prepareDb()
        cls.db.generate_mapping(create_tables=True)

    @classmethod
    def tearDownClass(cls):
        with db_session:
            cls.db.execute("""
                drop table e
            """)

    @db_session
    def tearDown(self):
        select(m for m in self.E).delete()

    @classmethod
    def prepareDb(cls):
        class E(cls.db.Entity):
            article = Required(str)
            info = Optional(ormtypes.Json)
            extra_info = Optional(ormtypes.Json)
            zero = Optional(int)

        cls.M = cls.E = E

    @db_session
    def setUp(self):
        info = [
            'description',
            4,
            {'size': '100x50'},
            ['item1', 'item2', 'smth', 'else'],
        ]
        extra_info = {'info': ['warranty 1 year', '2 weeks testing']}
        self.E(article='A-347', info=info, extra_info=extra_info)



