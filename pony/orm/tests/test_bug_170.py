import unittest

from pony import orm

class Test(unittest.TestCase):
    def test_1(self):
        db = orm.Database('sqlite', ':memory:')

        class Person(db.Entity):
            id = orm.PrimaryKey(int, auto=True)
            name = orm.Required(str)
            orm.composite_key(id, name)

        db.generate_mapping(create_tables=True)

        table = db.schema.tables[Person._table_]
        pk_column = table.column_dict[Person.id.column]
        self.assertTrue(pk_column.is_pk)

        with orm.db_session:
            p1 = Person(name='John')
            p2 = Person(name='Mike')
