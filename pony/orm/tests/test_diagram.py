import unittest
from pony.orm.core import *
from pony.orm.core import Entity
from testutils import *

class TestDiag(unittest.TestCase):

    @raises_exception(ERDiagramError, 'Entity Entity1 already exists')
    def test_entity_duplicate(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            id = PrimaryKey(int)
        class Entity1(db.Entity):
            id = PrimaryKey(int)

    @raises_exception(ERDiagramError, 'Interrelated entities must belong to same database.'
                                    ' Entities Entity2 and Entity1 belongs to different databases')
    def test_diagram1(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            id = PrimaryKey(int)
            attr1 = Required('Entity2')
        db = Database('sqlite', ':memory:')
        class Entity2(db.Entity):
            id = PrimaryKey(int)
            attr2 = Optional(Entity1)

    @raises_exception(ERDiagramError, 'Entity definition Entity2 was not found')
    def test_diagram2(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            id = PrimaryKey(int)
            attr1 = Required('Entity2')
        db.generate_mapping()

    @raises_exception(TypeError, 'Entity1._table_ property must be a string. Got: 123')
    def test_diagram3(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            _table_ = 123
            id = PrimaryKey(int)
        db.generate_mapping()

    def test_diagram4(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            id = PrimaryKey(int)
            attr1 = Set('Entity2', table='Table1')
        class Entity2(db.Entity):
            id = PrimaryKey(int)
            attr2 = Set(Entity1, table='Table1')
        db.generate_mapping(create_tables=True)

    def test_diagram5(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            id = PrimaryKey(int)
            attr1 = Set('Entity2')
        class Entity2(db.Entity):
            id = PrimaryKey(int)
            attr2 = Required(Entity1)
        db.generate_mapping(create_tables=True)

    @raises_exception(MappingError, "Parameter 'table' for Entity1.attr1 and Entity2.attr2 do not match")
    def test_diagram6(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            id = PrimaryKey(int)
            attr1 = Set('Entity2', table='Table1')
        class Entity2(db.Entity):
            id = PrimaryKey(int)
            attr2 = Set(Entity1, table='Table2')
        db.generate_mapping()

    @raises_exception(MappingError, "Table name 'Table1' is already in use")
    def test_diagram7(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            _table_ = 'Table1'
            id = PrimaryKey(int)
            attr1 = Set('Entity2', table='Table1')
        class Entity2(db.Entity):
            id = PrimaryKey(int)
            attr2 = Set(Entity1, table='Table1')
        db.generate_mapping()

    def test_diagram8(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            id = PrimaryKey(int)
            attr1 = Set('Entity2')
        class Entity2(db.Entity):
            id = PrimaryKey(int)
            attr2 = Set(Entity1)
        db.generate_mapping(create_tables=True)
        m2m_table = db.schema.tables['Entity1_Entity2']
        col_names = set([ col.name for col in m2m_table.column_list ])
        self.assertEqual(col_names, set(['entity1', 'entity2']))
        self.assertEqual(Entity1.attr1.get_m2m_columns(), ['entity1'])

    def test_diagram9(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            a = Required(int)
            b = Required(str)
            PrimaryKey(a, b)
            attr1 = Set('Entity2')
        class Entity2(db.Entity):
            id = PrimaryKey(int)
            attr2 = Set(Entity1)
        db.generate_mapping(create_tables=True)
        m2m_table = db.schema.tables['Entity1_Entity2']
        col_names = set([ col.name for col in m2m_table.column_list ])
        self.assertEqual(col_names, set(['entity1_a', 'entity1_b', 'entity2']))

    def test_diagram10(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            a = Required(int)
            b = Required(str)
            PrimaryKey(a, b)
            attr1 = Set('Entity2', column='z')
        class Entity2(db.Entity):
            id = PrimaryKey(int)
            attr2 = Set(Entity1, columns=['x', 'y'])
        db.generate_mapping(create_tables=True)

    @raises_exception(MappingError, 'Invalid number of columns for Entity2.attr2')
    def test_diagram11(self):
        db = Database('sqlite', ':memory:')
        class Entity1(db.Entity):
            a = Required(int)
            b = Required(str)
            PrimaryKey(a, b)
            attr1 = Set('Entity2', column='z')
        class Entity2(db.Entity):
            id = PrimaryKey(int)
            attr2 = Set(Entity1, columns=['x'])
        db.generate_mapping()

    @raises_exception(ERDiagramError, 'Base Entity does not belong to any database')
    def test_diagram12(self):
        class Test(Entity):
        	name = Required(unicode)

if __name__ == '__main__':
    unittest.main()