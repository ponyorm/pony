import unittest
from pony.orm3 import *
from pony.db import Database
from testutils import *

class TestDiag(unittest.TestCase):

    def setUp(self):
        self.db = Database('sqlite', ':memory:')

    @raises_exception(DiagramError, 'Entity Entity1 already exists')
    def test_entity_duplicate(self):
        _diagram_ = Diagram()
        class Entity1(Entity):
            id = PrimaryKey(int)
        class Entity1(Entity):
            id = PrimaryKey(int)

    @raises_exception(DiagramError, 'Interrelated entities must belong to same diagram.'
                                    ' Entities Entity2 and Entity1 belongs to different diagrams')
    def test_diagram1(self):
        _diagram_ = Diagram()
        class Entity1(Entity):
            id = PrimaryKey(int)
            attr1 = Required('Entity2')
        _diagram_ = Diagram()
        class Entity2(Entity):
            id = PrimaryKey(int)
            attr2 = Optional(Entity1)

    @raises_exception(DiagramError, 'Entity definition Entity2 was not found')
    def test_diagram2(self):
        _diagram_ = Diagram()
        class Entity1(Entity):
            id = PrimaryKey(int)
            attr1 = Required('Entity2')
        generate_mapping(self.db, check_tables=False)

    @raises_exception(TypeError, 'Entity1._table_ property must be a string. Got: 123')
    def test_diagram3(self):
        _diagram_ = Diagram()
        class Entity1(Entity):
            _table_ = 123
            id = PrimaryKey(int)
        generate_mapping(self.db, check_tables=False)

    def test_diagram4(self):
        _diagram_ = Diagram()
        class Entity1(Entity):
            id = PrimaryKey(int)
            attr1 = Set('Entity2', table='Table1')
        class Entity2(Entity):
            id = PrimaryKey(int)
            attr2 = Set(Entity1, table='Table1')
        generate_mapping(self.db, check_tables=False)

    def test_diagram5(self):
        _diagram_ = Diagram()
        class Entity1(Entity):
            id = PrimaryKey(int)
            attr1 = Set('Entity2')
        class Entity2(Entity):
            id = PrimaryKey(int)
            attr2 = Required(Entity1)
        generate_mapping(self.db, check_tables=False)

    @raises_exception(MappingError, "Parameter 'table' for Entity1.attr1 and Entity2.attr2 do not match")
    def test_diagram6(self):
        _diagram_ = Diagram()
        class Entity1(Entity):
            id = PrimaryKey(int)
            attr1 = Set('Entity2', table='Table1')
        class Entity2(Entity):
            id = PrimaryKey(int)
            attr2 = Set(Entity1, table='Table2')
        generate_mapping(self.db, check_tables=False)

    @raises_exception(MappingError, "Table name 'Table1' is already in use")
    def test_diagram7(self):
        _diagram_ = Diagram()
        class Entity1(Entity):
            _table_ = 'Table1'
            id = PrimaryKey(int)
            attr1 = Set('Entity2', table='Table1')
        class Entity2(Entity):
            id = PrimaryKey(int)
            attr2 = Set(Entity1, table='Table1')
        generate_mapping(self.db, check_tables=False)

    def test_diagram8(self):
        _diagram_ = Diagram()
        class Entity1(Entity):
            id = PrimaryKey(int)
            attr1 = Set('Entity2')
        class Entity2(Entity):
            id = PrimaryKey(int)
            attr2 = Set(Entity1)
        generate_mapping(self.db, check_tables=False)
        m2m_table = _diagram_.mapping.tables['Entity1_Entity2']
        col_names = set([ col.name for col in m2m_table.column_list ])
        self.assertEquals(col_names, set(['entity1', 'entity2']))
        self.assertEquals(Entity1.attr1.get_m2m_columns(), ['entity1'])

    def test_diagram9(self):
        _diagram_ = Diagram()
        class Entity1(Entity):
            a = Required(int)
            b = Required(str)
            PrimaryKey(a, b)
            attr1 = Set('Entity2')
        class Entity2(Entity):
            id = PrimaryKey(int)
            attr2 = Set(Entity1)
        generate_mapping(self.db, check_tables=False)
        m2m_table = _diagram_.mapping.tables['Entity1_Entity2']
        col_names = set([ col.name for col in m2m_table.column_list ])
        self.assertEquals(col_names, set(['entity1_a', 'entity1_b', 'entity2']))

    def test_diagram10(self):
        _diagram_ = Diagram()
        class Entity1(Entity):
            a = Required(int)
            b = Required(str)
            PrimaryKey(a, b)
            attr1 = Set('Entity2', column='z')
        class Entity2(Entity):
            id = PrimaryKey(int)
            attr2 = Set(Entity1, columns=['x', 'y'])
        generate_mapping(self.db, check_tables=False)

    @raises_exception(MappingError, 'Invalid number of columns for Entity2.attr2')
    def test_diagram11(self):
        _diagram_ = Diagram()
        class Entity1(Entity):
            a = Required(int)
            b = Required(str)
            PrimaryKey(a, b)
            attr1 = Set('Entity2', column='z')
        class Entity2(Entity):
            id = PrimaryKey(int)
            attr2 = Set(Entity1, columns=['x'])
        generate_mapping(self.db, check_tables=False)


if __name__ == '__main__':
    unittest.main()