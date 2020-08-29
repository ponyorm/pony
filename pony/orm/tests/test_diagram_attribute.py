from __future__ import absolute_import, print_function, division
from pony.py23compat import PY2
from datetime import date
import unittest

from pony.orm.core import *
from pony.orm.core import Attribute
from pony.orm.tests.testutils import *
from pony.orm.tests import db_params, only_for, setup_database, teardown_database


class TestAttribute(unittest.TestCase):
    def setUp(self):
        self.db = Database(**db_params)

    def tearDown(self):
        teardown_database(self.db)

    @raises_exception(TypeError, "Attribute Entity1.id has unknown option 'another_option'")
    def test_attribute1(self):
        db = self.db
        class Entity1(db.Entity):
            id = PrimaryKey(int, another_option=3)
        db.generate_mapping(check_tables=False)

    @raises_exception(TypeError, 'Cannot link attribute Entity1.b to abstract Entity class. Use specific Entity subclass instead')
    def test_attribute2(self):
        db = self.db
        class Entity1(db.Entity):
            id = PrimaryKey(int)
            b = Required(db.Entity)
        db.generate_mapping(check_tables=False)

    @raises_exception(TypeError, 'Default value for required attribute Entity1.b cannot be None')
    def test_attribute3(self):
        db = self.db
        class Entity1(db.Entity):
            id = PrimaryKey(int)
            b = Required(int, default=None)

    def test_attribute4(self):
        db = self.db
        class Entity1(db.Entity):
            id = PrimaryKey(int)
            attr1 = Required('Entity2', reverse='attr2')
        class Entity2(db.Entity):
            id = PrimaryKey(int)
            attr2 = Optional(Entity1)
        db.generate_mapping(check_tables=False)
        self.assertEqual(Entity1.attr1.reverse, Entity2.attr2)

    def test_attribute5(self):
        db = self.db
        class Entity1(db.Entity):
            id = PrimaryKey(int)
            attr1 = Required('Entity2')
        class Entity2(db.Entity):
            id = PrimaryKey(int)
            attr2 = Optional(Entity1, reverse=Entity1.attr1)
        self.assertEqual(Entity2.attr2.reverse, Entity1.attr1)

    @raises_exception(TypeError, "Value of 'reverse' option must be name of reverse attribute). Got: 123")
    def test_attribute6(self):
        db = self.db
        class Entity1(db.Entity):
            id = PrimaryKey(int)
            attr1 = Required('Entity2', reverse=123)

    @raises_exception(TypeError, "Reverse option cannot be set for this type: %r" % str)
    def test_attribute7(self):
        db = self.db
        class Entity1(db.Entity):
            id = PrimaryKey(int)
            attr1 = Required(str, reverse='attr1')

    @raises_exception(TypeError, "'Attribute' is abstract type")
    def test_attribute8(self):
        db = self.db
        class Entity1(db.Entity):
            id = PrimaryKey(int)
            attr1 = Attribute(str)

    @raises_exception(ERDiagramError, "Attribute name cannot both start and end with underscore. Got: _attr1_")
    def test_attribute9(self):
        db = self.db
        class Entity1(db.Entity):
            id = PrimaryKey(int)
            _attr1_ = Required(str)

    @raises_exception(ERDiagramError, "Duplicate use of attribute Entity1.attr1 in entity Entity2")
    def test_attribute10(self):
        db = self.db
        class Entity1(db.Entity):
            id = PrimaryKey(int)
            attr1 = Required(str)
        class Entity2(db.Entity):
            id = PrimaryKey(int)
            attr2 = Entity1.attr1

    @raises_exception(ERDiagramError, "Invalid use of attribute Entity1.a in entity Entity2")
    def test_attribute11(self):
        db = self.db
        class Entity1(db.Entity):
            a = Required(str)
        class Entity2(db.Entity):
            b = Required(str)
            composite_key(Entity1.a, b)

    @raises_exception(ERDiagramError, "Cannot create default primary key attribute for Entity1 because name 'id' is already in use."
                                      " Please create a PrimaryKey attribute for entity Entity1 or rename the 'id' attribute")
    def test_attribute12(self):
        db = self.db
        class Entity1(db.Entity):
            id = Optional(str)

    @raises_exception(ERDiagramError, "Reverse attribute for Entity1.attr1 not found")
    def test_attribute13(self):
        db = self.db
        class Entity1(db.Entity):
            id = PrimaryKey(int)
            attr1 = Required('Entity2')
        class Entity2(db.Entity):
            id = PrimaryKey(int)
        db.generate_mapping(check_tables=False)

    @raises_exception(ERDiagramError, "Reverse attribute Entity1.attr1 not found")
    def test_attribute14(self):
        db = self.db
        class Entity1(db.Entity):
            id = PrimaryKey(int)
        class Entity2(db.Entity):
            id = PrimaryKey(int)
            attr2 = Required(Entity1, reverse='attr1')
        db.generate_mapping(check_tables=False)

    @raises_exception(ERDiagramError, "Inconsistent reverse attributes Entity3.attr3 and Entity2.attr2")
    def test_attribute15(self):
        db = self.db
        class Entity1(db.Entity):
            id = PrimaryKey(int)
            attr1 = Optional('Entity2')
        class Entity2(db.Entity):
            id = PrimaryKey(int)
            attr2 = Required(Entity1)
        class Entity3(db.Entity):
            id = PrimaryKey(int)
            attr3 = Required(Entity2, reverse='attr2')
        db.generate_mapping(check_tables=False)

    @raises_exception(ERDiagramError, "Inconsistent reverse attributes Entity3.attr3 and Entity2.attr2")
    def test_attribute16(self):
        db = self.db
        class Entity1(db.Entity):
            id = PrimaryKey(int)
            attr1 = Optional('Entity2')
        class Entity2(db.Entity):
            id = PrimaryKey(int)
            attr2 = Required(Entity1)
        class Entity3(db.Entity):
            id = PrimaryKey(int)
            attr3 = Required(Entity2, reverse=Entity2.attr2)
        db.generate_mapping(check_tables=False)

    @raises_exception(ERDiagramError, 'Reverse attribute for Entity2.attr2 not found')
    def test_attribute18(self):
        db = self.db
        class Entity1(db.Entity):
            id = PrimaryKey(int)
        class Entity2(db.Entity):
            id = PrimaryKey(int)
            attr2 = Required('Entity1')
        db.generate_mapping(check_tables=False)

    @raises_exception(ERDiagramError, "Ambiguous reverse attribute for Entity1.a. Use the 'reverse' parameter for pointing to right attribute")
    def test_attribute19(self):
        db = self.db
        class Entity1(db.Entity):
            id = PrimaryKey(int)
            a = Required('Entity2')
            b = Optional('Entity2')
        class Entity2(db.Entity):
            id = PrimaryKey(int)
            c = Set(Entity1)
            d = Set(Entity1)
        db.generate_mapping(check_tables=False)

    @raises_exception(ERDiagramError, "Ambiguous reverse attribute for Entity1.c. Use the 'reverse' parameter for pointing to right attribute")
    def test_attribute20(self):
        db = self.db
        class Entity1(db.Entity):
            id = PrimaryKey(int)
            c = Set('Entity2')
        class Entity2(db.Entity):
            id = PrimaryKey(int)
            a = Required(Entity1, reverse='c')
            b = Optional(Entity1, reverse='c')
        db.generate_mapping(check_tables=False)

    def test_attribute21(self):
        db = self.db
        class Entity1(db.Entity):
            id = PrimaryKey(int)
            a = Required('Entity2', reverse='c')
            b = Optional('Entity2')
        class Entity2(db.Entity):
            id = PrimaryKey(int)
            c = Set(Entity1)
            d = Set(Entity1)

    def test_attribute22(self):
        db = self.db
        class Entity1(db.Entity):
            id = PrimaryKey(int)
            a = Required('Entity2', reverse='c')
            b = Optional('Entity2')
        class Entity2(db.Entity):
            id = PrimaryKey(int)
            c = Set(Entity1, reverse='a')
            d = Set(Entity1)

    @raises_exception(ERDiagramError, 'Inconsistent reverse attributes Entity1.a and Entity2.b')
    def test_attribute23(self):
        db = self.db
        class Entity1(db.Entity):
            a = Required('Entity2', reverse='b')
        class Entity2(db.Entity):
            b = Optional('Entity3')
        class Entity3(db.Entity):
            c = Required('Entity2')
        db.generate_mapping(check_tables=False)

    @raises_exception(ERDiagramError, 'Inconsistent reverse attributes Entity1.a and Entity2.c')
    def test_attribute23(self):
        db = self.db
        class Entity1(db.Entity):
            a = Required('Entity2', reverse='c')
            b = Required('Entity2', reverse='d')
        class Entity2(db.Entity):
            c = Optional('Entity1', reverse='b')
            d = Optional('Entity1', reverse='a')
        db.generate_mapping(check_tables=False)

    def test_attribute24(self):
        db = self.db
        class Entity1(db.Entity):
            a = PrimaryKey(str, auto=True)
        db.generate_mapping(create_tables=True)
        table_name = 'Entity1' if db.provider.dialect == 'SQLite' and pony.__version__ < '0.9' else 'entity1'
        self.assertTrue('AUTOINCREMENT' not in db.schema.tables[table_name].get_create_command())

    @raises_exception(TypeError, "Parameters 'column' and 'columns' cannot be specified simultaneously")
    def test_columns1(self):
        db = self.db
        class Entity1(db.Entity):
            id = PrimaryKey(int)
            attr1 = Optional("Entity2", column='a', columns=['b', 'c'])
        class Entity2(db.Entity):
            id = PrimaryKey(int)
            attr2 = Optional(Entity1)
        db.generate_mapping(check_tables=False)

    def test_columns2(self):
        db = self.db
        class Entity1(db.Entity):
            id = PrimaryKey(int, column='a')
        db.generate_mapping(check_tables=False)
        self.assertEqual(Entity1.id.columns, ['a'])

    def test_columns3(self):
        db = self.db
        class Entity1(db.Entity):
            id = PrimaryKey(int, columns=['a'])
        self.assertEqual(Entity1.id.column, 'a')

    @raises_exception(MappingError, "Too many columns were specified for Entity1.id")
    def test_columns5(self):
        db = self.db
        class Entity1(db.Entity):
            id = PrimaryKey(int, columns=['a', 'b'])
        db.generate_mapping(check_tables=False)

    @raises_exception(TypeError, "Parameter 'columns' must be a list. Got: %r'" % {'a'})
    def test_columns6(self):
        db = self.db
        class Entity1(db.Entity):
            id = PrimaryKey(int, columns={'a'})
        db.generate_mapping(check_tables=False)

    @raises_exception(TypeError, "Parameter 'column' must be a string. Got: 4")
    def test_columns7(self):
        db = self.db
        class Entity1(db.Entity):
            id = PrimaryKey(int, column=4)
        db.generate_mapping(check_tables=False)

    def test_columns8(self):
        db = self.db
        class Entity1(db.Entity):
            a = Required(int)
            b = Required(int)
            attr1 = Optional('Entity2')
            PrimaryKey(a, b)
        class Entity2(db.Entity):
            attr2 = Required(Entity1, columns=['x', 'y'])
        db.generate_mapping(check_tables=False)
        self.assertEqual(Entity2.attr2.column, None)
        self.assertEqual(Entity2.attr2.columns, ['x', 'y'])

    @raises_exception(MappingError, 'Invalid number of columns specified for Entity2.attr2')
    def test_columns9(self):
        db = self.db
        class Entity1(db.Entity):
            a = Required(int)
            b = Required(int)
            attr1 = Optional('Entity2')
            PrimaryKey(a, b)
        class Entity2(db.Entity):
            attr2 = Required(Entity1, columns=['x', 'y', 'z'])
        db.generate_mapping(check_tables=False)

    @raises_exception(MappingError, 'Invalid number of columns specified for Entity2.attr2')
    def test_columns10(self):
        db = self.db
        class Entity1(db.Entity):
            a = Required(int)
            b = Required(int)
            attr1 = Optional('Entity2')
            PrimaryKey(a, b)
        class Entity2(db.Entity):
            attr2 = Required(Entity1, column='x')
        db.generate_mapping(check_tables=False)

    @raises_exception(TypeError, "Items of parameter 'columns' must be strings. Got: [1, 2]")
    def test_columns11(self):
        db = self.db
        class Entity1(db.Entity):
            a = Required(int)
            b = Required(int)
            attr1 = Optional('Entity2')
            PrimaryKey(a, b)
        class Entity2(db.Entity):
            attr2 = Required(Entity1, columns=[1, 2])

    def test_columns12(self):
        db = self.db
        class Entity1(db.Entity):
            attr1 = Set('Entity1', reverse='attr1', column='column1', reverse_column='column2', reverse_columns=['column2'])
        db.generate_mapping(check_tables=False)

    @raises_exception(TypeError, "Parameters 'reverse_column' and 'reverse_columns' cannot be specified simultaneously")
    def test_columns13(self):
        db = self.db
        class Entity1(db.Entity):
            attr1 = Set('Entity1', reverse='attr1', column='column1', reverse_column='column2', reverse_columns=['column3'])
        db.generate_mapping(check_tables=False)

    @raises_exception(TypeError, "Parameter 'reverse_column' must be a string. Got: 5")
    def test_columns14(self):
        db = self.db
        class Entity1(db.Entity):
            attr1 = Set('Entity1', reverse='attr1', column='column1', reverse_column=5)
        db.generate_mapping(check_tables=False)

    @raises_exception(TypeError, "Parameter 'reverse_columns' must be a list. Got: 'column3'")
    def test_columns15(self):
        db = self.db
        class Entity1(db.Entity):
            attr1 = Set('Entity1', reverse='attr1', column='column1', reverse_columns='column3')
        db.generate_mapping(check_tables=False)

    @raises_exception(TypeError, "Parameter 'reverse_columns' must be a list of strings. Got: [5]")
    def test_columns16(self):
        db = self.db
        class Entity1(db.Entity):
            attr1 = Set('Entity1', reverse='attr1', column='column1', reverse_columns=[5])
        db.generate_mapping(check_tables=False)

    def test_columns17(self):
        db = self.db
        class Entity1(db.Entity):
            attr1 = Set('Entity1', reverse='attr1', column='column1', reverse_columns=['column2'])
        db.generate_mapping(check_tables=False)

    def test_columns18(self):
        db = self.db
        class Entity1(db.Entity):
            attr1 = Set('Entity1', reverse='attr1', table='T1')
        db.generate_mapping(check_tables=False)

    @raises_exception(TypeError, "Parameter 'table' must be a string. Got: 5")
    def test_columns19(self):
        db = self.db
        class Entity1(db.Entity):
            attr1 = Set('Entity1', reverse='attr1', table=5)
        db.generate_mapping(check_tables=False)

    @raises_exception(TypeError, "Each part of table name must be a string. Got: 1")
    def test_columns20(self):
        db = self.db
        class Entity1(db.Entity):
            attr1 = Set('Entity1', reverse='attr1', table=[1, 'T1'])
        db.generate_mapping(check_tables=False)

    def test_columns_21(self):
        db = self.db
        class Stat(db.Entity):
            webinarshow = Optional('WebinarShow')
        class WebinarShow(db.Entity):
            stats = Required('Stat')
        db.generate_mapping(check_tables=False)
        self.assertEqual(Stat.webinarshow.column, None)
        self.assertEqual(WebinarShow.stats.column, 'stats')

    def test_columns_22(self):
        db = self.db
        class ZStat(db.Entity):
            webinarshow = Optional('WebinarShow')
        class WebinarShow(db.Entity):
            stats = Required('ZStat')
        db.generate_mapping(check_tables=False)
        self.assertEqual(ZStat.webinarshow.column, None)
        self.assertEqual(WebinarShow.stats.column, 'stats')

    def test_nullable1(self):
        db = self.db
        class Entity1(db.Entity):
            a = Optional(unicode, unique=True)
        db.generate_mapping(check_tables=False)
        self.assertEqual(Entity1.a.nullable, True)

    def test_nullable2(self):
        db = self.db
        class Entity1(db.Entity):
            a = Optional(unicode, unique=True)
        setup_database(db)
        with db_session:
            Entity1()
            commit()
            Entity1()
            commit()

    def test_lambda_1(self):
        db = self.db
        class Entity1(db.Entity):
            a = Required(lambda: db.Entity2)
        class Entity2(db.Entity):
            b = Set(lambda: db.Entity1)
        db.generate_mapping(check_tables=False)
        self.assertEqual(Entity1.a.py_type, Entity2)
        self.assertEqual(Entity2.b.py_type, Entity1)

    @raises_exception(TypeError, "Invalid type of attribute Entity1.a: expected entity class, got 'Entity2'")
    def test_lambda_2(self):
        db = self.db
        class Entity1(db.Entity):
            a = Required(lambda: 'Entity2')
        class Entity2(db.Entity):
            b = Set(lambda: db.Entity1)
        db.generate_mapping(check_tables=False)

    @raises_exception(ERDiagramError, 'Interrelated entities must belong to same database. '
                                      'Entities Entity1 and Entity2 belongs to different databases')
    def test_lambda_3(self):
        db1 = Database('sqlite', ':memory:')
        class Entity1(db1.Entity):
            a = Required(lambda: db2.Entity2)
        db2 = Database('sqlite', ':memory:')
        class Entity2(db2.Entity):
            b = Set(lambda: db1.Entity1)
        db1.generate_mapping(check_tables=False)

    @raises_exception(ValueError, 'Check for attribute Entity1.a failed. Value: 1')
    def test_py_check_1(self):
        db = self.db
        class Entity1(db.Entity):
            a = Required(int, py_check=lambda val: val > 5 and val < 10)
        setup_database(db)
        with db_session:
            obj = Entity1(a=1)

    def test_py_check_2(self):
        db = self.db
        class Entity1(db.Entity):
            a = Required(int, py_check=lambda val: val > 5 and val < 10)
        setup_database(db)
        with db_session:
            obj = Entity1(a=7)

    def test_py_check_3(self):
        db = self.db
        class Entity1(db.Entity):
            a = Optional(date, py_check=lambda val: val.year >= 2000)
        setup_database(db)
        with db_session:
            obj = Entity1(a=None)

    @raises_exception(ValueError, 'Check for attribute Entity1.a failed. Value: datetime.date(1999, 1, 1)')
    def test_py_check_4(self):
        db = self.db
        class Entity1(db.Entity):
            a = Optional(date, py_check=lambda val: val.year >= 2000)
        setup_database(db)
        with db_session:
            obj = Entity1(a=date(1999, 1, 1))

    def test_py_check_5(self):
        db = self.db
        class Entity1(db.Entity):
            a = Optional(date, py_check=lambda val: val.year >= 2000)
        setup_database(db)
        with db_session:
            obj = Entity1(a=date(2010, 1, 1))

    @raises_exception(ValueError, 'Should be positive number')
    def test_py_check_6(self):
        def positive_number(val):
            if val <= 0: raise ValueError('Should be positive number')
        db = self.db
        class Entity1(db.Entity):
            a = Optional(int, py_check=positive_number)
        setup_database(db)
        with db_session:
            obj = Entity1(a=-1)

    def test_py_check_7(self):
        def positive_number(val):
            if val <= 0: raise ValueError('Should be positive number')
            return True
        db = self.db
        class Entity1(db.Entity):
            a = Optional(int, py_check=positive_number)
        setup_database(db)
        with db_session:
            obj = Entity1(a=1)

    @raises_exception(NotImplementedError, "'py_check' parameter is not supported for collection attributes")
    def test_py_check_8(self):
        db = self.db
        class Entity1(db.Entity):
            a = Required('Entity2')
        class Entity2(db.Entity):
            a = Set('Entity1', py_check=lambda val: True)
        db.generate_mapping(check_tables=False)

    def test_py_check_truncate(self):
        db = self.db
        class Entity1(db.Entity):
            a = Required(str, py_check=lambda val: False)
        setup_database(db)
        with db_session:
            try:
                obj = Entity1(a='1234567890' * 1000)
            except ValueError as e:
                error_message = "Check for attribute Entity1.a failed. Value: " + (
                    "u'12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345..." if PY2
                    else "'123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456...")
                self.assertEqual(str(e), error_message)
            else:
                self.assert_(False)

    @raises_exception(ValueError, 'Value for attribute Entity1.a is too long. Max length is 10, value length is 10000')
    def test_str_max_len(self):
        db = self.db
        class Entity1(db.Entity):
            a = Required(str, 10)
        setup_database(db)
        with db_session:
            obj = Entity1(a='1234567890' * 1000)

    @only_for('sqlite')
    def test_foreign_key_sql_type_1(self):
        db = self.db
        class Foo(db.Entity):
            id = PrimaryKey(unicode, sql_type='SOME_TYPE')
            bars = Set('Bar')
        class Bar(db.Entity):
            foo = Required(Foo)
        db.generate_mapping(check_tables=False)

        table = db.schema.tables.get(Bar._table_)
        sql_type = table.column_list[1].sql_type
        self.assertEqual(sql_type, 'SOME_TYPE')

    @only_for('sqlite')
    def test_foreign_key_sql_type_2(self):
        db = self.db
        class Foo(db.Entity):
            id = PrimaryKey(unicode, sql_type='SOME_TYPE')
            bars = Set('Bar')
        class Bar(db.Entity):
            foo = Required(Foo, sql_type='ANOTHER_TYPE')
        db.generate_mapping(check_tables=False)

        table = db.schema.tables.get(Bar._table_)
        sql_type = table.column_list[1].sql_type
        self.assertEqual(sql_type, 'ANOTHER_TYPE')

    @only_for('sqlite')
    def test_foreign_key_sql_type_3(self):
        db = self.db
        class Foo(db.Entity):
            id = PrimaryKey(unicode, sql_type='SERIAL')
            bars = Set('Bar')
        class Bar(db.Entity):
            foo = Required(Foo, sql_type='ANOTHER_TYPE')
        db.generate_mapping(check_tables=False)

        table = db.schema.tables.get(Bar._table_)
        sql_type = table.column_list[1].sql_type
        self.assertEqual(sql_type, 'ANOTHER_TYPE')

    def test_foreign_key_sql_type_4(self):
        db = self.db
        class Foo(db.Entity):
            id = PrimaryKey(unicode, sql_type='SERIAL')
            bars = Set('Bar')
        class Bar(db.Entity):
            foo = Required(Foo)
        db.generate_mapping(check_tables=False)

        table = db.schema.tables.get(Bar._table_)
        sql_type = table.column_list[1].sql_type
        required_type = 'INT8' if db.provider_name == 'cockroach' else 'INTEGER'
        self.assertEqual(required_type, sql_type)

    def test_foreign_key_sql_type_5(self):
        db = self.db
        class Foo(db.Entity):
            id = PrimaryKey(unicode, sql_type='serial')
            bars = Set('Bar')
        class Bar(db.Entity):
            foo = Required(Foo)
        db.generate_mapping(check_tables=False)

        table = db.schema.tables.get(Bar._table_)
        sql_type = table.column_list[1].sql_type
        required_type = 'int8' if db.provider_name == 'cockroach' else 'integer'
        self.assertEqual(required_type, sql_type)

    def test_self_referenced_m2m_1(self):
        db = self.db
        class Node(db.Entity):
            id = PrimaryKey(int)
            prev_nodes = Set("Node")
            next_nodes = Set("Node")
        db.generate_mapping(check_tables=False)

    def test_implicit_1(self):
        db = self.db
        class Foo(db.Entity):
            name = Required(str)
            bar = Required("Bar")
        class Bar(db.Entity):
            id = PrimaryKey(int)
            name = Optional(str)
            foos = Set("Foo")
        db.generate_mapping(check_tables=False)

        self.assertTrue(Foo.id.is_implicit)
        self.assertFalse(Foo.name.is_implicit)
        self.assertFalse(Foo.bar.is_implicit)

        self.assertFalse(Bar.id.is_implicit)
        self.assertFalse(Bar.name.is_implicit)
        self.assertFalse(Bar.foos.is_implicit)

    def test_implicit_2(self):
        db = self.db
        class Foo(db.Entity):
            x = Required(str)
        class Bar(Foo):
            y = Required(str)
        db.generate_mapping(check_tables=False)

        self.assertTrue(Foo.id.is_implicit)
        self.assertTrue(Foo.classtype.is_implicit)
        self.assertFalse(Foo.x.is_implicit)

        self.assertTrue(Bar.id.is_implicit)
        self.assertTrue(Bar.classtype.is_implicit)
        self.assertFalse(Bar.x.is_implicit)
        self.assertFalse(Bar.y.is_implicit)

    @raises_exception(TypeError, 'Attribute Foo.x has invalid type NoneType')
    def test_none_type(self):
        db = self.db
        class Foo(db.Entity):
            x = Required(type(None))
        db.generate_mapping(check_tables=False)

    @raises_exception(TypeError, "'sql_default' option value cannot be empty string, "
                                 "because it should be valid SQL literal or expression. "
                                 "Try to use \"''\", or just specify default='' instead.")
    def test_none_type(self):
        db = self.db
        class Foo(db.Entity):
            x = Required(str, sql_default='')


if __name__ == '__main__':
    unittest.main()
