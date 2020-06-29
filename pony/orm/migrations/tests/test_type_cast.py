import unittest
from datetime import datetime, date, time, timedelta
from decimal import Decimal
from uuid import UUID
from pony.orm import *
from pony.orm.migrations import VirtualDB, Migration
from collections import defaultdict


class TestTypeCast(unittest.TestCase):
    db_params = dict(provider='postgres', user='ponytest', password='ponytest', host='localhost', database='ponytest')

    def tearDown(self):
        self.db.drop_all_tables()
        self.db2.drop_all_tables()

    def apply_migrate(self, rename_map=None):
        self.db2.generate_mapping(check_tables=False)
        base_vdb = self.db.vdb
        tmp_vdb = self.db2.vdb

        new_vdb = VirtualDB.from_db(self.db)
        new_vdb.schema = self.db.provider.vdbschema_cls.from_vdb(new_vdb, self.db.provider)

        m = Migration.make(base_vdb, tmp_vdb, rename_map)
        new_vdb.vdb_only = False
        for op in m.operations:
            op.apply(new_vdb)
        expected_schema = tmp_vdb.schema
        actual_schema = new_vdb.schema
        with db_session:
            connection = self.db.get_connection()
            sql_ops = actual_schema.apply(connection, False, False)

        return expected_schema, actual_schema, m, sql_ops

    def test_change_attr_type_str_to_int(self):
        """
            Changes string attribute "name" in entity "Item" to integer type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(int)

        correct_sql = 'ALTER TABLE "item" ALTER COLUMN "name" TYPE INTEGER USING "name"::INTEGER'

        migration_op = "ChangeColumnType(entity_name='Item', attr_name='name', new_options={'py_type': int}, cast_sql='{colname}::INTEGER')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_attr_type_str_to_float(self):
        """
            Changes string attribute "name" in entity "Item" to float type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(float)

        correct_sql = ''

        migration_op = ""

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_attr_type_str_to_decimal(self):
        """
            Changes string attribute "name" in entity "Item" to decimal type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(Decimal)

        correct_sql = ''

        migration_op = ""

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_attr_type_str_to_datetime(self):
        """
            Changes string attribute "name" in entity "Item" to datetime type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(datetime)

        correct_sql = ''

        migration_op = ""

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_attr_type_str_to_date(self):
        """
            Changes string attribute "name" in entity "Item" to date type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(date)

        correct_sql = ''

        migration_op = ""

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_attr_type_str_to_time(self):
        """
            Changes string attribute "name" in entity "Item" to time type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(time)

        correct_sql = ''

        migration_op = ""

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_attr_type_str_to_timedelta(self):
        """
            Changes string attribute "name" in entity "Item" to timedelta type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(timedelta)

        correct_sql = ''

        migration_op = ""

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_attr_type_str_to_bool(self):
        """
            Changes string attribute "name" in entity "Item" to bool type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(bool)

        correct_sql = ''

        migration_op = ""

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_attr_type_str_to_bytes(self):
        """
            Changes string attribute "name" in entity "Item" to bytes type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(bytes)

        correct_sql = ''

        migration_op = ""

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_attr_type_str_to_longstr(self):
        """
            Changes string attribute "name" in entity "Item" to longstr type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(LongStr)

        correct_sql = ''

        migration_op = ""

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_attr_type_str_to_uuid(self):
        """
            Changes string attribute "name" in entity "Item" to uuid type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(UUID)

        correct_sql = ''

        migration_op = ""

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_attr_type_str_to_json(self):
        """
            Changes string attribute "name" in entity "Item" to json type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(Json)

        correct_sql = ''

        migration_op = ""

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual(expected_schema, actual_schema)

if __name__ == '__main__':
    unittest.main()
