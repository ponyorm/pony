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

        migration_op = "ChangeColumnType(entity_name='Item', attr_name='name', py_type=int, options={}, " \
                       "cast_sql='{colname}::INTEGER')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
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

        correct_sql = 'ALTER TABLE "item" ALTER COLUMN "name" TYPE DOUBLE PRECISION USING "name"::DOUBLE PRECISION'

        migration_op = "ChangeColumnType(entity_name='Item', attr_name='name', py_type=float, options={}, " \
                       "cast_sql='{colname}::DOUBLE PRECISION')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
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

        correct_sql = 'ALTER TABLE "item" ALTER COLUMN "name" TYPE DECIMAL(12, 2) USING "name"::DECIMAL(12, 2)'

        migration_op = "ChangeColumnType(entity_name='Item', attr_name='name', py_type=Decimal, options={}, " \
                       "cast_sql='{colname}::DECIMAL(12, 2)')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
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

        correct_sql = 'ALTER TABLE "item" ALTER COLUMN "name" TYPE TIMESTAMP USING "name"::TIMESTAMP'

        migration_op = "ChangeColumnType(entity_name='Item', attr_name='name', py_type=datetime, options={}, " \
                       "cast_sql='{colname}::TIMESTAMP')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
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

        correct_sql = 'ALTER TABLE "item" ALTER COLUMN "name" TYPE DATE USING "name"::DATE'

        migration_op = "ChangeColumnType(entity_name='Item', attr_name='name', py_type=date, options={}, " \
                       "cast_sql='{colname}::DATE')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
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

        correct_sql = 'ALTER TABLE "item" ALTER COLUMN "name" TYPE TIME USING "name"::TIME'

        migration_op = "ChangeColumnType(entity_name='Item', attr_name='name', py_type=time, options={}, " \
                       "cast_sql='{colname}::TIME')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
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

        correct_sql = 'ALTER TABLE "item" ALTER COLUMN "name" TYPE INTERVAL DAY TO SECOND USING ' \
                      '"name"::INTERVAL DAY TO SECOND'

        migration_op = "ChangeColumnType(entity_name='Item', attr_name='name', py_type=timedelta, options={}, " \
                       "cast_sql='{colname}::INTERVAL DAY TO SECOND')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
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

        correct_sql = 'ALTER TABLE "item" ALTER COLUMN "name" TYPE BOOLEAN USING "name"::BOOLEAN'

        migration_op = "ChangeColumnType(entity_name='Item', attr_name='name', py_type=bool, options={}, " \
                       "cast_sql='{colname}::BOOLEAN')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
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

        correct_sql = 'ALTER TABLE "item" ALTER COLUMN "name" TYPE BYTEA USING "name"::BYTEA'

        migration_op = "ChangeColumnType(entity_name='Item', attr_name='name', py_type=bytes, options={}, " \
                       "cast_sql='{colname}::BYTEA')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
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

        migration_op = "ChangeColumnType(entity_name='Item', attr_name='name', py_type=LongStr, options={}, " \
                       "cast_sql='{colname}::TEXT')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
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

        correct_sql = 'ALTER TABLE "item" ALTER COLUMN "name" TYPE UUID USING "name"::UUID'

        migration_op = "ChangeColumnType(entity_name='Item', attr_name='name', py_type=UUID, options={}, " \
                       "cast_sql='{colname}::UUID')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
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

        correct_sql = 'ALTER TABLE "item" ALTER COLUMN "name" TYPE JSONB USING "name"::JSONB'

        migration_op = "ChangeColumnType(entity_name='Item', attr_name='name', py_type=Json, options={}, " \
                       "cast_sql='{colname}::JSONB')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_attr_type_int_to_str(self):
        """
            Changes integer attribute "number" in entity "Item" to string type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            number = Required(int)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            number = Required(str)

        correct_sql = 'ALTER TABLE "item" ALTER COLUMN "number" TYPE TEXT USING "number"::TEXT'

        migration_op = "ChangeColumnType(entity_name='Item', attr_name='number', py_type=str, options={}, " \
                       "cast_sql='{colname}::TEXT')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_attr_type_int_to_float(self):
        """
            Changes integer attribute "number" in entity "Item" to float type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            number = Required(int)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            number = Required(float)

        correct_sql = 'ALTER TABLE "item" ALTER COLUMN "number" TYPE DOUBLE PRECISION USING "number"::DOUBLE PRECISION'

        migration_op = "ChangeColumnType(entity_name='Item', attr_name='number', py_type=float, options={}, " \
                       "cast_sql='{colname}::DOUBLE PRECISION')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_attr_type_int_to_decimal(self):
        """
            Changes integer attribute "number" in entity "Item" to decimal type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            number = Required(int)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            number = Required(Decimal)

        correct_sql = 'ALTER TABLE "item" ALTER COLUMN "number" TYPE DECIMAL(12, 2) USING "number"::DECIMAL(12, 2)'

        migration_op = "ChangeColumnType(entity_name='Item', attr_name='number', py_type=Decimal, options={}, " \
                       "cast_sql='{colname}::DECIMAL(12, 2)')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_attr_type_int_to_bool(self):
        """
            Changes integer attribute "number" in entity "Item" to bool type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            number = Required(int)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            number = Required(bool)

        correct_sql = 'ALTER TABLE "item" ALTER COLUMN "number" TYPE BOOLEAN USING "number"::BOOLEAN'

        migration_op = "ChangeColumnType(entity_name='Item', attr_name='number', py_type=bool, options={}, " \
                       "cast_sql='{colname}::BOOLEAN')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_attr_type_int_to_longstr(self):
        """
            Changes integer attribute "number" in entity "Item" to longstr type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            number = Required(int)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            number = Required(LongStr)

        correct_sql = 'ALTER TABLE "item" ALTER COLUMN "number" TYPE TEXT USING "number"::TEXT'

        migration_op = "ChangeColumnType(entity_name='Item', attr_name='number', py_type=LongStr, options={}, " \
                       "cast_sql='{colname}::TEXT')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_attr_type_float_to_str(self):
        """
            Changes float attribute "number" in entity "Item" to string type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            number = Required(float)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            number = Required(str)

        correct_sql = 'ALTER TABLE "item" ALTER COLUMN "number" TYPE TEXT USING "number"::TEXT'

        migration_op = "ChangeColumnType(entity_name='Item', attr_name='number', py_type=str, options={}, " \
                       "cast_sql='{colname}::TEXT')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_attr_type_float_to_int(self):
        """
            Changes float attribute "number" in entity "Item" to integer type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            number = Required(float)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            number = Required(int)

        correct_sql = 'ALTER TABLE "item" ALTER COLUMN "number" TYPE INTEGER USING "number"::INTEGER'

        migration_op = "ChangeColumnType(entity_name='Item', attr_name='number', py_type=int, options={}, " \
                       "cast_sql='{colname}::INTEGER')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_attr_type_float_to_decimal(self):
        """
            Changes float attribute "number" in entity "Item" to decimal type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            number = Required(float)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            number = Required(Decimal)

        correct_sql = 'ALTER TABLE "item" ALTER COLUMN "number" TYPE DECIMAL(12, 2) USING "number"::DECIMAL(12, 2)'

        migration_op = "ChangeColumnType(entity_name='Item', attr_name='number', py_type=Decimal, options={}, " \
                       "cast_sql='{colname}::DECIMAL(12, 2)')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_attr_type_float_to_bool(self):
        """
            Changes float attribute "number" in entity "Item" to bool type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            number = Required(float)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            number = Required(bool)
        # apply migrate () raises exceptions :
        # psycopg2.errors.CannotCoerce: cannot cast type double precision to boolean
        # pony.orm.dbapiprovider.ProgrammingError: cannot cast type double precision to boolean
        correct_sql = 'ALTER TABLE "item" ALTER COLUMN "number" TYPE BOOLEAN USING "number"::BOOLEAN'

        migration_op = "ChangeColumnType(entity_name='Item', attr_name='number', new_options={'py_type': bool}, " \
                       "cast_sql='{colname}::BOOLEAN')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_attr_type_float_to_longstr(self):
        """
            Changes float attribute "number" in entity "Item" to longstr type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            number = Required(float)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            number = Required(LongStr)

        correct_sql = 'ALTER TABLE "item" ALTER COLUMN "number" TYPE TEXT USING "number"::TEXT'

        migration_op = "ChangeColumnType(entity_name='Item', attr_name='number', py_type=LongStr, options={}, " \
                       "cast_sql='{colname}::TEXT')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_attr_type_decimal_to_str(self):
        """
            Changes decimal attribute "number" in entity "Item" to string type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            number = Required(Decimal)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            number = Required(str)

        correct_sql = 'ALTER TABLE "item" ALTER COLUMN "number" TYPE TEXT USING "number"::TEXT'

        migration_op = "ChangeColumnType(entity_name='Item', attr_name='number', py_type=str, options={}, " \
                       "cast_sql='{colname}::TEXT')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)


    def test_change_attr_type_decimal_to_int(self):
        """
            Changes decimal attribute "number" in entity "Item" to integer type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            number = Required(Decimal)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            number = Required(int)

        correct_sql = 'ALTER TABLE "item" ALTER COLUMN "number" TYPE INTEGER USING "number"::INTEGER'

        migration_op = "ChangeColumnType(entity_name='Item', attr_name='number', py_type=int, options={}, " \
                       "cast_sql='{colname}::INTEGER')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_attr_type_decimal_to_float(self):
        """
            Changes decimal attribute "number" in entity "Item" to float type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            number = Required(Decimal)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            number = Required(float)

        correct_sql = 'ALTER TABLE "item" ALTER COLUMN "number" TYPE DOUBLE PRECISION USING "number"::DOUBLE PRECISION'

        migration_op = "ChangeColumnType(entity_name='Item', attr_name='number', py_type=float, options={}, " \
                       "cast_sql='{colname}::DOUBLE PRECISION')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_attr_type_decimal_to_bool(self):
        """
            Changes decimal attribute "number" in entity "Item" to bool type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            number = Required(Decimal)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            number = Required(bool)

        correct_sql = ''

        migration_op = ""

        # apply migrate () raises exceptions :
        # psycopg2.errors.CannotCoerce: cannot cast type double precision to boolean
        # pony.orm.dbapiprovider.ProgrammingError: cannot cast type double precision to boolean
        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_attr_type_decimal_to_longstr(self):
        """
            Changes decimal attribute "number" in entity "Item" to longstr type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            number = Required(Decimal)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            number = Required(LongStr)

        correct_sql = 'ALTER TABLE "item" ALTER COLUMN "number" TYPE TEXT USING "number"::TEXT'

        migration_op = "ChangeColumnType(entity_name='Item', attr_name='number', py_type=LongStr, options={}, " \
                       "cast_sql='{colname}::TEXT')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_attr_type_datetime_to_str(self):
        """
            Changes datetime attribute "number" in entity "Item" to string type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            number = Required(datetime)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            number = Required(LongStr)

        correct_sql = 'ALTER TABLE "item" ALTER COLUMN "number" TYPE TEXT USING "number"::TEXT'

        migration_op = "ChangeColumnType(entity_name='Item', attr_name='number', py_type=LongStr, options={}, " \
                       "cast_sql='{colname}::TEXT')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_attr_type_datetime_to_date(self):
        """
            Changes datetime attribute "number" in entity "Item" to date type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            number = Required(datetime)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            number = Required(date)

        correct_sql = 'ALTER TABLE "item" ALTER COLUMN "number" TYPE DATE USING "number"::DATE'

        migration_op = "ChangeColumnType(entity_name='Item', attr_name='number', py_type=date, options={}, " \
                       "cast_sql='{colname}::DATE')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_attr_type_datetime_to_time(self):
        """
            Changes datetime attribute "number" in entity "Item" to time type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            number = Required(datetime)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            number = Required(time)

        correct_sql = 'ALTER TABLE "item" ALTER COLUMN "number" TYPE TIME USING "number"::TIME'

        migration_op = "ChangeColumnType(entity_name='Item', attr_name='number', py_type=time, options={}, " \
                       "cast_sql='{colname}::TIME')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_attr_type_datetime_to_longstr(self):
        """
            Changes datetime attribute "number" in entity "Item" to longstr type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            number = Required(datetime)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            number = Required(LongStr)

        correct_sql = 'ALTER TABLE "item" ALTER COLUMN "number" TYPE TEXT USING "number"::TEXT'

        migration_op = "ChangeColumnType(entity_name='Item', attr_name='number', py_type=LongStr, options={}, " \
                       "cast_sql='{colname}::TEXT')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_attr_type_date_to_str(self):
        """
            Changes date attribute "number" in entity "Item" to string type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            number = Required(date)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            number = Required(str)

        correct_sql = 'ALTER TABLE "item" ALTER COLUMN "number" TYPE TEXT USING "number"::TEXT'

        migration_op = "ChangeColumnType(entity_name='Item', attr_name='number', py_type=str, options={}, " \
                       "cast_sql='{colname}::TEXT')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_attr_type_date_to_datetime(self):
        """
            Changes date attribute "number" in entity "Item" to datetime type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            number = Required(date)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            number = Required(datetime)

        correct_sql = 'ALTER TABLE "item" ALTER COLUMN "number" TYPE TIMESTAMP USING "number"::TIMESTAMP'

        migration_op = "ChangeColumnType(entity_name='Item', attr_name='number', py_type=datetime, options={}, " \
                       "cast_sql='{colname}::TIMESTAMP')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_attr_type_date_to_longstr(self):
        """
            Changes date attribute "number" in entity "Item" to longstr type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            number = Required(date)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            number = Required(LongStr)

        correct_sql = 'ALTER TABLE "item" ALTER COLUMN "number" TYPE TEXT USING "number"::TEXT'

        migration_op = "ChangeColumnType(entity_name='Item', attr_name='number', py_type=LongStr, options={}, " \
                       "cast_sql='{colname}::TEXT')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_attr_type_time_to_str(self):
        """
            Changes time attribute "number" in entity "Item" to string type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            number = Required(time)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            number = Required(str)

        correct_sql = 'ALTER TABLE "item" ALTER COLUMN "number" TYPE TEXT USING "number"::TEXT'

        migration_op = "ChangeColumnType(entity_name='Item', attr_name='number', py_type=str, options={}, " \
                       "cast_sql='{colname}::TEXT')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_attr_type_time_to_timedelta(self):
        """
            Changes time attribute "number" in entity "Item" to timedelta type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            number = Required(time)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            number = Required(timedelta)

        correct_sql = 'ALTER TABLE "item" ALTER COLUMN "number" TYPE INTERVAL DAY TO SECOND ' \
                      'USING "number"::INTERVAL DAY TO SECOND'

        migration_op = "ChangeColumnType(entity_name='Item', attr_name='number', py_type=timedelta, options={}, " \
                       "cast_sql='{colname}::INTERVAL DAY TO SECOND')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_attr_type_time_to_longstr(self):
        """
            Changes time attribute "number" in entity "Item" to longstr type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            number = Required(time)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            number = Required(LongStr)

        correct_sql = 'ALTER TABLE "item" ALTER COLUMN "number" TYPE TEXT USING "number"::TEXT'

        migration_op = "ChangeColumnType(entity_name='Item', attr_name='number', py_type=LongStr, options={}, " \
                       "cast_sql='{colname}::TEXT')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_attr_type_timedelta_to_str(self):
        """
            Changes timedelta attribute "number" in entity "Item" to string type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            number = Required(timedelta)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            number = Required(str)

        correct_sql = 'ALTER TABLE "item" ALTER COLUMN "number" TYPE TEXT USING "number"::TEXT'

        migration_op = "ChangeColumnType(entity_name='Item', attr_name='number', py_type=str, options={}, " \
                       "cast_sql='{colname}::TEXT')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_attr_type_timedelta_to_time(self):
        """
            Changes timedelta attribute "number" in entity "Item" to time type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            number = Required(timedelta)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            number = Required(time)

        correct_sql = 'ALTER TABLE "item" ALTER COLUMN "number" TYPE TIME USING "number"::TIME'

        migration_op = "ChangeColumnType(entity_name='Item', attr_name='number', py_type=time, options={}, " \
                       "cast_sql='{colname}::TIME')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_attr_type_timedelta_to_longstr(self):
        """
            Changes timedelta attribute "number" in entity "Item" to longstr type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            number = Required(timedelta)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            number = Required(LongStr)

        correct_sql = 'ALTER TABLE "item" ALTER COLUMN "number" TYPE TEXT USING "number"::TEXT'

        migration_op = "ChangeColumnType(entity_name='Item', attr_name='number', py_type=LongStr, options={}, " \
                       "cast_sql='{colname}::TEXT')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_attr_type_bool_to_str(self):
        """
            Changes bool attribute "truth" in entity "Item" to string type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            truth = Required(bool)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            truth = Required(str)

        correct_sql = 'ALTER TABLE "item" ALTER COLUMN "truth" TYPE TEXT USING "truth"::TEXT'

        migration_op = "ChangeColumnType(entity_name='Item', attr_name='truth', py_type=str, options={}, " \
                       "cast_sql='{colname}::TEXT')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_attr_type_bool_to_int(self):
        """
            Changes bool attribute "truth" in entity "Item" to integer type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            truth = Required(bool)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            truth = Required(int)

        correct_sql = 'ALTER TABLE "item" ALTER COLUMN "truth" TYPE INTEGER USING "truth"::INTEGER'

        migration_op = "ChangeColumnType(entity_name='Item', attr_name='truth', py_type=int, options={}, " \
                       "cast_sql='{colname}::INTEGER')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_attr_type_bool_to_float(self):
        """
            Changes bool attribute "truth" in entity "Item" to float type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            truth = Required(bool)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            truth = Required(float)

        correct_sql = ''

        migration_op = ""
        # apply migrate () raises exceptions :
        # psycopg2.errors.CannotCoerce: cannot cast type boolean to double precision
        # pony.orm.dbapiprovider.ProgrammingError: cannot cast type boolean to double precision

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_attr_type_bool_to_decimal(self):
        """
            Changes bool attribute "truth" in entity "Item" to decimal type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            truth = Required(bool)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            truth = Required(Decimal)

        correct_sql = ''

        migration_op = ""
        # apply migrate () raises exceptions :
        # psycopg2.errors.CannotCoerce: cannot cast type boolean to numeric
        # pony.orm.dbapiprovider.ProgrammingError: cannot cast type boolean to numeric

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_attr_type_bool_to_longstr(self):
        """
            Changes bool attribute "truth" in entity "Item" to longstr type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            truth = Required(bool)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            truth = Required(LongStr)

        correct_sql = 'ALTER TABLE "item" ALTER COLUMN "truth" TYPE TEXT USING "truth"::TEXT'

        migration_op = "ChangeColumnType(entity_name='Item', attr_name='truth', py_type=LongStr, options={}, " \
                       "cast_sql='{colname}::TEXT')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_attr_type_bytes_to_str(self):
        """
            Changes bytes attribute "my_attr" in entity "Item" to string type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            my_attr = Required(bytes)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            my_attr = Required(str)

        correct_sql = 'ALTER TABLE "item" ALTER COLUMN "my_attr" TYPE TEXT USING "my_attr"::TEXT'

        migration_op = "ChangeColumnType(entity_name='Item', attr_name='my_attr', py_type=str, options={}, " \
                       "cast_sql='{colname}::TEXT')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_attr_type_bytes_to_longstr(self):
        """
            Changes bytes attribute "my_attr" in entity "Item" to longstr type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            my_attr = Required(bytes)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            my_attr = Required(LongStr)

        correct_sql = 'ALTER TABLE "item" ALTER COLUMN "my_attr" TYPE TEXT USING "my_attr"::TEXT'

        migration_op = "ChangeColumnType(entity_name='Item', attr_name='my_attr', py_type=LongStr, options={}, " \
                       "cast_sql='{colname}::TEXT')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_attr_type_longstr_to_str(self):
        """
            Changes longstr attribute "description" in entity "Item" to str type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            description = Required(LongStr)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            description = Required(str)

        correct_sql = ''

        migration_op = "ChangeColumnType(entity_name='Item', attr_name='description', py_type=str, options={}, " \
                       "cast_sql='{colname}::TEXT')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_attr_type_longstr_to_int(self):
        """
            Changes longstr attribute "description" in entity "Item" to integer type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            description = Required(LongStr)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            description = Required(int)

        correct_sql = 'ALTER TABLE "item" ALTER COLUMN "description" TYPE INTEGER USING "description"::INTEGER'

        migration_op = "ChangeColumnType(entity_name='Item', attr_name='description', py_type=int, options={}, " \
                       "cast_sql='{colname}::INTEGER')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_attr_type_longstr_to_float(self):
        """
            Changes longstr attribute "description" in entity "Item" to float type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            description = Required(LongStr)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            description = Required(float)

        correct_sql = 'ALTER TABLE "item" ALTER COLUMN "description" TYPE DOUBLE PRECISION ' \
                      'USING "description"::DOUBLE PRECISION'

        migration_op = "ChangeColumnType(entity_name='Item', attr_name='description', py_type=float, options={}, " \
                       "cast_sql='{colname}::DOUBLE PRECISION')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_attr_type_longstr_to_decimal(self):
        """
            Changes longstr attribute "description" in entity "Item" to decimal type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            description = Required(LongStr)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            description = Required(Decimal)

        correct_sql = 'ALTER TABLE "item" ALTER COLUMN "description" TYPE DECIMAL(12, 2) ' \
                      'USING "description"::DECIMAL(12, 2)'

        migration_op = "ChangeColumnType(entity_name='Item', attr_name='description', py_type=Decimal, options={}, " \
                       "cast_sql='{colname}::DECIMAL(12, 2)')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_attr_type_longstr_to_datetime(self):
        """
            Changes longstr attribute "description" in entity "Item" to datetime type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            description = Required(LongStr)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            description = Required(datetime)

        correct_sql = 'ALTER TABLE "item" ALTER COLUMN "description" TYPE TIMESTAMP USING "description"::TIMESTAMP'

        migration_op = "ChangeColumnType(entity_name='Item', attr_name='description', py_type=datetime, options={}, " \
                       "cast_sql='{colname}::TIMESTAMP')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_attr_type_longstr_to_date(self):
        """
            Changes longstr attribute "description" in entity "Item" to date type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            description = Required(LongStr)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            description = Required(date)

        correct_sql = 'ALTER TABLE "item" ALTER COLUMN "description" TYPE DATE USING "description"::DATE'

        migration_op = "ChangeColumnType(entity_name='Item', attr_name='description', py_type=date, options={}, " \
                       "cast_sql='{colname}::DATE')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_attr_type_longstr_to_time(self):
        """
            Changes longstr attribute "description" in entity "Item" to time type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            description = Required(LongStr)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            description = Required(time)

        correct_sql = 'ALTER TABLE "item" ALTER COLUMN "description" TYPE TIME USING "description"::TIME'

        migration_op = "ChangeColumnType(entity_name='Item', attr_name='description', py_type=time, options={}, " \
                       "cast_sql='{colname}::TIME')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_attr_type_longstr_to_timedelta(self):
        """
            Changes longstr attribute "description" in entity "Item" to timedelta type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            description = Required(LongStr)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            description = Required(timedelta)

        correct_sql = 'ALTER TABLE "item" ALTER COLUMN "description" TYPE INTERVAL DAY TO SECOND ' \
                      'USING "description"::INTERVAL DAY TO SECOND'

        migration_op = "ChangeColumnType(entity_name='Item', attr_name='description', py_type=timedelta, options={}, " \
                       "cast_sql='{colname}::INTERVAL DAY TO SECOND')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_attr_type_longstr_to_bool(self):
        """
            Changes longstr attribute "description" in entity "Item" to bool type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            description = Required(LongStr)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            description = Required(bool)

        correct_sql = 'ALTER TABLE "item" ALTER COLUMN "description" TYPE BOOLEAN USING "description"::BOOLEAN'

        migration_op = "ChangeColumnType(entity_name='Item', attr_name='description', py_type=bool, options={}, " \
                       "cast_sql='{colname}::BOOLEAN')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_attr_type_longstr_to_bytes(self):
        """
            Changes longstr attribute "description" in entity "Item" to bytes type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            description = Required(LongStr)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            description = Required(bytes)

        correct_sql = 'ALTER TABLE "item" ALTER COLUMN "description" TYPE BYTEA USING "description"::BYTEA'

        migration_op = "ChangeColumnType(entity_name='Item', attr_name='description', py_type=bytes, options={}, " \
                       "cast_sql='{colname}::BYTEA')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_attr_type_longstr_to_uuid(self):
        """
            Changes longstr attribute "description" in entity "Item" to uuid type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            description = Required(LongStr)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            description = Required(UUID)

        correct_sql = 'ALTER TABLE "item" ALTER COLUMN "description" TYPE UUID USING "description"::UUID'

        migration_op = "ChangeColumnType(entity_name='Item', attr_name='description', py_type=UUID, options={}, " \
                       "cast_sql='{colname}::UUID')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_attr_type_longstr_to_json(self):
        """
            Changes longstr attribute "description" in entity "Item" to json type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            description = Required(LongStr)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            description = Required(Json)

        correct_sql = 'ALTER TABLE "item" ALTER COLUMN "description" TYPE JSONB USING "description"::JSONB'

        migration_op = "ChangeColumnType(entity_name='Item', attr_name='description', py_type=Json, options={}, " \
                       "cast_sql='{colname}::JSONB')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_attr_type_uuid_to_str(self):
        """
            Changes UUID attribute "uuid" in entity "Item" to string type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            uuid = Required(UUID)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            uuid = Required(str)

        correct_sql = 'ALTER TABLE "item" ALTER COLUMN "uuid" TYPE TEXT USING "uuid"::TEXT'

        migration_op = "ChangeColumnType(entity_name='Item', attr_name='uuid', py_type=str, options={}, " \
                       "cast_sql='{colname}::TEXT')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_attr_type_uuid_to_longstr(self):
        """
            Changes UUID attribute "uuid" in entity "Item" to longstr type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            uuid = Required(UUID)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            uuid = Required(LongStr)

        correct_sql = 'ALTER TABLE "item" ALTER COLUMN "uuid" TYPE TEXT USING "uuid"::TEXT'

        migration_op = "ChangeColumnType(entity_name='Item', attr_name='uuid', py_type=LongStr, options={}, " \
                       "cast_sql='{colname}::TEXT')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_attr_type_json_to_str(self):
        """
            Changes json attribute "request" in entity "Item" to str type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            request = Required(Json)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            request = Required(str)

        correct_sql = 'ALTER TABLE "item" ALTER COLUMN "request" TYPE TEXT USING "request"::TEXT'

        migration_op = "ChangeColumnType(entity_name='Item', attr_name='request', py_type=str, options={}, " \
                       "cast_sql='{colname}::TEXT')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_attr_type_json_to_longstr(self):
        """
            Changes json attribute "request" in entity "Item" to longstr type
        """
        # Logically correct type casting
        self.db = db = Database(**self.db_params)

        class Item(db.Entity):
            id = PrimaryKey(int, auto=True)
            request = Required(Json)

        db.generate_mapping(create_tables=True)

        self.db2 = db2 = Database(**self.db_params)

        class Item(db2.Entity):
            id = PrimaryKey(int, auto=True)
            request = Required(LongStr)

        correct_sql = 'ALTER TABLE "item" ALTER COLUMN "request" TYPE TEXT USING "request"::TEXT'

        migration_op = "ChangeColumnType(entity_name='Item', attr_name='request', py_type=LongStr, options={}, " \
                       "cast_sql='{colname}::TEXT')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)


if __name__ == '__main__':
    unittest.main()
