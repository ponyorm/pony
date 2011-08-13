
class DBSchemaError(Exception): pass

class DBSchema(object):
    def __init__(schema, database, uppercase=False):
        schema.database = database
        schema.tables = {}
        schema.constraints = {}
        schema.indent = '  '
        schema.command_separator = ';\n'
        schema.uppercase = uppercase
    def quote_name(schema, name):
        cache = schema.database._get_cache()
        return schema.database.provider.quote_name(cache.connection, name)
    def column_list(schema, columns):
        return '(%s)' % ', '.join(schema.quote_name(column.name) for column in columns)
    def case(schema, s):
        if schema.uppercase: return s.upper().replace('%S', '%s').replace('%R', '%r')
        else: return s.lower()
    def get_create_commands(schema, uppercase=None):
        prev_uppercase = schema.uppercase
        try:
            if uppercase is not None: schema.uppercase = uppercase
            created_tables = set()
            result = []
            tables_to_create = set(schema.tables.values())
            while tables_to_create:
                for table in tables_to_create:
                    if table.parent_tables.issubset(created_tables):
                        tables_to_create.remove(table)
                        break
                else: table = tables_to_create.pop()
                result.extend(table.get_create_commands(created_tables))
            return result
        finally:
            if uppercase is not None: schema.uppercase = prev_uppercase
    def get_create_script(schema, uppercase=None):
        commands = schema.get_create_commands(uppercase)
        return schema.command_separator.join(commands)
    def add_table(schema, table_name):
        return schema.table_class(table_name, schema)

class Table(object):
    def __init__(table, name, schema):
        if name in schema.tables:
            raise DBSchemaError("Table %r already exists in database schema" % name)
        schema.tables[name] = table
        table.schema = schema
        table.name = name
        table.column_list = []
        table.column_dict = {}
        table.indexes = {}
        table.pk_index = None
        table.foreign_keys = {}
        table.parent_tables = set()
        table.child_tables = set()
        
        table.entities = set()
        table.m2m = set()
    def __repr__(table):
        return '<Table(%s)>' % table.name
    def get_create_commands(table, created_tables=None):
        if created_tables is None: created_tables = set()
        schema = table.schema
        case = schema.case
        cmd = []
        cmd.append(case('CREATE TABLE IF NOT EXISTS %s (') % schema.quote_name(table.name))
        for column in table.column_list:
            cmd.append(schema.indent + column.get_sql(created_tables) + ',')
        if len(table.pk_index.columns) > 1:
            cmd.append(schema.indent + table.pk_index.get_sql() + ',')
        for index in table.indexes.values():
            if index.is_pk: continue
            if not index.is_unique: continue
            if len(index.columns) == 1: continue
            cmd.append(index.get_sql() + ',')
        for foreign_key in table.foreign_keys.values():
            if len(foreign_key.child_columns) == 1: continue
            if not foreign_key.parent_table in created_tables: continue
            cmd.append(foreign_key.get_sql() + ',')
        cmd[-1] = cmd[-1][:-1]
        cmd.append(')')
        cmd = '\n'.join(cmd)
        result = [ cmd ]
        for child_table in table.child_tables:
            if child_table not in created_tables: continue
            for foreign_key in child_table.foreign_keys.values():
                if foreign_key.parent_table is not table: continue
                result.append(foreign_key.get_create_command())
        created_tables.add(table)
        return result
    def add_column(table, column_name, sql_type, is_not_null=None):
        return table.schema.column_class(column_name, table, sql_type, is_not_null)
    def add_index(table, index_name, columns, is_pk=False, is_unique=None):
        return table.schema.index_class(index_name, table, columns, is_pk, is_unique)
    def add_foreign_key(table, fk_name, child_columns, parent_table, parent_columns):
        return table.schema.fk_class(fk_name, table, child_columns, parent_table, parent_columns)

class Column(object):
    auto_sql_types = frozenset([ 'INTEGER' ])
    autoincrement = 'AUTOINCREMENT'
    def __init__(column, name, table, sql_type, is_not_null=None):
        if name in table.column_dict:
            raise DBSchemaError("Column %r already exists in table %r" % (name, table.name))
        table.column_dict[name] = column
        table.column_list.append(column)
        column.table = table
        column.name = name
        column.sql_type = sql_type
        column.is_not_null = is_not_null
        column.is_pk = False
        column.is_pk_part = False
        column.is_unique = False
    def __repr__(column):
        return '<Column(%s.%s)>' % (column.table.name, column.name)
    def get_sql(column, created_tables=None):
        if created_tables is None: created_tables = set()
        table = column.table
        schema = table.schema
        quote = schema.quote_name
        case = schema.case
        result = []
        append = result.append
        append(quote(column.name))
        append(case(column.sql_type))
        if column.is_pk:
            append(case('PRIMARY KEY'))
            if column.is_pk == 'auto' and column.autoincrement: append(case(column.autoincrement))
        else:
            if column.is_unique: append(case('UNIQUE'))
            if column.is_not_null: append(case('NOT NULL'))
        foreign_key = table.foreign_keys.get((column,))
        if foreign_key is not None:
            parent_table = foreign_key.parent_table
            if parent_table in created_tables or parent_table is table:
                append(case('REFERENCES'))
                append(quote(parent_table.name))
                append(schema.column_list(foreign_key.parent_columns)) 
        return ' '.join(result)

class Constraint(object):
    def __init__(constraint, name, schema):
        if name is not None:
            if name in schema.constraints: raise DBSchemaError(
                "Constraint with name %r already exists" % name)
            schema.constraints[name] = constraint
        constraint.schema = schema
        constraint.name = name

class Index(Constraint):
    def __init__(index, name, table, columns, is_pk=False, is_unique=None):
        assert len(columns) > 0
        for column in columns:
            if column.table is not table: raise DBSchemaError(
                "Column %r does not belong to table %r and cannot be part of its index"
                % (column.name, table.name))
        if columns in table.indexes:
            if len(columns) == 1: raise DBSchemaError("Index for column %r already exists" % columns[0].name)
            else: raise DBSchemaError("Index for columns (%s) already exists" % ', '.join(repr(column.name) for column in columns))
        if is_pk:
            if table.pk_index is not None: raise DBSchemaError(
                'Primary key for table %r is already defined' % table.name)
            table.pk_index = index
            if is_unique is None: is_unique = True
            elif not is_unique: raise DBSchemaError(
                "Incompatible combination of is_unique=False and is_pk=True")
        elif is_unique is None: is_unique = False
        for column in columns:
            if is_pk == 'auto' and column.sql_type not in column.auto_sql_types:
                raise DBSchemaError("Column %s of type %s cannot be declared as 'auto'" % (column.name, column.sql_type))
            column.is_pk = len(columns) == 1 and is_pk
            column.is_pk_part = bool(is_pk)
            column.is_unique = is_unique and len(columns) == 1
        Constraint.__init__(index, name, table.schema)
        table.indexes[columns] = index
        index.table = table
        index.columns = columns
        index.is_pk = is_pk
        index.is_unique = is_unique
    def get_sql(index):
        return index._get_create_sql(inside_table=True)
    def get_create_command(index):
        return index._get_create_sql(inside_table=False)
    def _get_create_sql(index, inside_table):
        schema = index.schema
        case = schema.case
        quote_name = schema.quote_name
        cmd = []
        append = cmd.append
        if not inside_table:
            if index.is_pk: raise DBSchemaError(
                'Primary key index cannot be defined outside of table definition')
            append(case('CREATE'))
            if index.is_unique: append(case('UNIQUE'))
            append(case('INDEX'))
            append(quote_name(index.name))
            append(case('ON'))
            append(quote_name(table.name))
        else:
            if index.name:
                append(case('CONSTRAINT'))
                append(quote_name(index.name))
            if index.is_pk: append(case('PRIMARY KEY'))
            elif index.is_unique: append(case('UNIQUE'))
            else: append(case('INDEX'))
        append(schema.column_list(index.columns))
        return ' '.join(cmd)

class ForeignKey(Constraint):
    def __init__(foreign_key, name, child_table, child_columns, parent_table, parent_columns):
        schema = parent_table.schema
        if schema is not child_table.schema: raise DBSchemaError(
            'Parent and child tables of foreign_key cannot belong to different schemata')
        for column in parent_columns:
            if column.table is not parent_table: raise DBSchemaError(
                'Column %r does not belong to table %r' % (column.name, parent_table.name))
        for column in child_columns:
            if column.table is not child_table: raise DBSchemaError(
                'Column %r does not belong to table %r' % (column.name, child_table.name))
        if len(parent_columns) != len(child_columns): raise DBSchemaError(
            'Foreign key columns count do not match')
        if child_columns in child_table.foreign_keys: 
            if len(child_columns) == 1: raise DBSchemaError('Foreign key for column %r already defined' % child_columns[0].name)
            else: raise DBSchemaError('Foreign key for columns (%s) already defined' % ', '.join(repr(column.name) for column in child_columns))
        child_table.foreign_keys[child_columns] = foreign_key
        child_table.parent_tables.add(parent_table)
        parent_table.child_tables.add(child_table)
        Constraint.__init__(foreign_key, name, schema)
        foreign_key.parent_table = parent_table
        foreign_key.parent_columns = parent_columns
        foreign_key.child_table = child_table
        foreign_key.child_columns = child_columns
    def get_sql(foreign_key):
        return foreign_key._get_create_sql(inside_table=True)
    def get_create_command(foreign_key):
        return foreign_key._get_create_sql(inside_table=False)
    def _get_create_sql(foreign_key, inside_table):
        schema = foreign_key.schema
        case = schema.case
        quote_name = schema.quote_name
        cmd = []
        append = cmd.append
        if not inside_table:
            append(case('ALTER TABLE'))
            append(quote_name(foreign_key.child_table.name))
            append(case('ADD'))
        if foreign_key.name:
            append(case('CONSTRAINT'))
            append(quote_name(foreign_key.name))
        append(case('FOREIGN KEY'))
        append(schema.column_list(foreign_key.child_columns))
        append(case('REFERENCES'))
        append(quote_name(foreign_key.parent_table.name))
        append(schema.column_list(foreign_key.parent_columns))
        return ' '.join(cmd)

DBSchema.table_class = Table
DBSchema.column_class = Column
DBSchema.index_class = Index
DBSchema.fk_class = ForeignKey
