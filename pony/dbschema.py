
class DBSchemaError(Exception): pass

class DBSchema(object):
    def __init__(schema, database):
        schema.database = database
        schema.tables = {}
        schema.constraints = {}
        schema.indent = '  '
        schema.command_separator = ';\n'
    def quote_name(schema, name):
        con, provider = schema.database._get_connection()
        return provider.quote_name(con, name)
    def column_list(schema, columns):
        return '(%s)' % ', '.join(schema.quote_name(column.name) for column in columns)

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
        table.references = {}
        table.parent_tables = set()
        table.child_tables = set()
    def get_create_sql(table, created_tables=None):
        if created_tables is None: created_tables = set()
        schema = table.schema
        result = []
        result.append('CREATE TABLE %s (' % schema.quote_name(table.name))
        for column in table.column_list:
            result.append(schema.indent + column.get_create_sql(created_tables) + ',')
        if len(table.pk_index.columns) > 1:
            result.append(table.pk_index.get_create_sql(inside_table=True))
        for index in table.indexes.values():
            if index.is_pk: continue
            if not index.is_unique: continue
            if len(index.columns) == 1: continue
            result.append(index.get_create_sql(inside_table=True) + ',')
        for reference in table.references.values():
            if len(reference.child_columns) == 1: continue
            if not reference.parent_table in created_tables: continue
            result.append(reference.get_create_sql(inside_table=True) + ',')
        result[-1] = result[-1][:-1]
        result.append(')' + schema.command_separator)
        for child_table in table.child_tables:
            if child_table not in created_tables: continue
            for reference in child_table.references.values():
                if reference.parent_table is not table: continue
                result.append(reference.get_create_sql(inside_table=False))
        created_tables.add(table)
        return '\n'.join(result)

class Column(object):
    def __init__(column, name, table, sql_type, is_pk=False, is_unique=None, is_not_null=None):
        if name in table.column_dict:
            raise DBSchemaError("Column %r already exists in table %r" % (name, table.name))

        if is_unique is None:
            is_unique = is_pk
        elif not is_unique:
            if is_pk: raise DBSchemaError(
                "Incompatible combination of is_unique=False and is_pk=True for column %s" % name)

        if is_not_null is None:
            is_not_null = is_pk
        elif not is_not_null:
            if is_pk: raise DBSchemaError(
                "Incompatible combination of is_not_null=False and is_pk=True for column %s" % name)

        table.column_dict[name] = column
        table.column_list.append(column)
        column.table = table
        column.name = name
        column.sql_type = sql_type
        column.is_pk = is_pk
        column.is_unique = is_unique
        column.is_not_null = is_not_null
        if is_unique: Index(None, table, (column,), is_pk=is_pk, is_unique=True)
    def get_create_sql(column, created_tables=None):
        if created_tables is None: created_tables = set()
        table = column.table
        schema = table.schema
        result = []
        result.append(schema.quote_name(column.name))
        result.append(column.sql_type)
        if column.is_pk: result.append('PRIMARY KEY')
        else:
            if column.is_unique: result.append('UNIQUE')
            if column.is_not_null: result.append('NOT NULL')
        reference = table.references.get((column,))
        if reference is not None:
            parent_table = reference.parent_table
            if parent_table in created_tables or parent_table is table:
                result.append('REFERENCES')
                result.append(schema.quote_name(parent_table.name))
                result.append(schema.column_list(reference.parent_columns)) 
        return ' '.join(result)

class Constraint(object):
    def __init__(constraint, name, schema):
        if name is not None:
            if name in schema.constraints: raise DBSchemaError(
                "Constraint with name %s already exists" % name)
            schema.constraints[name] = constraint
        constraint.schema = schema
        constraint.name = name

class Index(Constraint):
    def __init__(index, name, table, columns, is_pk=False, is_unique=None):
        assert len(columns) > 0
        for column in columns:
            if column.table is not table: raise DBSchemaError(
                "Column %s does not belong to table %s and cannot be part of its index"
                % (column.name, table.name))
        if columns in table.indexes:
            if len(columns) == 1: raise DBSchemaError("Index for column %s already exists" % columns[0].name)
            else: raise DBSchemaError("Index for columns (%s) already exists" % ', '.join(column.name for column in columns))
        if is_pk:
            if table.pk_index is not None: raise DBSchemaError(
                'Primary key for table %s is already defined' % table.name)
            table.pk_index = index
            if is_unique is None: is_unique = True
            elif not is_unique: raise DBSchemaError(
                "Incompatible combination of is_unique=False and is_pk=True for index %s" % name)
            if len(columns) == 1:
                column = columns[0]
                column.is_pk = column.is_unique = True
        Constraint.__init__(index, name, table.schema)
        table.indexes[columns] = index
        index.table = table
        index.columns = columns
        index.is_pk = is_pk
        index.is_unique = is_unique
    def get_create_sql(index, inside_table):
        schema = index.schema
        quote_name = schema.quote_name
        result = []
        append = result.append
        if not inside_table:
            if index.is_pk: raise DBSchemaError(
                'Primary key index cannot be defined outside of table definition')
            append('CREATE')
            if index.is_unique: append('UNIQUE')
            append('INDEX')
            append(quote_name(index.name))
            append('ON')
            append(quote_name(table.name))
        else:
            if index.name:
                append('CONSTRAINT')
                append(quote_name(index.name))
            if index.is_pk: append('PRIMARY KEY')
            elif index.is_unique: append('UNIQUE')
            else: append('INDEX')
        append(schema.column_list(index.columns))
        if not inside_table: append(schema.command_separator)
        return ' '.join(result)

class Reference(Constraint):
    def __init__(reference, name, parent_table, parent_columns, child_table, child_columns):
        schema = parent_table.schema
        if schema is not child_table.schema: raise DBSchemaError(
            'Parent and child tables of reference cannot belong to different schemata')
        for column in parent_columns:
            if column.table is not parent_table: raise DBSchemaError(
                'Column %s does not belong to table %s' % (column.name, parent_table.name))
        for column in child_columns:
            if column.table is not child_table: raise DBSchemaError(
                'Column %s does not belong to table %s' % (column.name, child_table.name))
        if len(parent_columns) != len(child_columns): raise DBSchemaError(
            'Foreign key columns count do not match')
        if child_columns in child_table.references: 
            if len(child_columns) == 1: raise DBSchemaError('Foreign key for column %s already defined' % child_columns[0].name)
            else: raise DBSchemaError('Foreign key for columns (%s) already defined' % ', '.join(column.name for column in child_columns))
        child_table.references[child_columns] = reference
        child_table.parent_tables.add(parent_table)
        parent_table.child_tables.add(child_table)
        Constraint.__init__(reference, name, schema)
        reference.parent_table = parent_table
        reference.parent_columns = parent_columns
        reference.child_table = child_table
        reference.child_columns = child_columns
    def get_create_sql(reference, inside_table):
        schema = reference.schema
        quote_name = schema.quote_name
        result = []
        append = result.append
        if not inside_table:
            append('ALTER TABLE')
            append(quote_name(reference.child_table.name))
            append('ADD')
        if reference.name:
            append('CONSTRAINT')
            append(quote_name(reference.name))
        append('FOREIGN KEY')
        append(schema.column_list(reference.child_columns))
        append('REFERENCES')
        append(quote_name(reference.parent_table.name))
        append(schema.column_list(reference.parent_columns))
        if not inside_table: append(schema.command_separator)
        return ' '.join(result)
