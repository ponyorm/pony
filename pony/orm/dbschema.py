from pony.orm import core
from pony.orm.core import log_sql, DBSchemaError
from pony.utils import throw

class DBSchema(object):
    dialect = None
    inline_fk_syntax = True
    named_foreign_keys = True
    def __init__(schema, provider, uppercase=True):
        schema.provider = provider
        schema.tables = {}
        schema.constraints = {}
        schema.indent = '  '
        schema.command_separator = ';\n\n'
        schema.uppercase = uppercase
        schema.names = {}
    def column_list(schema, columns):
        quote_name = schema.provider.quote_name
        return '(%s)' % ', '.join(quote_name(column.name) for column in columns)
    def case(schema, s):
        if schema.uppercase: return s.upper().replace('%S', '%s') \
            .replace(')S', ')s').replace('%R', '%r').replace(')R', ')r')
        else: return s.lower()
    def add_table(schema, table_name):
        return schema.table_class(table_name, schema)
    def order_tables_to_create(schema):
        tables = []
        created_tables = set()
        tables_to_create = set(schema.tables.values())
        while tables_to_create:
            for table in tables_to_create:
                if table.parent_tables.issubset(created_tables):
                    created_tables.add(table)
                    tables_to_create.remove(table)
                    break
            else: table = tables_to_create.pop()
            tables.append(table)
        return tables
    def generate_create_script(schema):
        created_tables = set()
        commands = []
        for table in schema.order_tables_to_create():
            for db_object in table.get_objects_to_create(created_tables):
                commands.append(db_object.get_create_command())
        return schema.command_separator.join(commands)
    def create_tables(schema, provider, connection):
        created_tables = set()
        for table in schema.order_tables_to_create():
            for db_object in table.get_objects_to_create(created_tables):
                name = db_object.exists(provider, connection, case_sensitive=False)
                if name is None: db_object.create(provider, connection)
                elif name != db_object.name:
                    quote_name = schema.provider.quote_name
                    n1, n2 = quote_name(db_object.name), quote_name(name)
                    tn1, tn2 = db_object.typename, db_object.typename.lower()
                    throw(DBSchemaError, '%s %s cannot be created, because %s %s ' \
                                         '(with a different letter case) already exists in the database. ' \
                                         'Try to delete %s %s first.' % (tn1, n1, tn2, n2, n2, tn2))
    def check_tables(schema, provider, connection):
        cursor = connection.cursor()
        for table in schema.tables.values():
            if isinstance(table.name, tuple): alias = table.name[-1]
            elif isinstance(table.name, basestring): alias = table.name
            else: assert False
            sql_ast = [ 'SELECT',
                        [ 'ALL', ] + [ [ 'COLUMN', alias, column.name ] for column in table.column_list ],
                        [ 'FROM', [ alias, 'TABLE', table.name ] ],
                        [ 'WHERE', [ 'EQ', [ 'VALUE', 0 ], [ 'VALUE', 1 ] ] ]
                      ]
            sql, adapter = provider.ast2sql(sql_ast)
            if core.debug: log_sql(sql)
            provider.execute(cursor, sql)

class DBObject(object):
    def create(table, provider, connection):
        sql = table.get_create_command()
        if core.debug: log_sql(sql)
        cursor = connection.cursor()
        provider.execute(cursor, sql)

class Table(DBObject):
    typename = 'Table'
    def __init__(table, name, schema):
        if name in schema.tables:
            throw(DBSchemaError, "Table %r already exists in database schema" % name)
        if name in schema.names:
            throw(DBSchemaError, "Table %r cannot be created, name is already in use" % name)
        schema.tables[name] = table
        schema.names[name] = table
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
        table_name = table.name
        if isinstance(table_name, tuple):
            table_name = '.'.join(table_name)
        return '<Table(%s)>' % table_name
    def exists(table, provider, connection, case_sensitive=True):
        return provider.table_exists(connection, table.name, case_sensitive)
    def get_create_command(table):
        schema = table.schema
        case = schema.case
        provider = schema.provider
        quote_name = provider.quote_name
        if_not_exists = False # provider.table_if_not_exists_syntax and provider.index_if_not_exists_syntax
        cmd = []
        if not if_not_exists: cmd.append(case('CREATE TABLE %s (') % quote_name(table.name))
        else: cmd.append(case('CREATE TABLE IF NOT EXISTS %s (') % quote_name(table.name))
        for column in table.column_list:
            cmd.append(schema.indent + column.get_sql() + ',')
        if len(table.pk_index.columns) > 1:
            cmd.append(schema.indent + table.pk_index.get_sql() + ',')
        for index in table.indexes.values():
            if index.is_pk: continue
            if not index.is_unique: continue
            if len(index.columns) == 1: continue
            cmd.append(schema.indent+index.get_sql() + ',')
        if not schema.named_foreign_keys:
            for foreign_key in table.foreign_keys.values():
                if schema.inline_fk_syntax and len(foreign_key.child_columns) == 1: continue
                cmd.append(schema.indent+foreign_key.get_sql() + ',')
        cmd[-1] = cmd[-1][:-1]
        cmd.append(')')
        return '\n'.join(cmd)
    def get_objects_to_create(table, created_tables=None):
        if created_tables is None: created_tables = set()
        result = [ table ]
        for index in table.indexes.values():
            if index.is_pk or index.is_unique: continue
            assert index.name is not None
            result.append(index)
        schema = table.schema
        if schema.named_foreign_keys:
            for foreign_key in table.foreign_keys.values():
                if foreign_key.parent_table not in created_tables: continue
                result.append(foreign_key)
            for child_table in table.child_tables:
                if child_table not in created_tables: continue
                for foreign_key in child_table.foreign_keys.values():
                    if foreign_key.parent_table is not table: continue
                    result.append(foreign_key)
        created_tables.add(table)
        return result
    def add_column(table, column_name, sql_type, is_not_null=None, sql_default=None):
        return table.schema.column_class(column_name, table, sql_type, is_not_null, sql_default)
    def add_index(table, index_name, columns, is_pk=False, is_unique=None, m2m=False):
        assert index_name is not False
        if index_name is True: index_name = None
        if index_name is None and not is_pk:
            provider = table.schema.provider
            index_name = provider.get_default_index_name(table.name, (column.name for column in columns),
                                                         is_pk=is_pk, is_unique=is_unique, m2m=m2m)
        index = table.indexes.get(columns)
        if index and index.name == index_name and index.is_pk == is_pk and index.is_unique == is_unique:
            return index
        return table.schema.index_class(index_name, table, columns, is_pk, is_unique)
    def add_foreign_key(table, fk_name, child_columns, parent_table, parent_columns, index_name=None):
        if fk_name is None:
            provider = table.schema.provider
            child_column_names = tuple(column.name for column in child_columns)
            fk_name = provider.get_default_fk_name(table.name, parent_table.name, child_column_names)
        return table.schema.fk_class(fk_name, table, child_columns, parent_table, parent_columns, index_name)

class Column(object):
    auto_template = '%(type)s PRIMARY KEY AUTOINCREMENT'
    def __init__(column, name, table, sql_type, is_not_null=None, sql_default=None):
        if name in table.column_dict:
            throw(DBSchemaError, "Column %r already exists in table %r" % (name, table.name))
        table.column_dict[name] = column
        table.column_list.append(column)
        column.table = table
        column.name = name
        column.sql_type = sql_type
        column.is_not_null = is_not_null
        column.sql_default = sql_default
        column.is_pk = False
        column.is_pk_part = False
        column.is_unique = False
    def __repr__(column):
        return '<Column(%s.%s)>' % (column.table.name, column.name)
    def get_sql(column):
        table = column.table
        schema = table.schema
        quote_name = schema.provider.quote_name
        case = schema.case
        result = []
        append = result.append
        append(quote_name(column.name))
        if column.is_pk == 'auto' and column.auto_template:
            append(case(column.auto_template % dict(type=column.sql_type)))
        else:
            append(case(column.sql_type))
            if column.is_pk:
                if schema.dialect == 'SQLite': append(case('NOT NULL'))
                append(case('PRIMARY KEY'))
            else:
                if column.is_unique: append(case('UNIQUE'))
                if column.is_not_null: append(case('NOT NULL'))
        if column.sql_default not in (None, True, False):
            append(case('DEFAULT'))
            append(column.sql_default)
        if schema.inline_fk_syntax and not schema.named_foreign_keys:
            foreign_key = table.foreign_keys.get((column,))
            if foreign_key is not None:
                parent_table = foreign_key.parent_table
                append(case('REFERENCES'))
                append(quote_name(parent_table.name))
                append(schema.column_list(foreign_key.parent_columns))
        return ' '.join(result)

class Constraint(DBObject):
    def __init__(constraint, name, schema):
        if name is not None:
            assert name not in schema.names
            if name in schema.constraints: throw(DBSchemaError,
                "Constraint with name %r already exists" % name)
            schema.names[name] = constraint
            schema.constraints[name] = constraint
        constraint.schema = schema
        constraint.name = name

class Index(Constraint):
    typename = 'Index'
    def __init__(index, name, table, columns, is_pk=False, is_unique=None):
        assert len(columns) > 0
        for column in columns:
            if column.table is not table: throw(DBSchemaError,
                "Column %r does not belong to table %r and cannot be part of its index"
                % (column.name, table.name))
        if columns in table.indexes:
            if len(columns) == 1: throw(DBSchemaError, "Index for column %r already exists" % columns[0].name)
            else: throw(DBSchemaError, "Index for columns (%s) already exists" % ', '.join(repr(column.name) for column in columns))
        if is_pk:
            if table.pk_index is not None: throw(DBSchemaError,
                'Primary key for table %r is already defined' % table.name)
            table.pk_index = index
            if is_unique is None: is_unique = True
            elif not is_unique: throw(DBSchemaError,
                "Incompatible combination of is_unique=False and is_pk=True")
        elif is_unique is None: is_unique = False
        schema = table.schema
        if name is not None and name in schema.names:
            throw(DBSchemaError, 'Index %s cannot be created, name is already in use')
        Constraint.__init__(index, name, schema)
        for column in columns:
            column.is_pk = len(columns) == 1 and is_pk
            column.is_pk_part = bool(is_pk)
            column.is_unique = is_unique and len(columns) == 1
        table.indexes[columns] = index
        index.table = table
        index.columns = columns
        index.is_pk = is_pk
        index.is_unique = is_unique
    def exists(index, provider, connection, case_sensitive=True):
        return provider.index_exists(connection, index.table.name, index.name, case_sensitive)
    def get_sql(index):
        return index._get_create_sql(inside_table=True)
    def get_create_command(index):
        return index._get_create_sql(inside_table=False)
    def _get_create_sql(index, inside_table):
        schema = index.schema
        case = schema.case
        quote_name = schema.provider.quote_name
        cmd = []
        append = cmd.append
        if not inside_table:
            if index.is_pk: throw(DBSchemaError,
                'Primary key index cannot be defined outside of table definition')
            append(case('CREATE'))
            if index.is_unique: append(case('UNIQUE'))
            append(case('INDEX'))
            # if schema.provider.index_if_not_exists_syntax:
            #     append(case('IF NOT EXISTS'))
            append(quote_name(index.name))
            append(case('ON'))
            append(quote_name(index.table.name))
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
    typename = 'Foreign key'
    def __init__(foreign_key, name, child_table, child_columns, parent_table, parent_columns, index_name):
        schema = parent_table.schema
        if schema is not child_table.schema: throw(DBSchemaError,
            'Parent and child tables of foreign_key cannot belong to different schemata')
        for column in parent_columns:
            if column.table is not parent_table: throw(DBSchemaError,
                'Column %r does not belong to table %r' % (column.name, parent_table.name))
        for column in child_columns:
            if column.table is not child_table: throw(DBSchemaError,
                'Column %r does not belong to table %r' % (column.name, child_table.name))
        if len(parent_columns) != len(child_columns): throw(DBSchemaError,
            'Foreign key columns count do not match')
        if child_columns in child_table.foreign_keys:
            if len(child_columns) == 1: throw(DBSchemaError, 'Foreign key for column %r already defined' % child_columns[0].name)
            else: throw(DBSchemaError, 'Foreign key for columns (%s) already defined' % ', '.join(repr(column.name) for column in child_columns))
        if name is not None and name in schema.names:
            throw(DBSchemaError, 'Foreign key %s cannot be created, name is already in use' % name)
        Constraint.__init__(foreign_key, name, schema)
        child_table.foreign_keys[child_columns] = foreign_key
        if child_table is not parent_table:
            child_table.parent_tables.add(parent_table)
            parent_table.child_tables.add(child_table)
        foreign_key.parent_table = parent_table
        foreign_key.parent_columns = parent_columns
        foreign_key.child_table = child_table
        foreign_key.child_columns = child_columns

        if index_name is not False:
            child_columns_len = len(child_columns)
            for columns in child_table.indexes:
                if columns[:child_columns_len] == child_columns: break
            else: child_table.add_index(index_name, child_columns, is_pk=False,
                                        is_unique=False, m2m=bool(child_table.m2m))
    def exists(foreign_key, provider, connection, case_sensitive=True):
        return provider.fk_exists(connection, foreign_key.child_table.name, foreign_key.name, case_sensitive)
    def get_sql(foreign_key):
        return foreign_key._get_create_sql(inside_table=True)
    def get_create_command(foreign_key):
        return foreign_key._get_create_sql(inside_table=False)
    def _get_create_sql(foreign_key, inside_table):
        schema = foreign_key.schema
        case = schema.case
        quote_name = schema.provider.quote_name
        cmd = []
        append = cmd.append
        if not inside_table:
            append(case('ALTER TABLE'))
            append(quote_name(foreign_key.child_table.name))
            append(case('ADD'))
        if schema.named_foreign_keys and foreign_key.name:
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
