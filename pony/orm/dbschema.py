from __future__ import absolute_import, print_function, division
from pony.py23compat import iteritems, itervalues, basestring, imap

from operator import attrgetter

import pony
from pony import orm
from pony.orm.core import log_sql, DBSchemaError, MappingError, UpgradeError
from pony.orm.dbapiprovider import obsolete
from pony.utils import throw, get_version_tuple
from collections import OrderedDict

from pony.migrate.operations import Op, alter_table, OperationBatch


class DBSchema(object):
    dialect = None
    inline_fk_syntax = True
    named_foreign_keys = True
    upgrades = []

    ADD_COLUMN = 'ADD COLUMN'
    ALTER_COLUMN = 'ALTER COLUMN'

    def __init__(schema, provider):
        schema.provider = provider
        schema.tables = {}
        schema.constraints = {}
        schema.indent = '  '
        schema.command_separator = ';\n\n'
        schema.names = {}
    def names_row(schema, col_names):
        quote_name = schema.provider.quote_name
        return '(%s)' % ', '.join(imap(quote_name, col_names))
    def add_table(schema, table_name, entity=None, **kwargs):
        return schema.table_class(schema, table_name, entity, **kwargs)
    def order_tables_to_create(schema):
        tables = []
        created_tables = set()
        split = schema.provider.split_table_name
        tables_to_create = sorted(itervalues(schema.tables), key=lambda table: split(table.name))
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
    def get_pony_version(schema, connection, create_version_table=False):
        provider = schema.provider
        cursor = connection.cursor()
        any_table_exists = any(provider.table_exists(cursor, table_name, case_sensitive=False)
                               for table_name in schema.tables)
        db_version = provider.get_pony_version(connection)
        if db_version is None:
            db_version = pony.__version__ if not any_table_exists else '0.7'
            if create_version_table:
                provider.make_pony_version_table(connection, version=db_version)
        return db_version
    def get_upgrades(schema, db_version):
        db_version = get_version_tuple(db_version)
        current_version = get_version_tuple(pony.__version__)
        return [ upgrade for upgrade in schema.upgrades
                 if db_version < get_version_tuple(upgrade.version) <= current_version ]
    def create_tables(schema, connection):
        provider = schema.provider
        cursor = connection.cursor()
        created_tables = set()
        for table in schema.order_tables_to_create():
            for db_object in table.get_objects_to_create(created_tables):
                base_name = provider.base_name(db_object.name)
                name = db_object.exists(provider, cursor, case_sensitive=False)
                if name is None: db_object.create(provider, cursor)
                elif schema.provider.dialect != 'SQLite' and name != base_name:
                    quote_name = provider.quote_name
                    n1, n2 = quote_name(db_object.name), quote_name(name)
                    tn1, tn2 = db_object.typename, db_object.typename.lower()
                    throw(DBSchemaError, '%s `%s` cannot be created, because %s `%s` ' \
                                         '(with a different letter case) already exists in the database. ' \
                                         'Try to delete `%s` %s first.' % (tn1, n1, tn2, n2, n2, tn2))
    def check_tables(schema, connection):
        provider = schema.provider
        cursor = connection.cursor()
        split = provider.split_table_name
        for table in sorted(itervalues(schema.tables), key=lambda table: split(table.name)):
            alias = provider.base_name(table.name)
            sql_ast = [ 'SELECT',
                        [ 'ALL', ] + [ [ 'COLUMN', alias, column.name ] for column in table.column_list ],
                        [ 'FROM', [ alias, 'TABLE', table.name ] ],
                        [ 'WHERE', [ 'EQ', [ 'VALUE', 0 ], [ 'VALUE', 1 ] ] ]
                      ]
            sql, adapter = provider.ast2sql(sql_ast)
            provider.execute(cursor, sql)

class DBObject(object):
    def create(db_object, provider, cursor):
        sql = db_object.get_create_command()
        provider.execute(cursor, sql)
    def get_create_ops(db_object):
        sql = db_object.get_create_command()
        return [ Op(sql, obj=db_object, type='create') ]

class Table(DBObject):
    typename = 'Table'
    rename_table_sql_template = "ALTER TABLE %(prev_name)s RENAME TO %(new_name)s"
    def __init__(table, schema, name, entity=None):
        if name in schema.tables:
            throw(DBSchemaError, "Table `%s` already exists in database schema" % name)
        if name in schema.names:
            throw(DBSchemaError, "Table `%s` cannot be created, name is already in use" % name)
        schema.tables[name] = table
        schema.names[name] = table
        table.schema = schema
        table.name = name
        table.prev = table.new = None
        table.column_list = []
        table.column_dict = {}
        table.indexes = {}
        table.pk_index = None
        table.foreign_keys = {}
        table.parent_tables = set()
        table.child_tables = set()
        table.entities = set()
        table.options = {}
        if entity is not None:
            table.entities.add(entity)
            table.options = entity._table_options_
        table.m2m = set()
    def __repr__(table):
        return '<Table %s>' % table.schema.provider.format_table_name(table.name)
    def add_entity(table, entity):
        for e in table.entities:
            if e._root_ is not entity._root_:
                throw(MappingError, "Entities %s and %s cannot be mapped to table %s "
                                   "because they don't belong to the same hierarchy"
                                   % (e, entity, table.name))
        assert '_table_options_' not in entity.__dict__
        table.entities.add(entity)
    def exists(table, provider, cursor, case_sensitive=True):
        return provider.table_exists(cursor, table.name, case_sensitive)
    def db_rename(table, cursor):
        provider = table.schema.provider
        provider.rename_table(cursor, obsolete(table.name), table.name)
    def _schema_rename(table, new_name):
        schema = table.schema
        assert new_name not in schema.tables
        schema.tables.pop(table.name)
        table.name = new_name
        schema.tables[new_name] = table
    def get_create_command(table):
        schema = table.schema
        provider = schema.provider
        quote_name = provider.quote_name
        if_not_exists = False # provider.table_if_not_exists_syntax and provider.index_if_not_exists_syntax
        cmd = []
        if not if_not_exists: cmd.append('CREATE TABLE %s (' % quote_name(table.name))
        else: cmd.append('CREATE TABLE IF NOT EXISTS %s (' % quote_name(table.name))
        for column in table.column_list:
            cmd.append(schema.indent + column.get_sql() + ',')
        if len(table.pk_index.col_names) > 1:
            cmd.append(schema.indent + table.pk_index.get_sql() + ',')
        indexes = [ index for index in itervalues(table.indexes)
                    if not index.is_pk and index.is_unique and len(index.col_names) > 1 ]
        for index in indexes: assert index.name is not None
        indexes.sort(key=attrgetter('name'))
        for index in indexes: cmd.append(schema.indent+index.get_sql() + ',')
        if not schema.named_foreign_keys:
            for fk in sorted(itervalues(table.foreign_keys), key=lambda fk: fk.name):
                if schema.inline_fk_syntax and len(fk.col_names) == 1: continue
                cmd.append(schema.indent + fk.get_sql() + ',')
        cmd[-1] = cmd[-1][:-1]
        cmd.append(')')
        for name, value in sorted(table.options.items()):
            option = table.format_option(name, value)
            if option: cmd.append(option)
        return '\n'.join(cmd)
    def format_option(table, name, value):
        if value is True:
            return name
        if value is False:
            return None
        return '%s %s' % (name, value)

    def get_rename_ops(table):
        schema = table.schema
        provider = schema.provider
        quote_name = provider.quote_name
        sql = '%s %s' % ('RENAME TO', quote_name(table.new.name))
        return [ Op(sql, obj=table, type='rename', prefix=alter_table(table)) ]
    def get_alter_ops(table):
        schema = table.schema
        drops = []
        ops = []
        for column in table.column_list:
            if column.prev is None:
                sql = '%s %s' % (schema.ADD_COLUMN, column.get_sql())
                ops.append(Op(sql, obj=column, type='create', prefix=alter_table(table)))
            elif column.get_definition() != column.prev.get_definition():
                ops.extend(column.get_alter_ops())
        for cols, fkey in table.foreign_keys.items():
            sql = fkey.get_create_command()
            prev_fkey = table.prev.foreign_keys.get(cols)
            if prev_fkey is None:
                ops.append(Op(sql, obj=fkey, type='create'))
            elif sql != prev_fkey.get_create_command():
                drops.extend(prev_fkey.get_drop_ops())
                ops.append(Op(sql, obj=fkey, type='create'))
        for cols, index in table.indexes.items():
            if index.is_pk: continue
            sql = index.get_create_command()
            prev_index = table.prev.indexes.get(cols)
            if prev_index is None:
                ops.append(Op(sql, obj=index, type='create'))
            elif sql != prev_index.get_create_command():
                drops.extend(prev_index.get_drop_ops())
                ops.append(Op(sql, obj=index, type='create'))
        for column in table.prev.column_list:
            if column.new is None:
                drops.extend(column.get_drop_ops())
        return drops + ops
    def get_drop_ops(table):
        schema = table.schema
        quote_name = schema.provider.quote_name
        sql = 'DROP TABLE %s' % quote_name(table.name)
        return [ Op(sql, obj=table, type='drop') ]
    def get_objects_to_create(table, created_tables=None):
        if created_tables is None: created_tables = set()
        created_tables.add(table)
        result = [ table ]
        indexes = [ index for index in itervalues(table.indexes) if not index.is_pk and not index.is_unique ]
        for index in indexes: assert index.name is not None
        indexes.sort(key=attrgetter('name'))
        result.extend(indexes)
        schema = table.schema
        if schema.named_foreign_keys:
            for fk in sorted(itervalues(table.foreign_keys), key=lambda fk: fk.name):
                if fk.parent_table not in created_tables: continue
                result.append(fk)
            for child_table in table.child_tables:
                if child_table not in created_tables: continue
                for fk in sorted(itervalues(child_table.foreign_keys), key=lambda fk: fk.name):
                    if fk.parent_table is not table: continue
                    result.append(fk)
        return result
    def add_column(table, column_name, sql_type, converter, is_not_null=None, sql_default=None):
        return table.schema.column_class(column_name, table, sql_type, converter, is_not_null, sql_default)
    def make_index_name(table, col_names, is_pk, is_unique, m2m, provided_name):
        if isinstance(provided_name, basestring): return provided_name
        return table.schema.provider.get_default_index_name(
                table.name, col_names, is_pk=is_pk, is_unique=is_unique, m2m=m2m)
    def add_index(table, col_names, is_pk=False, is_unique=None, m2m=False, provided_name=None):
        assert type(col_names) is tuple
        assert provided_name is not False
        index_name = table.make_index_name(col_names, is_pk, is_unique, m2m, provided_name=provided_name)
        index = table.indexes.get(col_names)
        if index:
            if index.is_pk:
                return index
            if index.name == index_name and index.is_unique == is_unique and is_pk == index.is_pk:
                return index
            elif not is_pk:
                throw(DBSchemaError, 'Two different indexes are defined for the same column%s for table `%s`: %s'
                                 % ('s' if len(col_names) > 1 else '', table.name, ', '.join(col_names)))
            # is_pk == True
            del table.indexes[col_names]
        return table.schema.index_class(index_name, table, col_names, is_pk, is_unique)
    def add_foreign_key(table, fk_name, col_names, parent_table, parent_col_names, index_name=None):
        assert type(parent_col_names) is tuple
        assert type(col_names) is tuple
        if fk_name is None:
            provider = table.schema.provider
            fk_name = provider.get_default_fk_name(table.name, col_names)
        return table.schema.fk_class(fk_name, table, col_names, parent_table, parent_col_names, index_name)
    def rename_column(table, prev_name, new_name, with_constraints=True):
        assert new_name not in table.column_dict
        column = table.column_dict.pop(prev_name)
        column.name = new_name
        table.column_dict[new_name] = column
        if with_constraints: throw(NotImplementedError)
    def rename_columns(table, renamed_columns):
        for prev_name, new_name in iteritems(renamed_columns):
            table.rename_column(prev_name, new_name, with_constraints=False)

        provider = table.schema.provider
        for index in itervalues(table.indexes):
            if any(name in renamed_columns for name in index.col_names):
                assert not index.is_pk
                new_index_col_names = tuple(renamed_columns.get(name, name) for name in index.col_names)
                new_index_name = table.make_index_name(new_index_col_names, is_pk=False, is_unique=index.is_unique,
                                                       m2m=index.m2m, provided_name=index.provided_name)
                if new_index_name != index.name: index.rename(new_index_name)

        for fk in itervalues(table.foreign_keys):
            if any(name in renamed_columns for name in fk.col_names):
                new_fk_col_names = tuple(renamed_columns.get(name, name) for name in fk.col_names)
                new_fk_name = provider.get_default_fk_name(table.name, new_fk_col_names)
                if new_fk_name != fk.name: fk.rename(new_fk_name)
    def remove_column(table, column_name, constraints_only=False):
        for index in list(itervalues(table.indexes)):
            if column_name in index.col_names:
                assert not index.is_pk
                index.remove()
        for fk in list(itervalues(table.foreign_keys)):
            if column_name in fk.col_names:
                fk.remove()
        if not constraints_only:
            column = table.column_dict.pop(column_name)
            table.column_list.remove(column)
    def remove(table):
        for fk in list(itervalues(table.foreign_keys)):
            fk.remove()
        for index in list(itervalues(table.indexes)):
            index.remove()
        schema = table.schema
        del schema.tables[table.name]
        del schema.names[table.name]

class Column(object):

    pk_constraint_template = 'CONSTRAINT %(constraint_name)s'
    auto_template = '%(type)s %(pk_constraint_template)s PRIMARY KEY AUTOINCREMENT'
    rename_sql_template = 'ALTER TABLE %(table_name)s RENAME COLUMN %(prev_name)s TO %(new_name)s'

    def __init__(column, name, table, sql_type, converter=None, is_not_null=None, sql_default=None):
        if name in table.column_dict:
            throw(DBSchemaError, "Column `%s` already exists in table `%s`" % (name, table.name))
        table.column_dict[name] = column
        table.column_list.append(column)
        column.table = table
        column.name = name
        column.prev = column.new = None
        column.sql_type = sql_type
        column.is_not_null = is_not_null
        column.sql_default = sql_default
        column.is_pk = False
        column.is_pk_part = False
        column.is_unique = False
        column.converter = converter
        column.attr = None
    def __repr__(column):
        return '<Column `%s`.`%s`>' % (column.table.name, column.name)
    def _schema_rename(column, new_name):
        table = column.table
        assert new_name not in table.column_dict
        table.column_dict.pop(column.name)
        column.name = new_name
        table.column_dict[new_name] = column
    def db_rename(column, cursor):
        schema = column.table.schema
        provider = schema.provider
        quote_name = provider.quote_name
        sql = column.rename_sql_template % dict(
            table_name=quote_name(column.table.name),
            prev_name=quote_name(obsolete(column.name)),
            new_name=quote_name(column.name))
        provider.execute(cursor, sql)
    def get_sql(column):
        table = column.table
        quote_name = table.schema.provider.quote_name
        return '%s %s' % (quote_name(column.name), column.get_definition())
    def get_definition(column):
        table = column.table
        schema = table.schema
        quote_name = schema.provider.quote_name
        result = []
        append = result.append
        if column.is_pk:
            constraint_name = quote_name(column.table.pk_index.name)
            pk_constraint_template = column.pk_constraint_template % {
                'constraint_name': constraint_name
            }
        if column.is_pk == 'auto' and column.auto_template:
            append(column.auto_template % {
                'type': column.sql_type,
                'pk_constraint_template': pk_constraint_template,
            })
        else:
            append(column.sql_type)
            if column.is_pk:
                if schema.provider.dialect == 'SQLite': append('NOT NULL')
                append(pk_constraint_template)
                append('PRIMARY KEY')
            else:
                if schema.provider.dialect == 'Oracle' \
                        and column.sql_default not in (None, True, False):
                    append('DEFAULT')
                    append(column.sql_default)
                if column.is_unique: append('UNIQUE')
                if column.is_not_null: append('NOT NULL')
        if column.sql_default not in (None, True, False) and schema.provider.dialect != 'Oracle':
            append('DEFAULT')
            append(column.sql_default)
        if schema.inline_fk_syntax and not schema.named_foreign_keys:
            fk = table.foreign_keys.get((column.name,))
            if fk is not None:
                parent_table = fk.parent_table
                append('REFERENCES')
                append(quote_name(parent_table.name))
                append(schema.names_row(fk.parent_col_names))
        return ' '.join(result)

    def get_alter_ops(column):
        table = column.table
        schema = column.table.schema
        sql = '%s %s' % (schema.ALTER_COLUMN, column.get_sql())

        provider = table.schema.provider
        sql, _ = provider.ast2sql(['ALTER COLUMN'])
        return [ Op(sql, obj=column, type='alter', prefix=alter_table(table)) ]

    def get_drop_ops(column):
        table = column.table
        schema = table.schema
        quote_name = schema.provider.quote_name
        sql = 'DROP COLUMN %s' % quote_name(column.name)
        return [ Op(sql, obj=column, type='drop', prefix=alter_table(table)) ]

class Constraint(DBObject):
    rename_sql_template = 'ALTER TABLE %(table_name)s RENAME CONSTRAINT %(prev_name)s TO %(new_name)s'
    drop_sql_template = 'ALTER TABLE %(table_name)s DROP CONSTRAINT %(name)s'
    def __init__(constraint, name, table):
        schema = table.schema
        if name is not None:
            assert name not in schema.names
            if name in schema.constraints: throw(DBSchemaError,
                "Constraint with name `%s` already exists" % name)
            schema.names[name] = constraint
            schema.constraints[name] = constraint
        constraint.name = name
        constraint.prev = constraint.new = None
        constraint.table = table
    def rename(constraint, new_name):
        schema = constraint.table.schema
        assert new_name not in schema.names
        del schema.names[constraint.name]
        del schema.constraints[constraint.name]
        constraint.name = new_name
        schema.names[new_name] = constraint
        schema.constraints[new_name] = constraint
    def remove(constraint):
        schema = constraint.table.schema
        del schema.names[constraint.name]
        del schema.constraints[constraint.name]
    def can_be_renamed(constraint):
        return True
    def db_rename(constraint, cursor):
        provider = constraint.table.schema.provider
        quote_name = provider.quote_name
        prev_name = quote_name(constraint.name.obsolete_name)
        new_name = quote_name(constraint.name)
        if constraint.can_be_renamed():
            sql = constraint.rename_sql_template % dict(
                table_name=constraint.table.name, prev_name=prev_name, new_name=new_name)
            provider.execute(cursor, sql)
        else:
            drop_sql = constraint.drop_sql_template % dict(
                table_name=constraint.table.name, name=prev_name)
            provider.execute(cursor, drop_sql)
            constraint.create(provider, cursor)

class Trigger(Constraint):
    typename = 'Trigger'

class DBIndex(Constraint):
    typename = 'Index'
    rename_sql_template = 'ALTER INDEX %(prev_name)s RENAME TO %(new_name)s'
    drop_sql_template = 'DROP INDEX %(name)s'
    def __init__(index, name, table, col_names, is_pk=False, is_unique=None, provided_name=None):
        assert len(col_names) > 0
        columns = []
        for col_name in col_names:
            column = table.column_dict.get(col_name)
            if column is None: throw(DBSchemaError,
                "Column `%s` does not belong to table `%s` and cannot be part of its index"
                % (col_name, table.name))
            columns.append(column)
        if col_names in table.indexes:
            if len(col_names) == 1: throw(DBSchemaError, "Index for column `%s` already exists" % col_names[0])
            else: throw(DBSchemaError, "Index for columns (%s) already exists"
                                       % ', '.join('`%s`' % column.name for column in columns))
        if is_pk:
            if table.pk_index is not None: throw(DBSchemaError,
                'Primary key for table `%s` is already defined' % table.name)
            table.pk_index = index
            if is_unique is None: is_unique = True
            elif not is_unique: throw(DBSchemaError,
                "Incompatible combination of is_unique=False and is_pk=True")
        elif is_unique is None: is_unique = False
        schema = table.schema
        if name is not None and name in schema.names:
            throw(DBSchemaError, 'Index `%s` cannot be created, name is already in use' % name)
        Constraint.__init__(index, name, table)

        if len(col_names) == 1:
            column = columns[0]
            if is_pk: column.is_pk = is_pk
            if is_unique: column.is_unique = is_unique
        if is_pk:
            for column in columns:
                column.is_pk_part = True

        table.indexes[col_names] = index
        index.col_names = col_names
        index.is_pk = is_pk
        index.is_unique = is_unique
        index.provided_name = provided_name
    def remove(index):
        table = index.table
        del table.indexes[index.col_names]
        Constraint.remove(index)
    def exists(index, provider, cursor, case_sensitive=True):
        return provider.index_exists(cursor, index.table.name, index.name, case_sensitive)
    def get_sql(index):
        return index._get_create_sql(inside_table=True)
    def get_create_command(index):
        return index._get_create_sql(inside_table=False)
    def _get_create_sql(index, inside_table):
        schema = index.table.schema
        quote_name = schema.provider.quote_name
        cmd = []
        append = cmd.append
        if not inside_table:
            if index.is_pk: throw(DBSchemaError,
                'Primary key index cannot be defined outside of table definition')
            append('CREATE')
            if index.is_unique: append('UNIQUE')
            append('INDEX')
            # if schema.provider.index_if_not_exists_syntax:
            #     append('IF NOT EXISTS')
            append(quote_name(index.name))
            append('ON')
            append(quote_name(index.table.name))
        else:
            if index.name:
                append('CONSTRAINT')
                append(quote_name(index.name))
            if index.is_pk: append('PRIMARY KEY')
            elif index.is_unique: append('UNIQUE')
            else: append('INDEX')
        append(schema.names_row(index.col_names))
        return ' '.join(cmd)
    def get_drop_ops(index):
        table = index.table
        schema = table.schema
        quote_name = schema.provider.quote_name
        sql = '%s %s' % ('DROP INDEX', quote_name(index.name))
        return [ Op(sql, obj=index, type='drop') ]


class ForeignKey(Constraint):
    typename = 'Foreign key'
    def __init__(fk, name, table, col_names, parent_table, parent_col_names, index_name):
        assert type(parent_col_names) is tuple
        assert type(col_names) is tuple
        schema = parent_table.schema
        if schema is not table.schema: throw(DBSchemaError,
            'Parent and child tables of foreign_key cannot belong to different schemata')
        for col_name in parent_col_names:
            column = parent_table.column_dict[col_name]
            if column.table is not parent_table: throw(DBSchemaError,
                'Column `%s` does not belong to table `%s`' % (col_name, parent_table.name))
        for col_name in col_names:
            column = table.column_dict[col_name]
            if column.table is not table: throw(DBSchemaError,
                'Column `%s` does not belong to table `%s`' % (col_name, table.name))
        if len(parent_col_names) != len(col_names): throw(DBSchemaError,
            'Foreign key columns count do not match')
        if col_names in table.foreign_keys:
            if len(col_names) == 1: throw(DBSchemaError, 'Foreign key for column `%s` already defined'
                                                               % col_names[0])
            else: throw(DBSchemaError, 'Foreign key for columns (%s) already defined'
                                       % ', '.join('`%s`' % col_name for col_name in col_names))
        if name is not None and name in schema.names:
            throw(DBSchemaError, 'Foreign key `%s` cannot be created, name is already in use' % name)
        Constraint.__init__(fk, name, table)
        table.foreign_keys[col_names] = fk
        if table is not parent_table:
            table.parent_tables.add(parent_table)
            parent_table.child_tables.add(table)
        fk.col_names = col_names
        fk.parent_table = parent_table
        fk.parent_col_names = parent_col_names

        if index_name is not False:
            child_columns_len = len(col_names)
            if all(index_col_names[:child_columns_len] != col_names for index_col_names in table.indexes):
                table.add_index(col_names, is_pk=False, is_unique=False,
                                m2m=bool(table.m2m), provided_name=index_name)

    def remove(fk):
        del fk.table.foreign_keys[fk.col_names]
        if not any(fk2.parent_table is fk.parent_table
                   for fk2 in itervalues(fk.table.foreign_keys)):
            fk.parent_table.child_tables.remove(fk.table)
            fk.table.parent_tables.remove(fk.parent_table)
        Constraint.remove(fk)
    def exists(fk, provider, cursor, case_sensitive=True):
        return provider.fk_exists(cursor, fk.table.name, fk.name, case_sensitive)
    def get_sql(fk):
        return fk._get_create_sql(inside_table=True)
    def get_create_command(fk):
        return fk._get_create_sql(inside_table=False)
    def _get_create_sql(fk, inside_table):
        schema = fk.table.schema
        quote_name = schema.provider.quote_name
        cmd = []
        append = cmd.append
        if not inside_table:
            append('ALTER TABLE')
            append(quote_name(fk.table.name))
            append('ADD')
        if schema.named_foreign_keys and fk.name:
            append('CONSTRAINT')
            append(quote_name(fk.name))
        append('FOREIGN KEY')
        append(schema.names_row(fk.col_names))
        append('REFERENCES')
        append(quote_name(fk.parent_table.name))
        append(schema.names_row(fk.parent_col_names))
        return ' '.join(cmd)
    def get_drop_ops(foreign_key):
        schema = foreign_key.table.schema
        quote_name = schema.provider.quote_name
        sql = 'DROP FOREIGN KEY %s' % quote_name(foreign_key.name)
        return [ Op(sql, obj=foreign_key, type='drop', prefix=alter_table(foreign_key.table)) ]

DBSchema.table_class = Table
DBSchema.column_class = Column
DBSchema.index_class = DBIndex
DBSchema.fk_class = ForeignKey

class DbUpgrade(object):
    @classmethod
    def get_description(cls, schema, connection):
        assert False, 'abstract method'

    @classmethod
    def apply(cls, schema, connection):
        assert False, 'abstract method'


class RenameM2MTables(DbUpgrade):
    version = '0.8'

    @staticmethod
    def prepare_rename_list(schema):
        ordered_rename_list = []
        tmp_rename_list = []
        name_mapping = {}

        for table in itervalues(schema.tables):
            prev_name = obsolete(table.name)
            #
            #
            name_mapping[prev_name] = table
            if prev_name != table.name:
                tmp_rename_list.append(table)

            tmp_rename_list.sort(key=attrgetter('name.obsolete_name'))


            while tmp_rename_list:
                rest = []

                for table in tmp_rename_list:
                    if table.name not in name_mapping:
                        name_mapping.pop(obsolete(table.name))
                        name_mapping[table.name] = table
                        ordered_rename_list.append(table)
                    else:
                        rest.append(table)

                if len(rest) == len(tmp_rename_list):
                    table = rest[0]
                    throw(UpgradeError, 'Cannot rename table `%s` to `%s`: new name is already taken'
                                        % (table.name.obsolete_name, table.name))
                tmp_rename_list = rest

        for table in ordered_rename_list[:]:

            constraints = sorted(index for key, index in iteritems(table.indexes) if not index.is_pk)
            if schema.named_foreign_keys:
                constraints.extend(fk for key, fk in sorted(iteritems(table.foreign_keys)))

            for constraint in constraints:
                name = constraint.name
                if name != obsolete(name):
                    ordered_rename_list.append(constraint)

        return ordered_rename_list

    @classmethod
    def get_description(cls, schema, connection):
        result = []
        rename_list = cls.prepare_rename_list(schema)
        for obj in rename_list:
            result.append("\trename %s %s -> %s" % (obj.typename.lower(), obsolete(obj.name), obj.name))
        return '\n'.join(result)

    @classmethod
    def apply(cls, schema, connection):
        ordered_rename_list = cls.prepare_rename_list(schema)
        cursor = connection.cursor()
        for obj in ordered_rename_list:
            obj.db_rename(cursor)


DBSchema.upgrades.extend([RenameM2MTables])
