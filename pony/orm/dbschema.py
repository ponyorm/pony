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

    MODIFY_COLUMN = 'ALTER COLUMN'
    ADD_COLUMN = 'ADD COLUMN'

    @property
    def MODIFY_COLUMN_DEF(self):
        return self.MODIFY_COLUMN

    def __init__(schema, provider, uppercase=True):
        schema.provider = provider
        schema.tables = {}
        schema.constraints = {}
        schema.indent = '  '
        schema.command_separator = ';\n\n'
        schema.uppercase = uppercase
        schema.names = {}
    def names_row(schema, col_names):
        quote_name = schema.provider.quote_name
        return '(%s)' % ', '.join(imap(quote_name, col_names))
    def case(schema, s):
        if schema.uppercase: return s.upper().replace('%S', '%s') \
            .replace(')S', ')s').replace('%R', '%r').replace(')R', ')r')
        else: return s.lower()
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
    def objects_to_create(schema):
        created_tables = set()
        result = OrderedDict()
        for table in schema.order_tables_to_create():
            for obj in table.get_objects_to_create(created_tables):
                result.setdefault(table, OrderedDict())[obj.name] = obj
        return result
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
                name = db_object.exists(provider, cursor, case_sensitive=False)
                if name is None: db_object.create(provider, cursor)
                elif schema.provider.dialect != 'SQLite' and name != db_object.name:
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
        yield Op(sql, obj=db_object, type='create')

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
        table_name = table.name
        if isinstance(table_name, tuple):
            table_name = '.'.join(table_name)
        return '<Table `%s`>' % table_name
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
    def get_rename_sql(table, new_name):
        quote_name = table.schema.provider.quote_name
        return 'ALTER TABLE %s RENAME TO %s' \
               % (quote_name(table.name), quote_name(new_name))
    def get_rename_column_sql(table, old_name, new_name):
        schema = table.schema
        quote_name = schema.provider.quote_name
        return ('ALTER TABLE %s %s %s RENAME TO %s'
               % (quote_name(table.name), schema.MODIFY_COLUMN_DEF, quote_name(old_name), quote_name(new_name)))

    extra_create_cmd = ()

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
        cmd.extend(table.extra_create_cmd)
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

    def get_rename_ops(table, prev_name):
        schema = table.schema
        case = schema.case
        provider = schema.provider
        quote_name = provider.quote_name
        new_name = quote_name(table.name)
        sql = case('RENAME TO {}').format(new_name)
        op = Op(sql, obj=table, type='rename', prefix=alter_table(prev_name))

        for entity in table.entities:
            break
        with orm.db_session:
            cache = entity._database_._get_cache()
        if schema.provider.dialect == 'SQLite' and not cache.saved_fk_state:
            op1 = Op('PRAGMA foreign_keys = true', obj=None, type='pragma_foreign_keys')
            op2 = Op('PRAGMA foreign_keys = false', obj=None, type='pragma_foreign_keys')
            yield OperationBatch([op1, op, op2], type='rename')
            raise StopIteration
        yield op

    def get_alter_ops(table, prev, new_tables, **kwargs):
        ops = table._get_alter_ops_unsorted(prev, new_tables, **kwargs)
        ops = sorted(ops, key=lambda op: op.type == 'drop', reverse=True)
        Index = table.schema.index_class
        Column = table.schema.column_class
        for op in ops:
            if isinstance(op.obj, Index) and op.obj.is_pk and op.type == 'create':
                new_columns = {
                    o.obj for o in ops
                    if o.type in ['create', 'alter'] and isinstance(o.obj, Column)
                }
                if set(op.obj.col_names) <= {c.name for c in new_columns}:
                    continue
            yield op

    def _get_alter_ops_unsorted(table, prev, new_tables, **kwargs):
        schema = table.schema
        case = schema.case
        provider = schema.provider
        quote_name = provider.quote_name

        renamed_columns = kwargs['renamed_columns']
        prev_columns = {c.name: c for c in prev.column_list}

        for column in table.column_list:
            sql = column.get_sql()
            column_name = column.name # prev column name
            was_renamed = column_name in renamed_columns
            if was_renamed:
                column_name = renamed_columns.get(column_name, column_name)

            if column_name not in prev_columns:
                op = case('{} {}').format(schema.ADD_COLUMN, sql)
                yield Op(op, column, type='create', prefix=alter_table(table))
                continue
            # FIXME sql is computed multiple times: for table and columns
            prev_column = prev_columns[column_name]
            if was_renamed:
                try:
                    prev_column.name = column.name
                    prev_sql = prev_column.get_sql()
                finally:
                    prev_column.name = column_name
            else:
                prev_sql = prev_column.get_sql()
            if sql == prev_sql:
                continue
            changes = list(column.get_alter_ops(prev_columns[column_name]))
            for op in changes:
                yield op

        column_names = [c.name for c in table.column_list]

        dropped_cols = set()

        for column in prev.column_list:
            if column.name in renamed_columns.values():
                continue
            if column.name not in column_names:
                dropped_cols.add(column)
                for op in column.get_drop_ops():
                    yield op

        fkeys, prev_fkeys = ({
                tuple(c for c in cols): fkey
                for cols, fkey in table.foreign_keys.items()
            }
            for table in (table, prev)
        )
        for cols, fkey in fkeys.items():
            if fkey.name in new_tables[table.name]:
                continue

            sql = fkey.get_sql()
            if cols not in prev_fkeys:
                op = [
                    case('ALTER TABLE %s') % quote_name(table.name), 'ADD', sql
                ]
                yield Op(op, fkey, type='create')
                continue
            if sql != prev_fkeys[cols].get_sql():
                for op in prev_fkeys[cols].get_drop_ops():
                    yield op
                op = [
                    case('ALTER TABLE %s') % quote_name(table.name), 'ADD', sql
                ]
                yield Op(op, fkey, type='create')

        indexes, prev_indexes = ({
                tuple(cols): index
                # tuple(c.name for c in cols): index
                for cols, index in table.indexes.items()
            }
            for table in (table, prev)
        )

        for cols, index in indexes.items():
            prev_index = prev_indexes.get(cols)
            sql = index.get_sql()
            if prev_index and index.get_sql() == prev_index.get_sql():
                continue

            elif not prev or set(prev.pk_index.col_names) <= {c.name for c in dropped_cols}:
                sql = 'ADD {}'.format(sql)
                yield Op(sql, index, type='create', prefix=alter_table(index.table))
                continue
            for op in index.get_alter_ops(prev_index):
                yield op

    def get_drop_ops(table, inside_table=False, **kw):
        assert not inside_table
        schema = table.schema
        case = schema.case
        quote_name = schema.provider.quote_name
        op = case('DROP TABLE %s') % quote_name(table.name)
        yield Op(op, table, type='drop')

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
        schema = table.schema
        quote_name = schema.provider.quote_name
        case = schema.case
        result = []
        append = result.append
        append(quote_name(column.name))
        if column.is_pk:
            constraint_name = quote_name(column.table.pk_index.name)
            pk_constraint_template = case(column.pk_constraint_template) % {
                case('constraint_name'): constraint_name
            }
        if column.is_pk == 'auto' and column.auto_template:
            append(case(column.auto_template) % {
                case('type'): case(column.sql_type),
                case('pk_constraint_template'): pk_constraint_template,
            })
        else:
            append(case(column.sql_type))
            if column.is_pk:
                if schema.provider.dialect == 'SQLite': append(case('NOT NULL'))
                append(pk_constraint_template)
                append(case('PRIMARY KEY'))
            else:
                if schema.provider.dialect == 'Oracle' \
                        and column.sql_default not in (None, True, False):
                    append(case('DEFAULT'))
                    append(column.sql_default)
                if column.is_unique: append(case('UNIQUE'))
                if column.is_not_null: append(case('NOT NULL'))
        if column.sql_default not in (None, True, False) and schema.provider.dialect != 'Oracle':
            append(case('DEFAULT'))
            append(column.sql_default)
        if schema.inline_fk_syntax and not schema.named_foreign_keys:
            fk = table.foreign_keys.get((column.name,))
            if fk is not None:
                parent_table = fk.parent_table
                append(case('REFERENCES'))
                append(quote_name(parent_table.name))
                append(schema.names_row(fk.parent_col_names))
        return ' '.join(result)

    def get_alter_ops(column, prev_column, **kwargs):
        table = column.table
        schema = column.table.schema
        op = '{} {}'.format(schema.MODIFY_COLUMN_DEF, column.get_sql())
        yield Op(op, column, type='alter', prefix=alter_table(table))

    def get_drop_ops(column, table=None, **kw):
        if table is None:
            table = column.table
        schema = table.schema
        quote_name = schema.provider.quote_name
        cmd = [
            schema.case('DROP COLUMN'), quote_name(column.name),
        ]
        yield Op(' '.join(cmd), column, type='drop', prefix=alter_table(table))

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
    def get_drop_ops(index, inside_table=True, table=None):
        schema = index.table.schema
        if table is None:
            table = index.table
        quote_name = schema.provider.quote_name
        sql = 'DROP INDEX %s' % quote_name(index.name)
        yield Op(sql, obj=index, type='drop', prefix=alter_table(table) if inside_table else None)
    def _get_create_sql(index, inside_table):
        schema = index.table.schema
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
        append(schema.names_row(index.col_names))
        return ' '.join(cmd)

    def get_alter_ops(index, prev, **kwargs):

        if index.is_pk:
            for op in index.get_pk_alter_ops(prev):
                yield op
        # if prev is None:
        #     yield ' '.join(('ADD', sql))
        #     raise StopIteration

        # if sql != prev.get_sql():
        #     for item in prev.get_drop_ops():
        #         yield item
        #     yield sql

    def get_pk_alter_ops(index, prev):
        sql = index.get_sql()

        for op in prev.get_drop_ops():
            op.prefix = alter_table(prev.table)
            yield op
        # if not set(c.name for c in index.columns) <= set(new_columns):
        op = ' '.join([
            # case('ALTER TABLE %s') % quote_name(index.table.name),
            'ADD', sql,
        ])
        yield Op(op, index, type='create', prefix=alter_table(index.table))


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
            for index_col_names in table.indexes:
                if index_col_names[:child_columns_len] == col_names: break
            else: table.add_index(col_names, is_pk=False, is_unique=False,
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
    def get_drop_ops(foreign_key, inside_table=True, **kw):
        schema = foreign_key.table.schema
        case = schema.case
        quote_name = schema.provider.quote_name
        cmd = [
            # case('ALTER TABLE'),
            # quote_name(foreign_key.table.name),
            case('DROP FOREIGN KEY'),
            quote_name(foreign_key.name),
        ]
        cmd = ' '.join(cmd)
        table = foreign_key.table
        yield Op(cmd, foreign_key, type='drop', prefix=alter_table(table))

    def get_alter_ops(foreign_key, prev, new_tables, **kwargs):
        for op in foreign_key.get_drop_ops():
            yield op
        for op in foreign_key.get_create_ops():
            yield op

    def _get_create_sql(fk, inside_table):
        schema = fk.table.schema
        case = schema.case
        quote_name = schema.provider.quote_name
        cmd = []
        append = cmd.append
        if not inside_table:
            append(case('ALTER TABLE'))
            append(quote_name(fk.table.name))
            append(case('ADD'))
        if schema.named_foreign_keys and fk.name:
            append(case('CONSTRAINT'))
            append(quote_name(fk.name))
        append(case('FOREIGN KEY'))
        append(schema.names_row(fk.col_names))
        append(case('REFERENCES'))
        append(quote_name(fk.parent_table.name))
        append(schema.names_row(fk.parent_col_names))
        return ' '.join(cmd)

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
