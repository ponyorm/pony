from __future__ import absolute_import, print_function
from pony.py23compat import PY2, imap, basestring, buffer, int_types, unicode

import os.path, sys, re, json
import sqlite3 as sqlite
from decimal import Decimal
from datetime import datetime, date, time, timedelta
from random import random
from collections import defaultdict
from time import strptime
from threading import Lock
from uuid import UUID
from binascii import hexlify
from functools import wraps

from pony.orm import core, dbschema, dbapiprovider
from pony.orm.core import log_orm, MigrationException, MigrationError, MappingError
from pony.orm.ormtypes import Json, TrackedArray
from pony.orm.sqltranslation import SQLTranslator, StringExprMonad
from pony.orm.sqlbuilding import SQLBuilder, Value, join, make_unary_func
from pony.orm.dbapiprovider import DBAPIProvider, Pool, wrap_dbapi_exceptions, Name, obsolete
from pony.orm.migrations import dbschema as vdbschema, Optional
from pony.orm.migrations.dbschema import provided_name
from pony.utils import datetime2timestamp, timestamp2datetime, absolutize_path, localbase, throw, reraise, \
    cut_traceback_depth

class SqliteExtensionUnavailable(Exception):
    pass

NoneType = type(None)

class SQLiteForeignKey(dbschema.ForeignKey):
    def get_create_command(foreign_key):
        assert False  # pragma: no cover

class SQLiteSchema(dbschema.DBSchema):
    dialect = 'SQLite'
    named_foreign_keys = False
    fk_class = SQLiteForeignKey


class SQLiteVirtualForeignKey(vdbschema.ForeignKey):
    def get_inline_sql(self, using_obsolete_names=False, inside_column=False):
        quotate = self.provider.quote_name
        result = []
        if not inside_column:
            result.append('FOREIGN KEY')
            if using_obsolete_names:
                result.append('(%s)' % (', '.join(quotate(obsolete(col.name)) for col in self.cols_from)))
            else:
                result.append('(%s)' % (', '.join(quotate(col.name) for col in self.cols_from)))
        result.append('REFERENCES')
        result.append(quotate(self.table_to.name))
        if using_obsolete_names:
            result.append('(%s)' % (', '.join(quotate(obsolete(col.name)) for col in self.cols_to)))
        else:
            result.append('(%s)' % (', '.join(quotate(col.name) for col in self.cols_to)))
        if self.on_delete:
            result.append('ON DELETE')
            result.append(self.on_delete)
        return ' '.join(result)

    def __eq__(self, other):
        if type(self) != type(other):
            return False
        if self.table.name != other.table.name:
            return False
        if self.table_to.name != other.table_to.name:
            return False
        if self.cols_from != other.cols_from:
            return False
        if self.cols_to != other.cols_to:
            return False
        if self.on_delete != other.on_delete:
            return False
        return True


class SQLiteVirtualIndex(vdbschema.Index):
    def __init__(self, table, cols, index_name=None):
        super(SQLiteVirtualIndex, self).__init__(table, cols, index_name)
        self.old_name = None

    def __eq__(self, other):
        if type(self) != type(other):
            return False
        if self.name != other.name:  # SQLite rename saves new_name instead of instant rename
            return False
        if self.table.name != other.table.name:
            return False
        if self.cols != other.cols:
            return False
        if self.is_unique != other.is_unique:
            return False
        return True

    @vdbschema.sql_op
    def get_drop_sql(self, using_obsolete_names=False):
        quote = self.provider.quote_name
        obs_if = lambda n: obsolete(n) if using_obsolete_names else n
        return 'DROP INDEX ' + quote(obs_if(self.old_name or self.name))


class SQLiteVirtualTable(vdbschema.Table):
    @vdbschema.sql_op
    def get_create_sql(self, using_obsolete_names=False):
        quote = self.provider.quote_name
        if using_obsolete_names:
            header = 'CREATE TABLE %s (\n  ' % quote(obsolete(self.name))
        else:
            header = 'CREATE TABLE %s (\n  ' % quote(self.name)
        body = []
        for col in self.columns.values():
            body.append(col.get_inline_sql(using_obsolete_names))
        if len(self.primary_key.cols) > 1:
            body.append(self.primary_key.get_inline_sql(using_obsolete_names))
        for ck in self.keys:
            if ck.is_pk:
                body.append(ck.get_inline_sql(using_obsolete_names))
        for fk in self.foreign_keys:
            if len(fk.cols_from) > 1:
                body.append(fk.get_inline_sql(using_obsolete_names))
        body = ',\n  '.join(body)

        return header + body + '\n)'


class SQLiteVirtualColumn(vdbschema.Column):
    def __init__(self, table, name, converter, sql_type=None):
        super(SQLiteVirtualColumn, self).__init__(table, name, converter, sql_type)
        self.old_name = None
        self.cast = None

    def get_inline_sql(self, using_obsolete_names=False, ignore_pk=False, without_name=False):
        line = super(SQLiteVirtualColumn, self).get_inline_sql(using_obsolete_names, ignore_pk, without_name)
        if self.check_constraint:
            line += ' CHECK (%s)' % self.check_constraint.check
        return line


class SQLiteVirtualUniqueConstraint(vdbschema.UniqueConstraint):
    inline_syntax = False

    @vdbschema.sql_op
    def get_create_sql(self, using_obsolete_names=False):
        quote = self.provider.quote_name
        result = ['CREATE UNIQUE INDEX']
        result.append(quote(self.name))
        result.append('ON')
        result.append(quote(self.table.name))
        result.append('(%s)' % ', '.join(quote(col.name) for col in self.cols))
        return ' '.join(result)

    @vdbschema.sql_op
    def get_drop_sql(self):
        return 'DROP INDEX %s' % self.provider.quote_name(self.name)

    def exists(self, provider, connection, case_sensitive=True):
        return provider.index_exists(connection, self.table.name, self.name, case_sensitive)


class SQLiteVirtualSchema(vdbschema.Schema):
    dialect = 'SQLite'
    named_foreign_keys = False
    inline_reference = True
    fk_cls = SQLiteVirtualForeignKey
    table_cls = SQLiteVirtualTable
    column_cls = SQLiteVirtualColumn
    index_cls = SQLiteVirtualIndex
    unique_cls = SQLiteVirtualUniqueConstraint

    def __init__(self, vdb, provider):
        super(SQLiteVirtualSchema, self).__init__(vdb, provider)
        self.affected_tables = defaultdict(TableAffection)
        self.subordinates = []
        self.legacy = False

    def get_create_sql_commands(self, using_obsolete_names=False):
        result = []
        for table in self.tables.values():
            result.extend([op.sql for op in table.get_create_sql(using_obsolete_names)])
            for index in table.indexes:
                result.extend([op.sql for op in index.get_create_sql()])
            for ck in table.keys:
                if not ck.is_pk:
                    result.extend([op.sql for op in ck.get_create_sql()])

        return result

    def get_default_table_name(self, e_name):
        obsolete_name = e_name
        new_name = e_name.lower()
        return Name(new_name, obsolete_name=obsolete_name)

    def get_default_m2m_table_name(self, attr1, attr2):
        e1_name, e2_name = attr1.entity.name, attr2.entity.name
        if e1_name < e2_name:
            name = '%s_%s' % (e1_name.lower(), attr1.name)
        else:
            name = '%s_%s' % (e2_name.lower(), attr2.name)

        if attr1.symmetric:
            obsolete_name = attr1.entity.name.lower() + '_' + attr1.name
        else:
            obsolete_name = "%s_%s" % (min(e1_name, e2_name), max(e1_name, e2_name))
        return Name(name, obsolete_name=obsolete_name)

    def create_migration_table_sql(self):
        query = 'create table "migration"'\
                '("name" text primary key, "applied" datetime not null)'
        return query

    def get_applied_sql(self):
        return 'select "name" from "migration"'

    def get_migration_insert_sql(self):
        return 'insert into migration("name", "applied") values(?, ?)'

    @vdbschema.sql_op
    def get_update_move_column_sql(self, table_to, table_to_pk, cols_to, table_from, table_from_pk, cols_from):
        quote = self.provider.quote_name
        result = ['UPDATE %s' % quote(table_to.name)]
        set_section = ', '.join(quote(col.name) for col in cols_to)
        result.append('SET (%s) = ' % set_section)
        select_section = ', '.join(quote(col.name) for col in table_from_pk)
        result.append('(SELECT %s' % select_section)
        result.append(' FROM %s' % quote(table_from.name))
        where_section = ' AND \n      '.join('%s.%s = %s.%s' %
             (quote(table_from.name), quote(col_from.name), quote(table_to.name), quote(pk_col.name))
             for col_from, pk_col in zip(cols_from, table_to_pk)
        )
        result.append(' WHERE %s)' % where_section)
        return '\n'.join(result)

    def find_subordinate_fks(schema, col):
        result = []
        for table in schema.tables.values():
            for fk in table.foreign_keys:
                if col in fk.cols_to:
                    result.append(fk)
        return result

    # migrations methods

    def add_columns(schema, columns):
        if not columns:
            return
        table = columns[0].table
        schema.affected_tables[table.name].added_columns.extend(columns)

    def drop_initial(schema, columns):
        pass

    def add_fk(schema, fk):
        table = fk.table
        schema.affected_tables[table.name].added_fks.append(fk)

    def drop_fk(schema, fk):
        table = fk.table
        table.foreign_keys.remove(fk)
        schema.affected_tables[table.name].removed_fks.append(fk)

    def add_index(schema, index):
        table = index.table
        schema.affected_tables[table.name].added_indexes.append(index)

    def drop_columns(schema, columns):
        for column in columns:
            table = column.table
            new_fk_list = []
            for fk in table.foreign_keys:
                if column not in fk.cols_from:
                    new_fk_list.append(fk)
            table.foreign_keys = new_fk_list
            new_indexes_list = []
            for index in table.indexes:
                if column not in index.cols:
                    new_indexes_list.append(index)
                else:
                    schema.affected_tables[table.name].disappeared_indexes.append(index)
            table.indexes = new_indexes_list
            table.columns.pop(column.name)
            schema.affected_tables[table.name].removed_columns.append(column)

    def rename_table(schema, table, new_name, ignore_indexes=False):
        del schema.tables[table.name]
        schema.ops.extend(table.get_rename_sql(new_name))
        table.name = new_name
        schema.tables[new_name] = table
        if not ignore_indexes:
            for index in table.indexes:
                index_new_name = schema.get_default_index_name(table, index.cols)
                if index.name == index_new_name:
                    continue
                schema.affected_tables[table.name].changed_indexes.append(index)
                if not index.old_name:
                    index.old_name = index.name
                index.name = index_new_name
        for col in table.columns.values():
            unq = col.unique_constraint
            if unq:
                new_name = schema.get_default_key_name(table, [col])
                if new_name != unq.name:
                    schema.rename_key(unq, new_name)
            chk = col.check_constraint
            if chk:
                chk.name = new_name = schema.get_default_check_name(table, col)
                # if new_name != chk.name:
                #     schema.rename_constraint(chk, new_name)

    def rename_constraint(schema, obj, new_name):
        schema.ops.extend(obj.get_drop_sql())
        obj.name = new_name
        schema.ops.extend(obj.get_create_sql())

    def rename_column(schema, column, new_name, ignore_indexes=False):
        return schema.rename_columns([column], [new_name], ignore_indexes)

    def rename_columns(schema, columns, new_names, ignore_indexes=False):
        assert len(columns) == len(new_names)
        table = columns[0].table
        for i, new_name in enumerate(new_names):
            column = columns[i]
            if new_name and new_name != column.name:
                column.old_name = column.name
                column.name = new_name
                schema.affected_tables[column.table.name].renamed_columns.append(column)
        for column in columns:
            for fk in schema.find_subordinate_fks(column):
                schema.affected_tables[column.table.name].changed_fks.append(fk)
            if not ignore_indexes:
                for index in schema.find_subordinate_index(column):
                    if not getattr(index.name, 'autogenerated', False):
                        new_index_name = schema.get_default_index_name(table, index.cols)
                        if index.name != new_index_name:
                            if not index.old_name:
                                index.old_name = index.name
                            index.name = new_index_name
                        schema.affected_tables[column.table.name].changed_indexes.append(index)

        for fk in table.foreign_keys:
            if provided_name(fk):
                continue
            new_fk_name = schema.get_default_fk_name(table, fk.cols_from)
            if new_fk_name != fk.name:
                schema.affected_tables[table.name].changed_fks.append(fk)

        if not ignore_indexes:
            for index in table.indexes:
                if provided_name(index):
                    continue
                new_index_name = schema.get_default_index_name(table, index.cols)
                if new_index_name != index.name:
                    schema.affected_tables[table.name].changed_indexes.append(index)

        for col in table.columns.values():
            unq = col.unique_constraint
            if unq:
                new_name = schema.get_default_key_name(table, [col])
                if new_name != unq.name:
                    schema.affected_tables[table.name].changed_constraints.append(unq)
                    unq.name = new_name
                    # schema.rename_key(unq, new_name)
            chk = col.check_constraint
            if chk:
                new_name = schema.get_default_check_name(table, col)
                chk.name = new_name # We don't use check name in SQLite

    def add_composite_key(schema, table, cols):
        key = schema.key_cls(table, cols)
        schema.ops.extend(key.get_create_sql())

    def change_attribute_class(schema, *args):
        throw(NotImplementedError)

    def drop_composite_key(schema, table, columns):
        for key in table.keys:
            if not key.is_pk:
                if key.cols == columns:
                    break
        else:
            raise MigrationError

        schema.affected_tables[table.name].removed_cks.append(key)
        table.keys.remove(key)

    def add_composite_key(schema, table, cols):
        key = schema.key_cls(table, cols)
        schema.affected_tables[table.name].added_cks.append(key)

    def rename_columns_by_attr(schema, attr, new_name):
        # is being used by RenameAttribute operation
        if 'column' in attr.provided.kwargs or 'columns' in attr.provided.kwargs:
            return
        entity = attr.entity
        if attr.reverse:
            resolved_pk = schema.resolve_pk(attr.reverse.entity, attr)
        for i, column in enumerate(attr.columns):
            table = column.table
            if attr.reverse:
                new_column_name = resolved_pk[i][0]
            else:
                assert i == 0
                new_column_name = schema.get_default_column_name(new_name)
            if column.name == new_column_name:
                continue
            column.old_name = column.name
            column.name = new_column_name
            schema.affected_tables[table.name].renamed_columns.append(column)
            # foreign keys and indexes handling
            assert table
            for index in table.indexes:
                if any(col in attr.columns for col in index.cols):
                    index_new_name = schema.get_default_index_name(table, index.cols)
                    if index_new_name != index.name:
                        schema.affected_tables[table.name].changed_indexes.append(index)
                        if not index.old_name:
                            index.old_name = index.name
                        index.name = index_new_name

        for a in entity.new_attrs.values():
            reverse = a.reverse
            if a.name not in entity.primary_key and reverse:
                if reverse.name in reverse.entity.primary_key:
                    schema.rename_columns_by_attr(reverse, None)  # we're not passing name for reverse attrs

    def move_column_with_data(schema, attr):
        cols_from = attr.reverse.columns
        table_from = cols_from[0].table
        table_from_pk = table_from.primary_key.cols
        for fk in table_from.foreign_keys:
            if fk.cols_from == cols_from:
                break
        else:
            throw(MigrationError, 'Foreign key was not found')
            return  # for pycharm

        for index in table_from.indexes:
            if index.cols == cols_from:
                break
        else:
            throw(MigrationError, 'Index was not found')
            return

        table_to = fk.table_to
        table_to_pk = table_to.primary_key.cols
        cols_to = []
        resolved_pk = schema.resolve_pk(attr.reverse.entity, attr)
        for colname, _, pk_attr, _ in resolved_pk:
            pk_conv = pk_attr.converters[0]
            new_conv = pk_conv.make_fk_converter(attr)
            new_col = schema.column_cls(table_to, colname, new_conv)
            new_col.nullable = True
            cols_to.append(new_col)
            schema.ops.extend(new_col.get_add_sql())
        transfer_data_sql = schema.get_update_move_column_sql(table_to, table_to_pk, cols_to,
                                                              table_from, table_from_pk, cols_from)
        schema.ops.extend(transfer_data_sql)
        schema.affected_tables[fk.table.name].removed_fks.append(fk)
        table_from.foreign_keys.remove(fk)
        schema.affected_tables[index.table.name].removed_indexes.append(index)
        table_from.indexes.remove(index)

        for col in cols_from:
            schema.affected_tables[col.table.name].renamed_columns.append(col)
            table_from.columns.pop(col.name)

        new_fk_name = schema.get_fk_name(attr, table_to, cols_to)
        new_fk = schema.fk_cls(table_to, table_from, cols_to, table_from_pk, new_fk_name)
        if attr.reverse.cascade_delete:
            new_fk.on_delete = 'CASCADE'
        elif isinstance(attr, Optional) and attr.nullable:
            new_fk.on_delete = 'SET NULL'

        index_name = schema.get_index_name(attr, table_to, cols_to)
        new_index = schema.index_cls(table_to, cols_to, index_name)
        schema.affected_tables[index.table.name].added_indexes.append(new_index)

    def change_column_type(schema, column, new_sql_type, cast):
        if column.sql_type != new_sql_type:
            column.sql_type = new_sql_type
            schema.affected_tables[column.table.name].changed_columns.append(column)
            column.cast = cast

    def change_sql_default(schema, column, new_sql_default):
        schema.affected_tables[column.table.name].changed_columns.append(column)
        column.sql_default = new_sql_default

    def change_nullable(schema, column, new_value):
        old_value = column.nullable
        if old_value == new_value:
            return
        column.nullable = new_value
        schema.affected_tables[column.table.name].changed_columns.append(column)

    def update_col_value(schema, col, old_value, new_value):
        # schema.ops.append(SQLOperation(col, col.get_update_value_sql(old_value, new_value)))
        schema.affected_tables[col.table.name].change_col_values.append((col, old_value, new_value))

    def add_unique_constraint(schema, cols):
        unq = schema.unique_cls(cols)
        schema.affected_tables[cols[0].table.name].changed_columns.extend(cols)  # TODO check it

    def drop_unique_constraint(schema, cols):
        table = cols[0].table
        unq = cols[0].unique_constraint
        assert unq in table.constraints
        for col in cols:
            col.unique_constraint = None
        table.constraints.remove(unq)
        schema.affected_tables[table.name].changed_columns.extend(unq.cols)

    def add_check_constraint(schema, col, check):
        if col.check_constraint:
            raise MigrationError
        chk = schema.check_cls(col, check)
        schema.affected_tables[col.table.name].changed_columns.append(col)

    def drop_check_constraint(schema, col):
        table = col.table
        chk = col.check_constraint
        if chk is None:
            raise MigrationError
        assert chk in table.constraints
        table.constraints.remove(chk)
        col.check_constraint = None
        schema.affected_tables[col.table.name].changed_columns.append(col)

    def rename_index(schema, index, new_name):
        table = index.table
        cols = index.cols
        if new_name is True:
            new_index_name = schema.get_default_index_name(table, cols)
            if new_index_name != index.name:
                schema.affected_tables[table.name].changed_indexes.append(index)
                if not index.old_name:
                    index.old_name = index.name
                index.name = new_index_name
        else:
            if new_name != index.name:
                new_index_name = schema.provider.normalize_name(new_name)
                schema.affected_tables[table.name].changed_indexes.append(index)
                if not index.old_name:
                    index.old_name = index.name
                index.name = new_index_name

    def drop_index(schema, index):
        table = schema.tables[index.table.name]
        table.indexes.remove(index)
        schema.affected_tables[table.name].removed_indexes.append(index)

    def rename_foreign_key(schema, fk, new_fk_name):
        throw(NotImplementedError, 'Renaming foreign key is not implemented for SQLite')

    @vdbschema.sql_op
    def table_recreation_sql(schema, table, affection):
        quote = schema.provider.quote_name
        old_colnames, new_colnames = [], []
        for fk in table.foreign_keys:
            if fk in affection.changed_fks:
                fk.name = schema.get_default_fk_name(fk.table, fk.cols_from)  # to make schema consistent
        for col in table.columns.values():
            if col in affection.removed_columns:
                continue
            if col not in affection.added_columns:
                if col in affection.changed_columns and col.cast:
                    old_colnames.append(col.cast.format(colname=quote(col.name), sql_type=col.sql_type))
                    col.cast = None
                else:
                    if col.old_name:
                        old_colnames.append(quote(col.old_name))
                    else:
                        old_colnames.append(quote(col.name))
            else:
                if col.initial is None:
                    continue
                if col in affection.added_columns:
                    old_colnames.append('%r' % col.initial)
                    col.initial = None

            # if col in affection.renamed_columns:
            #     col.name = col.new_name
            #     col.new_name = None
            new_colnames.append(quote(col.name))

        # for removed_col in affection.removed_columns:
        #     table.columns.pop(removed_col.name)

        name = table.name
        tmp_name = '_tmp_%s' % table.name
        table.name = tmp_name
        table.created = False
        sql_ops = [''.join([op.sql for op in table.create()])]
        insert_sql = ['INSERT INTO %s' % quote(table.name)]
        insert_sql.append('(%s)' % ', '.join(new_colnames))
        insert_sql.append('SELECT')
        insert_sql.append(', '.join(old_colnames))
        insert_sql.append('FROM %s' % quote(name))
        sql_ops.append(' '.join(insert_sql))
        sql_ops.append('DROP TABLE %s' % quote(name))
        sql_ops.extend([op.sql for op in table.get_rename_sql(name)])
        table.name = name
        return sql_ops

    def prepare_sql(schema, connection):
        sql_ops = []
        # for op in schema.ops:
        #     sql_ops.append(op.sql)

        not_resolved_attrs = {}
        for attr, table in schema.attrs_to_create.items():
            if table not in schema.tables_to_create:
                not_resolved_attrs[attr] = table
                continue
            columns, fk, index = schema.make_column(attr, table)
            # if attr.unique:
            #     schema.unique_cls(columns)

        schema.attrs_to_create = not_resolved_attrs

        for table in schema.tables_to_create:
            sql_ops.extend(table.create())
            sql_ops.extend(table.create_indexes())

        for op in schema.ops:
            sql_ops.append(op)

        for table_name, affection in schema.affected_tables.items():
            for index in affection.removed_indexes:
                sql_ops.extend(index.get_drop_sql())
                # if index.new_name:
                #     index.name = index.new_name
                #     index.new_name = None

            for index in affection.changed_indexes:
                sql_ops.extend(index.get_drop_sql())
                affection.added_indexes.append(index)

            table = schema.tables[table_name]
            if not affection.should_recreate():
                for col in affection.added_columns:
                    sql_ops.extend(col.get_add_sql())
            else:
                schema.legacy = schema.provider.server_version[:2] >= (3, 25)
                sql = "select name, type, sql " \
                      "from sqlite_master " \
                      "where (type='index' or type='trigger') and tbl_name=? and sql is not null"
                cursor = connection.cursor()
                cursor.execute(sql, (table.name,))
                res = cursor.fetchall()
                index_names = set()
                for index in table.indexes + affection.changed_indexes + affection.removed_indexes +\
                             affection.disappeared_indexes:
                    index_names.add(index.name)
                    if index.old_name:
                        index_names.add(index.old_name)
                for unq in table.constraints:
                    if unq.typename == 'Unique constraint':
                        index_names.add(unq.name)

                sql_ops.extend(schema.table_recreation_sql(table, affection))
                for col, old_val, new_val in affection.change_col_values:
                    sql_ops.extend(col.get_update_value_sql(old_val, new_val))

                for name, obj_type, sql in res:
                    if obj_type == 'index' and name in index_names:
                        continue
                    schema.subordinates.append((name, obj_type, table.name, sql))

            for index in affection.added_indexes:
                if table_name not in [table.name for table in schema.tables_to_create]:
                    sql_ops.extend(index.get_create_sql())

        schema.affected_tables = defaultdict(TableAffection)
        schema.tables_to_create = []
        schema.ops = []
        return sql_ops

    def apply(schema, connection, verbose, sql_only):
        sql_ops = schema.prepare_sql(connection)

        if sql_only:
            for op in sql_ops:
                print(op.sql)
            for _, _, sql in schema.subordinates:
                print(sql)
            return

        last_sql = None
        try:
            cursor = connection.cursor()
            schema.provider.execute(cursor, 'PRAGMA foreign_key = false')
            if schema.legacy:
                schema.provider.execute(cursor, 'PRAGMA legacy_alter_table = true')

            for op in sql_ops:
                last_sql = op.sql
                last_obj = op.obj
                schema.provider.execute(cursor, op.sql)
                if verbose:
                    print(last_sql)
        except Exception as e:
            schema.errors += 1
            if last_sql:
                print('Last SQL: %r' % last_sql, file=sys.stderr)
            if last_obj:
                print('last object: %s %s' % (last_obj.typename, last_obj.name), file=sys.stderr)
            raise

        for name, obj_type, table_name, sql in schema.subordinates:
            try:
                last_sql = sql
                schema.provider.execute(cursor, sql)
                if verbose:
                    print(sql)
            except Exception as e:
                schema.errors += 1
                quote = schema.provider.quote_name
                print("Pony tried to recreate the subordinate %s %s of table %s but failed with exception %s: %s"
                      % (obj_type, quote(name), quote(table_name), e.__class__.__name__, e), file=sys.stderr
                )
                if last_sql:
                    print('Last SQL: %r' % last_sql, file=sys.stderr)

        if schema.legacy:
            schema.provider.execute(cursor, 'PRAGMA legacy_alter_table = false')
        schema.provider.execute(cursor, 'PRAGMA foreign_key = true')


class UniqueList(list):
    def append(self, obj):
        if obj not in self:
            super(UniqueList, self).append(obj)

    def extend(self, objects):
        for obj in objects:
            self.append(obj)


class TableAffection(object):
    def __init__(self):
        self.added_columns = UniqueList()
        self.removed_columns = UniqueList()
        self.renamed_columns = UniqueList()
        self.changed_columns = UniqueList()
        self.added_fks = UniqueList()
        self.removed_fks = UniqueList()
        self.changed_fks = UniqueList()
        self.added_cks = UniqueList()
        self.removed_cks = UniqueList()
        self.added_indexes = UniqueList()
        self.removed_indexes = UniqueList()
        self.disappeared_indexes = []  # used in drop_columns and prepare_sql (dont add idx to subordinate)
        self.changed_indexes = UniqueList()
        self.changed_constraints = UniqueList()
        self.change_col_values = []

    def should_recreate(self):
        sign1 = bool(self.removed_columns or self.changed_columns or self.renamed_columns or self.changed_constraints or
                    self.added_fks or self.removed_fks or self.added_fks or self.removed_cks or self.changed_fks)
        sign2 = any(col.initial for col in self.added_columns)
        return sign1 or sign2


def make_overriden_string_func(sqlop):
    def func(translator, monad):
        sql = monad.getsql()
        assert len(sql) == 1
        translator = monad.translator
        return StringExprMonad(monad.type, [ sqlop, sql[0] ])
    func.__name__ = sqlop
    return func


class SQLiteTranslator(SQLTranslator):
    dialect = 'SQLite'
    sqlite_version = sqlite.sqlite_version_info
    row_value_syntax = False
    rowid_support = True

    StringMixin_UPPER = make_overriden_string_func('PY_UPPER')
    StringMixin_LOWER = make_overriden_string_func('PY_LOWER')

class SQLiteValue(Value):
    __slots__ = []
    def __unicode__(self):
        value = self.value
        if isinstance(value, datetime):
            return self.quote_str(datetime2timestamp(value))
        if isinstance(value, date):
            return self.quote_str(str(value))
        if isinstance(value, timedelta):
            return repr(value.total_seconds() / (24 * 60 * 60))
        return Value.__unicode__(self)
    if not PY2: __str__ = __unicode__

class SQLiteBuilder(SQLBuilder):
    dialect = 'SQLite'
    least_func_name = 'min'
    greatest_func_name = 'max'
    value_class = SQLiteValue
    def __init__(builder, provider, ast):
        builder.json1_available = provider.json1_available
        SQLBuilder.__init__(builder, provider, ast)
    def SELECT_FOR_UPDATE(builder, nowait, skip_locked, *sections):
        assert not builder.indent
        return builder.SELECT(*sections)
    def INSERT(builder, table_name, columns, values, returning=None):
        if not values: return 'INSERT INTO %s DEFAULT VALUES' % builder.quote_name(table_name)
        return SQLBuilder.INSERT(builder, table_name, columns, values, returning)
    def STRING_SLICE(builder, expr, start, stop):
        if start is None:
            start = [ 'VALUE', None ]
        if stop is None:
            stop = [ 'VALUE', None ]
        return "py_string_slice(", builder(expr), ', ', builder(start), ', ', builder(stop), ")"
    def IN(builder, expr1, x):
        if not x:
            return '0 = 1'
        if len(x) >= 1 and x[0] == 'SELECT':
            return builder(expr1), ' IN ', builder(x)
        op = ' IN (VALUES ' if expr1[0] == 'ROW' else ' IN ('
        expr_list = [ builder(expr) for expr in x ]
        return builder(expr1), op, join(', ', expr_list), ')'
    def NOT_IN(builder, expr1, x):
        if not x:
            return '1 = 1'
        if len(x) >= 1 and x[0] == 'SELECT':
            return builder(expr1), ' NOT IN ', builder(x)
        op = ' NOT IN (VALUES ' if expr1[0] == 'ROW' else ' NOT IN ('
        expr_list = [ builder(expr) for expr in x ]
        return builder(expr1), op, join(', ', expr_list), ')'
    def TODAY(builder):
        return "date('now', 'localtime')"
    def NOW(builder):
        return "datetime('now', 'localtime')"
    def YEAR(builder, expr):
        return 'cast(substr(', builder(expr), ', 1, 4) as integer)'
    def MONTH(builder, expr):
        return 'cast(substr(', builder(expr), ', 6, 2) as integer)'
    def DAY(builder, expr):
        return 'cast(substr(', builder(expr), ', 9, 2) as integer)'
    def HOUR(builder, expr):
        return 'cast(substr(', builder(expr), ', 12, 2) as integer)'
    def MINUTE(builder, expr):
        return 'cast(substr(', builder(expr), ', 15, 2) as integer)'
    def SECOND(builder, expr):
        return 'cast(substr(', builder(expr), ', 18, 2) as integer)'
    def datetime_add(builder, funcname, expr, td):
        assert isinstance(td, timedelta)
        modifiers = []
        seconds = td.seconds + td.days * 24 * 3600
        sign = '+' if seconds > 0 else '-'
        seconds = abs(seconds)
        if seconds >= (24 * 3600):
            days = seconds // (24 * 3600)
            modifiers.append(", '%s%d days'" % (sign, days))
            seconds -= days * 24 * 3600
        if seconds >= 3600:
            hours = seconds // 3600
            modifiers.append(", '%s%d hours'" % (sign, hours))
            seconds -= hours * 3600
        if seconds >= 60:
            minutes = seconds // 60
            modifiers.append(", '%s%d minutes'" % (sign, minutes))
            seconds -= minutes * 60
        if seconds:
            modifiers.append(", '%s%d seconds'" % (sign, seconds))
        if not modifiers: return builder(expr)
        return funcname, '(', builder(expr), modifiers, ')'
    def DATE_ADD(builder, expr, delta):
        if delta[0] == 'VALUE' and isinstance(delta[1], timedelta):
            return builder.datetime_add('date', expr, delta[1])
        return 'datetime(julianday(', builder(expr), ') + ', builder(delta), ')'
    def DATE_SUB(builder, expr, delta):
        if delta[0] == 'VALUE' and isinstance(delta[1], timedelta):
            return builder.datetime_add('date', expr, -delta[1])
        return 'datetime(julianday(', builder(expr), ') - ', builder(delta), ')'
    def DATE_DIFF(builder, expr1, expr2):
        return 'julianday(', builder(expr1), ') - julianday(', builder(expr2), ')'
    def DATETIME_ADD(builder, expr, delta):
        if delta[0] == 'VALUE' and isinstance(delta[1], timedelta):
            return builder.datetime_add('datetime', expr, delta[1])
        return 'datetime(julianday(', builder(expr), ') + ', builder(delta), ')'
    def DATETIME_SUB(builder, expr, delta):
        if delta[0] == 'VALUE' and isinstance(delta[1], timedelta):
            return builder.datetime_add('datetime', expr, -delta[1])
        return 'datetime(julianday(', builder(expr), ') - ', builder(delta), ')'
    def DATETIME_DIFF(builder, expr1, expr2):
        return 'julianday(', builder(expr1), ') - julianday(', builder(expr2), ')'
    def RANDOM(builder):
        return 'rand()'  # return '(random() / 9223372036854775807.0 + 1.0) / 2.0'
    PY_UPPER = make_unary_func('py_upper')
    PY_LOWER = make_unary_func('py_lower')
    def FLOAT_EQ(builder, a, b):
        a, b = builder(a), builder(b)
        return 'abs(', a, ' - ', b, ') / coalesce(nullif(max(abs(', a, '), abs(', b, ')), 0), 1) <= 1e-14'
    def FLOAT_NE(builder, a, b):
        a, b = builder(a), builder(b)
        return 'abs(', a, ' - ', b, ') / coalesce(nullif(max(abs(', a, '), abs(', b, ')), 0), 1) > 1e-14'
    def JSON_QUERY(builder, expr, path):
        fname = 'json_extract' if builder.json1_available else 'py_json_extract'
        path_sql, has_params, has_wildcards = builder.build_json_path(path)
        return 'py_json_unwrap(', fname, '(', builder(expr), ', null, ', path_sql, '))'
    json_value_type_mapping = {unicode: 'text', bool: 'integer', int: 'integer', float: 'real'}
    def JSON_VALUE(builder, expr, path, type):
        func_name = 'json_extract' if builder.json1_available else 'py_json_extract'
        path_sql, has_params, has_wildcards = builder.build_json_path(path)
        type_name = builder.json_value_type_mapping.get(type)
        result = func_name, '(', builder(expr), ', ', path_sql, ')'
        if type_name is not None: result = 'CAST(', result, ' as ', type_name, ')'
        return result
    def JSON_NONZERO(builder, expr):
        return builder(expr), ''' NOT IN ('null', 'false', '0', '""', '[]', '{}')'''
    def JSON_ARRAY_LENGTH(builder, value):
        func_name = 'json_array_length' if builder.json1_available else 'py_json_array_length'
        return func_name, '(', builder(value), ')'
    def JSON_CONTAINS(builder, expr, path, key):
        path_sql, has_params, has_wildcards = builder.build_json_path(path)
        return 'py_json_contains(', builder(expr), ', ', path_sql, ',  ', builder(key), ')'
    def ARRAY_INDEX(builder, col, index):
        return 'py_array_index(', builder(col), ', ', builder(index), ')'
    def ARRAY_CONTAINS(builder, key, not_in, col):
        return ('NOT ' if not_in else ''), 'py_array_contains(', builder(col), ', ', builder(key), ')'
    def ARRAY_SUBSET(builder, array1, not_in, array2):
        return ('NOT ' if not_in else ''), 'py_array_subset(', builder(array2), ', ', builder(array1), ')'
    def ARRAY_LENGTH(builder, array):
        return 'py_array_length(', builder(array), ')'
    def ARRAY_SLICE(builder, array, start, stop):
        return 'py_array_slice(', builder(array), ', ', \
               builder(start) if start else 'null', ',',\
               builder(stop) if stop else 'null', ')'
    def MAKE_ARRAY(builder, *items):
        return 'py_make_array(', join(', ', (builder(item) for item in items)), ')'

class SQLiteIntConverter(dbapiprovider.IntConverter):
    def sql_type(converter):
        attr = converter.attr
        if attr is not None and attr.auto: return 'INTEGER'  # Only this type can have AUTOINCREMENT option
        return dbapiprovider.IntConverter.sql_type(converter)

class SQLiteDecimalConverter(dbapiprovider.DecimalConverter):
    inf = Decimal('infinity')
    neg_inf = Decimal('-infinity')
    NaN = Decimal('NaN')
    def sql2py(converter, val):
        try: val = Decimal(str(val))
        except: return val
        exp = converter.exp
        if exp is not None: val = val.quantize(exp)
        return val
    def py2sql(converter, val):
        if type(val) is not Decimal: val = Decimal(val)
        exp = converter.exp
        if exp is not None:
            if val in (converter.inf, converter.neg_inf, converter.NaN):
                throw(ValueError, 'Cannot store %s Decimal value in database' % val)
            val = val.quantize(exp)
        return str(val)

class SQLiteDateConverter(dbapiprovider.DateConverter):
    def sql2py(converter, val):
        try:
            time_tuple = strptime(val[:10], '%Y-%m-%d')
            return date(*time_tuple[:3])
        except: return val
    def py2sql(converter, val):
        return val.strftime('%Y-%m-%d')

class SQLiteTimeConverter(dbapiprovider.TimeConverter):
    def sql2py(converter, val):
        try:
            if len(val) <= 8: dt = datetime.strptime(val, '%H:%M:%S')
            else: dt = datetime.strptime(val, '%H:%M:%S.%f')
            return dt.time()
        except: return val
    def py2sql(converter, val):
        return val.isoformat()

class SQLiteTimedeltaConverter(dbapiprovider.TimedeltaConverter):
    def sql2py(converter, val):
        return timedelta(days=val)
    def py2sql(converter, val):
        return val.days + (val.seconds + val.microseconds / 1000000.0) / 86400.0

class SQLiteDatetimeConverter(dbapiprovider.DatetimeConverter):
    def sql2py(converter, val):
        try: return timestamp2datetime(val)
        except: return val
    def py2sql(converter, val):
        return datetime2timestamp(val)

class SQLiteJsonConverter(dbapiprovider.JsonConverter):
    json_kwargs = {'separators': (',', ':'), 'sort_keys': True, 'ensure_ascii': False}

def dumps(items):
    return json.dumps(items, **SQLiteJsonConverter.json_kwargs)

class SQLiteArrayConverter(dbapiprovider.ArrayConverter):
    array_types = {
        int: ('int', SQLiteIntConverter),
        unicode: ('text', dbapiprovider.StrConverter),
        float: ('real', dbapiprovider.RealConverter)
    }

    def dbval2val(converter, dbval, obj=None):
        if not dbval: return None
        items = json.loads(dbval)
        if obj is None:
            return items
        return TrackedArray(obj, converter.attr, items)

    def val2dbval(converter, val, obj=None):
        return dumps(val)

class LocalExceptions(localbase):
    def __init__(self):
        self.exc_info = None
        self.keep_traceback = False

local_exceptions = LocalExceptions()

def keep_exception(func):
    @wraps(func)
    def new_func(*args):
        local_exceptions.exc_info = None
        try:
            return func(*args)
        except Exception:
            local_exceptions.exc_info = sys.exc_info()
            if not local_exceptions.keep_traceback:
                local_exceptions.exc_info = local_exceptions.exc_info[:2] + (None,)
            raise
        finally:
            local_exceptions.keep_traceback = False
    return new_func


class SQLiteProvider(DBAPIProvider):
    dialect = 'SQLite'
    local_exceptions = local_exceptions
    quote_char = "`"
    max_name_len = 1024

    dbapi_module = sqlite
    dbschema_cls = SQLiteSchema
    vdbschema_cls = SQLiteVirtualSchema
    translator_cls = SQLiteTranslator
    sqlbuilder_cls = SQLiteBuilder
    array_converter_cls = SQLiteArrayConverter

    cast_sql = 'CAST({colname} AS {sql_type})'

    name_before_table = 'db_name'

    server_version = sqlite.sqlite_version_info

    converter_classes = [
        (NoneType, dbapiprovider.NoneConverter),
        (bool, dbapiprovider.BoolConverter),
        (basestring, dbapiprovider.StrConverter),
        (int_types, SQLiteIntConverter),
        (float, dbapiprovider.RealConverter),
        (Decimal, SQLiteDecimalConverter),
        (datetime, SQLiteDatetimeConverter),
        (date, SQLiteDateConverter),
        (time, SQLiteTimeConverter),
        (timedelta, SQLiteTimedeltaConverter),
        (UUID, dbapiprovider.UuidConverter),
        (buffer, dbapiprovider.BlobConverter),
        (Json, SQLiteJsonConverter)
    ]

    def __init__(provider, *args, **kwargs):
        DBAPIProvider.__init__(provider, *args, **kwargs)
        provider.pre_transaction_lock = Lock()
        provider.transaction_lock = Lock()

    @wrap_dbapi_exceptions
    def inspect_connection(provider, conn):
        DBAPIProvider.inspect_connection(provider, conn)
        provider.json1_available = provider.check_json1(conn)

    def restore_exception(provider):
        if provider.local_exceptions.exc_info is not None:
            try: reraise(*provider.local_exceptions.exc_info)
            finally: provider.local_exceptions.exc_info = None

    def acquire_lock(provider):
        provider.pre_transaction_lock.acquire()
        try:
            provider.transaction_lock.acquire()
        finally:
            provider.pre_transaction_lock.release()

    def release_lock(provider):
        provider.transaction_lock.release()

    @wrap_dbapi_exceptions
    def set_transaction_mode(provider, connection, cache):
        assert not cache.in_transaction
        if cache.immediate:
            provider.acquire_lock()
        try:
            cursor = connection.cursor()

            db_session = cache.db_session
            if db_session is not None and db_session.ddl:
                cursor.execute('PRAGMA foreign_keys')
                fk = cursor.fetchone()
                if fk is not None: fk = fk[0]
                if fk:
                    sql = 'PRAGMA foreign_keys = false'
                    if core.local.debug: log_orm(sql)
                    cursor.execute(sql)
                cache.saved_fk_state = bool(fk)
                assert cache.immediate

            if cache.immediate:
                sql = 'BEGIN IMMEDIATE TRANSACTION'
                if core.local.debug: log_orm(sql)
                cursor.execute(sql)
                cache.in_transaction = True
            elif core.local.debug: log_orm('SWITCH TO AUTOCOMMIT MODE')
        finally:
            if cache.immediate and not cache.in_transaction:
                provider.release_lock()

    def commit(provider, connection, cache=None):
        in_transaction = cache is not None and cache.in_transaction
        try:
            DBAPIProvider.commit(provider, connection, cache)
        finally:
            if in_transaction:
                cache.in_transaction = False
                provider.release_lock()

    def rollback(provider, connection, cache=None):
        in_transaction = cache is not None and cache.in_transaction
        try:
            DBAPIProvider.rollback(provider, connection, cache)
        finally:
            if in_transaction:
                cache.in_transaction = False
                provider.release_lock()

    def drop(provider, connection, cache=None):
        in_transaction = cache is not None and cache.in_transaction
        try:
            DBAPIProvider.drop(provider, connection, cache)
        finally:
            if in_transaction:
                cache.in_transaction = False
                provider.release_lock()

    @wrap_dbapi_exceptions
    def release(provider, connection, cache=None):
        if cache is not None:
            db_session = cache.db_session
            if db_session is not None and db_session.ddl and cache.saved_fk_state:
                try:
                    cursor = connection.cursor()
                    sql = 'PRAGMA foreign_keys = true'
                    if core.local.debug: log_orm(sql)
                    cursor.execute(sql)
                except:
                    provider.pool.drop(connection)
                    raise
        DBAPIProvider.release(provider, connection, cache)

    def get_pool(provider, filename, create_db=False, **kwargs):
        if filename != ':memory:':
            # When relative filename is specified, it is considered
            # not relative to cwd, but to user module where
            # Database instance is created

            # the list of frames:
            # 7 - user code: db = Database(...)
            # 6 - cut_traceback decorator wrapper
            # 5 - cut_traceback decorator
            # 4 - pony.orm.Database.__init__() / .bind()
            # 3 - pony.orm.Database._bind()
            # 2 - pony.dbapiprovider.DBAPIProvider.__init__()
            # 1 - SQLiteProvider.__init__()
            # 0 - pony.dbproviders.sqlite.get_pool()
            filename = absolutize_path(filename, frame_depth=cut_traceback_depth+5)
        return SQLitePool(filename, create_db, **kwargs)

    def table_exists(provider, connection, table_name, case_sensitive=True):
        return provider._exists(connection, table_name, None, case_sensitive)

    def column_exists(provider, connection, table_name, column_name, case_sensitive=True):
        db_name, table_name = provider.split_table_name(table_name)
        cursor = connection.cursor()
        sql = 'SELECT name FROM pragma_table_info(?) WHERE name=?'
        if not case_sensitive:
            sql += ' COLLATE NOCASE'
        cursor.execute(sql, [table_name, column_name])
        row = cursor.fetchone()
        return row[0] if row is not None else None

    def index_exists(provider, connection, table_name, index_name, case_sensitive=True):
        return provider._exists(connection, table_name, index_name, case_sensitive)

    def _exists(provider, connection, table_name, index_name=None, case_sensitive=True):
        db_name, table_name = provider.split_table_name(table_name)

        if db_name is None: catalog_name = 'sqlite_master'
        else: catalog_name = (db_name, 'sqlite_master')
        catalog_name = provider.quote_name(catalog_name)

        cursor = connection.cursor()
        if index_name is not None:
            sql = "SELECT name FROM %s WHERE type='index' AND name=?" % catalog_name
            if not case_sensitive: sql += ' COLLATE NOCASE'
            cursor.execute(sql, [ index_name ])
        else:
            sql = "SELECT name FROM %s WHERE type='table' AND name=?" % catalog_name
            if not case_sensitive: sql += ' COLLATE NOCASE'
            cursor.execute(sql, [ table_name ])
        row = cursor.fetchone()
        return row[0] if row is not None else None

    def fk_exists(provider, connection, table_name, fk_name, case_sensitive=True):
        assert False  # pragma: no cover

    def check_json1(provider, connection):
        cursor = connection.cursor()
        sql = '''
            select json('{"this": "is", "a": ["test"]}')'''
        try:
            cursor.execute(sql)
            return True
        except sqlite.OperationalError:
            return False

    def purge(provider, connection):
        cursor = connection.cursor()
        fetch_objects_sql = "select 'drop ' || type || ' ' ||  name || ';' from sqlite_master where name != 'sqlite_sequence';"
        cursor.execute(fetch_objects_sql)
        for row in cursor.fetchall():
            sql = row[0]
            cursor.execute(sql)


provider_cls = SQLiteProvider

def _text_factory(s):
    return s.decode('utf8', 'replace')

def make_string_function(name, base_func):
    def func(value):
        if value is None:
            return None
        t = type(value)
        if t is not unicode:
            if t is buffer:
                value = hexlify(value).decode('ascii')
            else:
                value = unicode(value)
        result = base_func(value)
        return result
    func.__name__ = name
    return func

py_upper = make_string_function('py_upper', unicode.upper)
py_lower = make_string_function('py_lower', unicode.lower)

def py_json_unwrap(value):
    # [null,some-value] -> some-value
    if value is None:
        return None
    assert value.startswith('[null,'), value
    return value[6:-1]

path_cache = {}

json_path_re = re.compile(r'\[(-?\d+)\]|\.(?:(\w+)|"([^"]*)")', re.UNICODE)

def _parse_path(path):
    if path in path_cache:
        return path_cache[path]
    keys = None
    if isinstance(path, basestring) and path.startswith('$'):
        keys = []
        pos = 1
        path_len = len(path)
        while pos < path_len:
            match = json_path_re.match(path, pos)
            if match is not None:
                g1, g2, g3 = match.groups()
                keys.append(int(g1) if g1 else g2 or g3)
                pos = match.end()
            else:
                keys = None
                break
        else: keys = tuple(keys)
    path_cache[path] = keys
    return keys

def _traverse(obj, keys):
    if keys is None: return None
    list_or_dict = (list, dict)
    for key in keys:
        if type(obj) not in list_or_dict: return None
        try: obj = obj[key]
        except (KeyError, IndexError): return None
    return obj

def _extract(expr, *paths):
    expr = json.loads(expr) if isinstance(expr, basestring) else expr
    result = []
    for path in paths:
        keys = _parse_path(path)
        result.append(_traverse(expr, keys))
    return result[0] if len(paths) == 1 else result

def py_json_extract(expr, *paths):
    result = _extract(expr, *paths)
    if type(result) in (list, dict):
        result = json.dumps(result, **SQLiteJsonConverter.json_kwargs)
    return result

def py_json_query(expr, path, with_wrapper):
    result = _extract(expr, path)
    if type(result) not in (list, dict):
        if not with_wrapper: return None
        result = [result]
    return json.dumps(result, **SQLiteJsonConverter.json_kwargs)

def py_json_value(expr, path):
    result = _extract(expr, path)
    return result if type(result) not in (list, dict) else None

def py_json_contains(expr, path, key):
    expr = json.loads(expr) if isinstance(expr, basestring) else expr
    keys = _parse_path(path)
    expr = _traverse(expr, keys)
    return type(expr) in (list, dict) and key in expr

def py_json_nonzero(expr, path):
    expr = json.loads(expr) if isinstance(expr, basestring) else expr
    keys = _parse_path(path)
    expr = _traverse(expr, keys)
    return bool(expr)

def py_json_array_length(expr, path=None):
    expr = json.loads(expr) if isinstance(expr, basestring) else expr
    if path:
        keys = _parse_path(path)
        expr = _traverse(expr, keys)
    return len(expr) if type(expr) is list else 0

def wrap_array_func(func):
    @wraps(func)
    def new_func(array, *args):
        if array is None:
            return None
        array = json.loads(array)
        return func(array, *args)
    return new_func

@wrap_array_func
def py_array_index(array, index):
    try:
        return array[index]
    except IndexError:
        return None

@wrap_array_func
def py_array_contains(array, item):
    return item in array

@wrap_array_func
def py_array_subset(array, items):
    if items is None: return None
    items = json.loads(items)
    return set(items).issubset(set(array))

@wrap_array_func
def py_array_length(array):
    return len(array)

@wrap_array_func
def py_array_slice(array, start, stop):
    return dumps(array[start:stop])

def py_make_array(*items):
    return dumps(items)

def py_string_slice(s, start, end):
    if s is None:
        return None
    if isinstance(start, basestring):
        start = int(start)
    if isinstance(end, basestring):
        end = int(end)
    return s[start:end]

class SQLitePool(Pool):
    def __init__(pool, filename, create_db, **kwargs): # called separately in each thread
        pool.filename = filename
        pool.create_db = create_db
        pool.kwargs = kwargs
        pool.con = None
    def _connect(pool):
        filename = pool.filename
        if filename != ':memory:' and not pool.create_db and not os.path.exists(filename):
            throw(IOError, "Database file is not found: %r" % filename)
        pool.con = con = sqlite.connect(filename, isolation_level=None, **pool.kwargs)
        con.text_factory = _text_factory

        def create_function(name, num_params, func):
            func = keep_exception(func)
            con.create_function(name, num_params, func)

        create_function('power', 2, pow)
        create_function('rand', 0, random)
        create_function('py_upper', 1, py_upper)
        create_function('py_lower', 1, py_lower)
        create_function('py_json_unwrap', 1, py_json_unwrap)
        create_function('py_json_extract', -1, py_json_extract)
        create_function('py_json_contains', 3, py_json_contains)
        create_function('py_json_nonzero', 2, py_json_nonzero)
        create_function('py_json_array_length', -1, py_json_array_length)

        create_function('py_array_index', 2, py_array_index)
        create_function('py_array_contains', 2, py_array_contains)
        create_function('py_array_subset', 2, py_array_subset)
        create_function('py_array_length', 1, py_array_length)
        create_function('py_array_slice', 3, py_array_slice)
        create_function('py_make_array', -1, py_make_array)

        create_function('py_string_slice', 3, py_string_slice)

        if sqlite.sqlite_version_info >= (3, 6, 19):
            con.execute('PRAGMA foreign_keys = true')

        con.execute('PRAGMA case_sensitive_like = true')
    def disconnect(pool):
        if pool.filename != ':memory:':
            Pool.disconnect(pool)
    def drop(pool, con):
        if pool.filename != ':memory:':
            Pool.drop(pool, con)
        else:
            con.rollback()
