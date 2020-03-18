from __future__ import print_function
from pony.py23compat import basestring

import sys
from collections import OrderedDict
from pony.utils import throw
from pony.orm.migrations.virtuals import Required, Set, Optional, Discriminator
from pony.orm.dbapiprovider import Name, obsolete
from pony.orm.sqlbuilding import SQLBuilder
from pony.orm.core import MigrationError, MappingError, SchemaError


class DBObject(object):
    name = None


class SQLOperation(object):
    def __init__(self, obj, sql):
        self.obj = obj
        self.sql = sql

    def get_sql(self):
        return self.sql

    def __repr__(self):
        return '<SQLOperation(%r)>' % self.sql


def sql_op(func):
    def wrap(obj, *args, **kwargs):
        res = func(obj, *args, **kwargs)
        if isinstance(res, list):
            return [SQLOperation(obj, sql) for sql in res]
        return [SQLOperation(obj, res)]
    return wrap


def provided_name(obj):
    name = obj.name
    if isinstance(name, tuple):
        name = name[1]
    return not isinstance(name, Name)


class Table(DBObject):
    typename = 'Table'

    def __init__(self, schema, name, is_m2m=False):
        self.created = False
        self.schema = schema
        self.provider = schema.provider
        self.name = name
        self.columns = OrderedDict()
        self.foreign_keys = []
        self.keys = []
        self.primary_key = None
        self.indexes = []
        self.constraints = []
        self.is_m2m = is_m2m

        self.schema.tables[name] = self

    def __repr__(self):
        return self.name

    def __eq__(self, other):
        if type(self) != type(other):
            return False
        if self.name != other.name:
            return False
        cols = sorted(self.columns.values(), key=lambda col: col.name)
        other_cols = sorted(other.columns.values(), key=lambda col: col.name)
        if cols != other_cols:
            return False

        fks = sorted(self.foreign_keys, key=lambda fk: ' '.join(col.name for col in fk.cols_from))
        other_fks = sorted(other.foreign_keys, key=lambda fk: ' '.join(col.name for col in fk.cols_from))
        if fks != other_fks:
            return False

        if self.primary_key != other.primary_key:
            return False

        indexes = sorted(self.indexes, key=lambda index: index.name)
        other_indexes = sorted(other.indexes, key=lambda index: index.name)
        if indexes != other_indexes:
            return False

        if self.is_m2m != other.is_m2m:
            return False

        constraints = sorted(self.constraints, key=lambda c: c.name)
        other_constraints = sorted(other.constraints, key=lambda c: c.name)
        if constraints != other_constraints:
            return False

        return True

    def exists(table, provider, connection, case_sensitive=True):
        return provider.table_exists(connection, table.name, case_sensitive)

    def get_alter_prefix(self):
        quote = self.provider.quote_name
        return 'ALTER TABLE %s' % quote(self.name)

    @sql_op
    def get_drop_sql(self):
        return 'DROP TABLE ' + self.provider.quote_name(self.name)

    @sql_op
    def get_rename_sql(self, new_name):
        return '%s RENAME TO %s' % (self.get_alter_prefix(), self.provider.quote_name(new_name))

    def create(self):
        assert not self.created
        self.created = True
        return self.get_create_sql()

    def create_constraints(self):
        result = []
        for constraint in self.constraints:
            if not constraint.created:
                constraint.created = True
                result.extend(constraint.get_add_sql())
        return result

    def create_indexes(self):
        result = []
        for index in self.indexes:
            if not index.created:
                index.created = True
                result.extend(index.get_create_sql())
        return result

    def create_fkeys(self):
        result = []
        for fk in self.foreign_keys:
            if not fk.created:
                fk.created = True
                result.extend(fk.get_create_sql())
        return result

    @sql_op
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
        for key in self.keys:
            body.append(key.get_inline_sql(using_obsolete_names))
        if not self.schema.unique_cls.inline_syntax:
            for con in self.constraints:
                if isinstance(con, UniqueConstraint):
                    body.append(con.get_inline_sql(using_obsolete_names))
                    con.created = True
        body = ',\n  '.join(body)
        return header + body + '\n)'

    @sql_op
    def get_change_schema_sql(self, schema):
        quote = self.provider.quote_name
        result = [self.get_alter_prefix()]
        result.append('SET SCHEMA')
        result.append(quote(schema))
        return ' '.join(result)

    @classmethod
    def from_entity(cls, schema, entity):
        return schema.create_entity_table(entity)


class Column(DBObject):
    auto_template = '%(type)s PRIMARY KEY AUTOINCREMENT'

    def __init__(self, table, name, converter, sql_type=None):
        if name in table.columns:
            raise SchemaError('Column `%s` already exists in table `%s`' % (name, table.name))

        self.table = table
        self.name = name
        self.converter = converter
        self.provider = table.schema.provider
        self.is_pk = False
        self.nullable = False
        self.auto = False
        self.unique_constraint = None
        self.check_constraint = None
        self.sql_default = None
        self.sql_type = sql_type or converter.sql_type()
        self.initial = None
        self.m2m_cols_links = []
        table.columns[self.name] = self

    def __repr__(self):
        return '%s.%s' % (self.table.name, self.name)

    def __eq__(self, other):
        if type(self) != type(other):
            return False
        if self.name != other.name:
            return False
        if self.table.name != other.table.name:
            return False
        if self.is_pk != other.is_pk:
            return False
        if self.nullable != other.nullable:
            return False
        if self.auto != other.auto:
            return False
        if self.unique_constraint != other.unique_constraint:
            return False
        if self.check_constraint != other.check_constraint:
            return False
        if self.sql_default != other.sql_default:
            return False
        if self.sql_type != other.sql_type:
            return False
        return True

    def exists(self, provider, connection, case_sensitive=True):
        return provider.column_exists(connection, self.table.name, self.name, case_sensitive)

    @classmethod
    def from_attr(cls, table, attr):
        schema = table.schema
        if not attr.converters:
            attr.converters = [schema.provider.get_converter_by_attr(attr)]
        converter = attr.converters[0]
        col_name = schema.get_column_name(attr)
        col = cls(table, col_name, converter)
        col.is_pk = attr.is_pk
        if isinstance(attr, Optional) and attr.is_string and schema.provider.dialect == 'Oracle':
            if attr.provided.kwargs.get('nullable') is False:
                throw(MappingError, 'In Oracle, optional string attribute %s must be nullable' % attr)
            else:
                attr.nullable = True
        col.nullable = attr.nullable or len(attr.entity.bases) != 0
        col.auto = attr.auto
        col.sql_default = attr.sql_default
        col.initial = attr.initial or attr.provided.initial
        col.sql_type = attr.sql_type or converter.get_sql_type()
        provided_cols = attr.provided.kwargs.get('columns', None)
        if provided_cols:
            throw(MappingError, "Too many columns were specified for %r" % attr)
        return col

    def get_inline_sql(self, using_obsolete_names=False, ignore_pk=False, without_name=False):
        quote = self.provider.quote_name
        obs_if = lambda n: obsolete(n) if using_obsolete_names else n
        schema = self.table.schema
        if without_name:
            result = []
        else:
            result = [quote(obs_if(self.name))]
        if self.is_pk and not ignore_pk:
            if self.auto and self.auto_template:
                result.append(self.auto_template % dict(type=self.converter.sql_type()))
            else:
                result.append(self.sql_type)
                result.append('PRIMARY KEY')
        else:
            result.append(self.sql_type)
        unq = self.unique_constraint
        if unq and len(unq.cols) == 1 and not unq.created and unq.inline_syntax:
            unq.created = True
            result.append(unq.get_inline_sql(inside_column=True))
        if self.initial is not None or self.sql_default is not None:
            if self.initial is not None:
                val = self.initial
            else:
                val = self.sql_default
            result.append('DEFAULT %r' % val)

        if schema.inline_reference:
            for fk in self.table.foreign_keys:
                if len(fk.cols_from) == 1 and fk.cols_from[0].name == self.name:
                    result.append(fk.get_inline_sql(using_obsolete_names, inside_column=True))
        # SQLite bug: PK might be null
        if not self.nullable and (not self.is_pk or self.provider.dialect == 'SQLite') and \
                not self.unique_constraint:
            result.append('NOT NULL')

        return ' '.join(result)

    def get_alter_prefix(self):
        return 'ALTER COLUMN %s' % self.provider.quote_name(self.name)

    @sql_op
    def get_add_sql(self):
        result = [self.table.get_alter_prefix()]
        result.append('ADD COLUMN')
        result.append(self.get_inline_sql(ignore_pk=True))
        return ' '.join(result)

    @sql_op
    def get_drop_sql(self):
        result = [self.table.get_alter_prefix()]
        result.append('DROP COLUMN')
        result.append(self.provider.quote_name(self.name))
        return ' '.join(result)

    @sql_op
    def get_drop_default_sql(self):
        # assert self.initial is not None
        result = [self.table.get_alter_prefix()]
        result.append(self.get_alter_prefix())
        result.append('DROP DEFAULT')
        return ' '.join(result)

    @sql_op
    def get_set_default_sql(self):
        assert self.sql_default
        result = [self.table.get_alter_prefix()]
        result.append(self.get_alter_prefix())
        result.append('SET DEFAULT')
        builder = SQLBuilder(self.provider, ['VALUE', self.sql_default])
        sql_default = builder.sql
        result.append(sql_default)
        return ' '.join(result)

    @sql_op
    def get_rename_sql(self, new_name):
        quote = self.provider.quote_name
        result = [self.table.get_alter_prefix()]
        result.append('RENAME COLUMN')
        result.append(quote(self.name))
        result.append('TO')
        result.append(quote(new_name))
        return ' '.join(result)

    @sql_op
    def get_update_value_sql(self, old_value, new_value):
        quote = self.provider.quote_name
        py2sql = self.converter.py2sql
        old_value = py2sql(old_value)
        new_value = py2sql(new_value)
        builder = SQLBuilder(self.provider, ['VALUE', old_value])
        old_value = builder.sql
        builder2 = SQLBuilder(self.provider, ['VALUE', new_value])
        new_value = builder2.sql
        result = ['UPDATE']
        result.append(quote(self.table.name))
        result.append('SET')
        result.append(quote(self.name))
        result.append('=')
        result.append(new_value)
        result.append('WHERE')
        result.append(quote(self.name))
        result.append('=')
        result.append(old_value)
        return ' '.join(result)

    @sql_op
    def get_change_type_sql(self, new_type, cast):
        quote = self.provider.quote_name
        result = [self.table.get_alter_prefix()]
        result.append(self.get_alter_prefix())
        result.append('TYPE')
        result.append(new_type)
        result.append('USING')
        if cast:
            result.append(cast.format(colname=quote(self.name), sql_type=new_type))
        return ' '.join(result)

    @sql_op
    def get_drop_not_null_sql(self):
        result = [self.table.get_alter_prefix()]
        result.append(self.get_alter_prefix())
        result.append('DROP NOT NULL')
        return ' '.join(result)

    @sql_op
    def get_set_not_null_sql(self):
        result = [self.table.get_alter_prefix()]
        result.append(self.get_alter_prefix())
        result.append('SET NOT NULL')
        return ' '.join(result)


class ForeignKey(DBObject):
    typename = 'Foreign key'

    def __init__(self, table, table_to, cols_from, cols_to, name):
        self.created = False
        self.table = table
        self.provider = self.table.schema.provider
        self.table_to = table_to
        self.cols_from = cols_from
        self.cols_to = cols_to
        self.name = name
        self.on_delete = None
        self.table.foreign_keys.append(self)

    def __eq__(self, other):
        if type(self) != type(other):
            return False
        if self.name != other.name:
            return False
        if self.table.name != other.table.name:  # comparing objects will lead to recursion
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

    def exists(foreign_key, provider, connection, case_sensitive=True):
        return provider.fk_exists(connection, foreign_key.table.name, foreign_key.name, case_sensitive)

    @sql_op
    def get_create_sql(self, using_obsolete_names=False):
        name = self.name[1] if isinstance(self.name, tuple) else self.name
        quote = self.provider.quote_name
        obs_if = lambda n: obsolete(n) if using_obsolete_names else n
        result = []
        result.append('ALTER TABLE')
        result.append(quote(obs_if(self.table.name)))
        result.append('ADD CONSTRAINT')
        result.append(quote(obs_if(name)))
        result.append('FOREIGN KEY')
        result.append('(%s)' % (', '.join(quote(obs_if(col.name)) for col in self.cols_from)))
        result.append('REFERENCES')
        result.append(quote(obs_if(self.table_to.name)))
        result.append('(%s)' % (', '.join(quote(obs_if(col.name)) for col in self.cols_to)))
        if self.on_delete:
            result.append('ON DELETE')
            result.append(self.on_delete)
        return ' '.join(result)

    @sql_op
    def get_drop_sql(self):
        quote = self.provider.quote_name
        result = [self.table.get_alter_prefix()]
        result.append('DROP CONSTRAINT')
        result.append(quote(self.name))
        return ' '.join(result)

    @sql_op
    def get_rename_sql(self, new_name):
        quote = self.provider.quote_name
        result = [self.table.get_alter_prefix()]
        result.append('RENAME CONSTRAINT')
        result.append(quote(self.name))
        result.append('TO')
        result.append(quote(new_name))
        return ' '.join(result)

    def get_inline_sql(self, using_obsolete_names=False, inside_column=False):
        raise NotImplementedError


class Constraint(DBObject):
    typename = 'Constraint'

    def __init__(self, table):
        self.table = table
        self.provider = table.provider

    @sql_op
    def get_drop_sql(self):
        quote = self.provider.quote_name
        result = [self.table.get_alter_prefix()]
        result.append('DROP CONSTRAINT')
        result.append(quote(self.name))
        return ' '.join(result)

    def get_add_sql(self):
        raise NotImplementedError

    @sql_op
    def get_create_sql(self, using_obsolete_names=False):
        # todo: pass using_obsolete_names to get_add_sql
        return self.get_add_sql()

    @sql_op
    def get_rename_sql(self, new_name):
        quote = self.provider.quote_name
        result = [self.table.get_alter_prefix()]
        result.append('RENAME CONSTRAINT')
        result.append(quote(self.name))
        result.append('TO')
        result.append(quote(new_name))
        return ' '.join(result)


class Key(Constraint):
    def __init__(self, table, cols, is_pk=False):
        Constraint.__init__(self, table)
        self.created = False
        self.cols = cols
        self.is_pk = is_pk
        self.is_unique = True
        if self.is_pk:
            self.table.primary_key = self
            self.name = None
        else:
            self.table.keys.append(self)
            self.name = table.schema.get_default_key_name(self.table, self.cols)

    def __eq__(self, other):
        if type(self) != type(other):
            return False
        if self.name != other.name:
            return False
        if self.table.name != other.table.name:
            return False
        self_cols = sorted(self.cols, key=lambda col: col.name)
        other_cols = sorted(other.cols, key=lambda col: col.name)
        if self_cols != other_cols:
            return False
        if self.is_pk != other.is_pk:
            return False
        if self.is_unique != other.is_unique:
            return False
        return True

    def get_pk_inline_sql(self, using_obsolete_names=False):
        quote = self.provider.quote_name
        result = ['PRIMARY KEY']
        if using_obsolete_names:
            result.append('(%s)' % (', '.join(quote(obsolete(col.name)) for col in self.cols)))
        else:
            result.append('(%s)' % (', '.join(quote(col.name) for col in self.cols)))
        return ' '.join(result)

    def get_key_inline_sql(self, using_obsolete_names=False):
        quote = self.provider.quote_name
        obs_if = lambda n: obsolete(n) if using_obsolete_names else n
        result = ['CONSTRAINT']
        name = obs_if(self.name)
        if isinstance(name, tuple):
            result.append(quote(name[1]))
        else:
            result.append(quote(name))
        result.append('UNIQUE')
        result.append('(%s)' % ', '.join(quote(obs_if(col.name)) for col in self.cols))
        return ' '.join(result)

    @sql_op
    def get_key_add_sql(self):
        quote = self.provider.quote_name
        result = [self.table.get_alter_prefix()]
        result.append('ADD CONSTRAINT')
        result.append(quote(self.name))
        result.append('UNIQUE')
        result.append('(%s)' % ', '.join(quote(col.name) for col in self.cols))
        return ' '.join(result)

    def get_inline_sql(self, using_obsolete_names=False, inside_column=False):
        if self.is_pk:
            return self.get_pk_inline_sql(using_obsolete_names)
        return self.get_key_inline_sql(using_obsolete_names)


class Index(DBObject):
    typename = 'Index'

    def __init__(self, table, cols, index_name):
        self.created = False
        self.table = table
        self.cols = cols
        self.is_unique = False
        self.is_pk = False
        self.name = index_name
        self.provider = self.table.schema.provider
        if self not in self.table.indexes:
            self.table.indexes.append(self)

    def __eq__(self, other):
        if type(self) != type(other):
            return False
        if self.name != other.name:
            return False
        if self.table.name != other.table.name:
            return False
        if self.cols != other.cols:
            return False
        if self.is_unique != other.is_unique:
            return False
        return True

    def exists(index, provider, connection, case_sensitive=True):
        return provider.index_exists(connection, index.table.name, index.name, case_sensitive)

    @sql_op
    def get_create_sql(self, using_obsolete_names=False):
        quote = self.provider.quote_name
        name = self.name[1] if isinstance(self.name, tuple) else self.name
        obs_if = lambda n: obsolete(n) if using_obsolete_names else n
        result = ['CREATE INDEX']
        result.append(quote(obs_if(name)))
        result.append('ON')
        result.append(quote(obs_if(self.table.name)))
        result.append('(%s)' % ', '.join(quote(obs_if(col.name)) for col in self.cols))
        return ' '.join(result)

    @sql_op
    def get_drop_sql(self, using_obsolete_names=False):
        quote = self.provider.quote_name
        obs_if = lambda n: obsolete(n) if using_obsolete_names else n
        return 'DROP INDEX ' + quote(obs_if(self.name))

    @sql_op
    def get_rename_sql(self, new_name):
        quote = self.provider.quote_name
        result = ['ALTER INDEX']
        result.append(quote(self.name))
        result.append('RENAME TO')
        result.append(quote(new_name))
        return ' '.join(result)


class UniqueConstraint(Constraint):
    typename = 'Unique constraint'
    inline_syntax = True

    def __init__(self, cols):
        table = cols[0].table
        Constraint.__init__(self, table)
        self.cols = cols
        for col in cols:
            col.unique_constraint = self
        self.table.constraints.append(self)
        self.name = self.table.schema.get_default_key_name(self.table, cols)
        self.created = False

    def __eq__(self, other):
        if type(self) != type(other):
            return False
        if self.table.name != other.table.name:
            return False
        if self.name != other.name:
            return False
        return True

    def get_inline_sql(self, using_obsolete_names=False, inside_column=False):
        quote = self.provider.quote_name
        result = ['CONSTRAINT']
        if not isinstance(self.name, tuple):
            result.append(quote(self.name))
        else:
            result.append(quote(self.name[1]))
        result.append('UNIQUE')
        if not inside_column:
            result.append('(%s)' % ', '.join(quote(col.name) for col in self.cols))
        return ' '.join(result)

    @sql_op
    def get_add_sql(self):
        quote = self.provider.quote_name
        result = [self.table.get_alter_prefix()]
        result.append('ADD CONSTRAINT')
        result.append(quote(self.name))
        result.append('UNIQUE')
        result.append('(%s)' % ', '.join(quote(col.name) for col in self.cols))
        return ' '.join(result)

    def exists(self, provider, connection, case_sensitive=True):
        return provider.unq_exists(connection, self.table.name, self.name, case_sensitive)


class CheckConstraint(Constraint):
    typename = 'Check constraint'

    def __init__(self, col, check):
        Constraint.__init__(self, col.table)
        self.col = col
        assert col.check_constraint is None
        col.check_constraint = self
        col.table.constraints.append(self)
        self.check = check
        self.name = self.table.schema.get_default_check_name(self.table, self.col)
        self.created = False

    def __eq__(self, other):
        if type(self) != type(other):
            return False
        if self.name != other.name:
            return False
        if self.check != other.check:
            return False
        return True

    def exists(self, provider, connection, case_sensitive=True):
        return provider.chk_exists(connection, self.table.name, self.name, case_sensitive)

    def get_inline_sql(self, using_obsolete_names=False, inside_column=False):
        quote = self.provider.quote_name
        result = ['CONSTRAINT']
        if not isinstance(self.name, tuple):
            result.append(quote(self.name))
        else:
            result.append(quote(self.name[1]))
        result.append('CHECK')
        result.append('(%s)' % self.check)
        return ' '.join(result)

    @sql_op
    def get_add_sql(self):
        quote = self.provider.quote_name
        result = [self.table.get_alter_prefix()]
        result.append('ADD CONSTRAINT')
        result.append(quote(self.name))
        result.append('CHECK')
        result.append('(%s)' % self.check)
        return ' '.join(result)


class Schema(object):
    dialect = None
    table_cls = Table
    column_cls = Column
    key_cls = Key
    fk_cls = ForeignKey
    index_cls = Index
    unique_cls = UniqueConstraint
    check_cls = CheckConstraint
    inline_reference = False
    command_separator = '\n\n'

    def __init__(self, vdb, provider):
        self.vdb = vdb
        self.obsolete = False
        self.provider = provider
        self.tables = OrderedDict()
        self.m2m_to_create = []
        self.tables_to_create = []
        self.attrs_to_create = {}
        self.ops = []
        self.warnings = []
        self.errors = 0

        vdb.schema = self

    def __eq__(self, other):
        self_tables = sorted(self.tables.values(), key=lambda table: table.name)
        other_tables = sorted(other.tables.values(), key=lambda table: table.name)
        if self_tables != other_tables:
            return False
        return True

    def find_subordinate_index(schema, col):
        result = []
        for table in schema.tables.values():
            for index in table.indexes:
                if col in index.cols:
                    result.append(index)
        return result

    def create_migration_table_sql(self):
        query = 'create table "migration"' \
                '("name" text primary key, "applied" timestamp not null)'
        return query

    def get_applied_sql(self):
        query = 'select "name" from "migration"'
        return query

    def get_migration_insert_sql(self):
        query = 'insert into "migration"("name", "applied") values(%(name)s, %(applied)s)'
        return query

    @sql_op
    def get_update_move_column_sql(self, table_to, table_to_pk, cols_to, table_from, table_from_pk, cols_from):
        quote = self.provider.quote_name
        result = ['UPDATE %s' % quote(table_to.name)]
        set_section = ',\n    '.join('%s = %s.%s' %
             (quote(col_to.name), quote(table_from.name), quote(pk_col.name)) for col_to, pk_col in
             zip(cols_to, table_from_pk)
        )
        result.append('SET %s' % set_section)
        result.append('FROM %s' % quote(table_from.name))
        where_section = ' AND \n      '.join('%s.%s = %s.%s' %
            (quote(table_from.name), quote(col_from.name), quote(table_to.name), quote(pk_col.name))
            for col_from, pk_col in zip(cols_from, table_to_pk)
        )
        result.append('WHERE %s' % where_section)
        return '\n'.join(result)

    def add_sql(schema, sql):
        if not isinstance(sql, (list, tuple)):
            throw(MigrationError, 'sql option should be a type of list')
        for op in sql:
            schema.ops.append(SQLOperation(None, op))

    def create_entity_table(schema, entity):
        table_name = schema.get_table_name(entity)
        if table_name in schema.tables:
            return schema.tables[table_name]
        table = schema.table_cls(schema, table_name)

        all_attrs = entity.all_attrs[:]
        pk_attrs = []
        for attr in all_attrs[:]:
            if attr.name in entity.primary_key:
                pk_attrs.append(attr)
                all_attrs.remove(attr)

        cols = []
        for attr in pk_attrs:
            schema.make_column(attr, table)
            cols.extend(attr.columns)

        schema.key_cls(table, cols, is_pk=True)

        for attr in all_attrs:
            if attr.reverse and (isinstance(attr.reverse, basestring) or attr.reverse.entity is None):
                schema.attrs_to_create[attr] = table
            else:
                schema.make_column(attr, table)

        for ck in entity.composite_keys:
            cols = []
            for attrname in ck:
                attr = entity.get_attr(attrname)
                cols.extend(attr.columns)
            schema.key_cls(table, cols)

        for ci in entity.composite_indexes:
            cols = []
            for attrname in ci:
                attr = entity.get_attr(attrname)
                cols.extend(attr.columns)
            index_name = schema.get_default_index_name(table, cols)
            schema.index_cls(table, cols, index_name)

        entity.table = table
        return table

    def get_fk(schema, columns):
        table = columns[0].table
        for fk in table.foreign_keys:
            if fk.cols_from == columns:
                return fk

    def get_index(schema, columns):
        table = columns[0].table
        for index in table.indexes:
            if index.cols == columns:
                return index

    def get_key(schema, columns):
        table = columns[0].table
        for key in table.keys:
            if not key.is_pk and key.cols == columns:
                return key

    # migrations methods

    def add_columns(schema, columns):
        for column in columns:
            schema.ops.extend(column.get_add_sql())

    def drop_initial(schema, columns):
        for col in columns:
            schema.ops.extend(col.get_drop_default_sql())
            col.initial = None
            if col.sql_default:
                schema.ops.extend(col.get_set_default_sql())

    def add_fk(schema, fk):
        schema.ops.extend(fk.get_create_sql())

    def add_index(schema, index):
        schema.ops.extend(index.get_create_sql())

    def drop_columns(schema, columns):
        for column in columns:
            col_name = column.name
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
            table.indexes = new_indexes_list
            schema.ops.extend(column.get_drop_sql())
            table.columns.pop(col_name)

    def drop_table(schema, table):
        schema.tables.pop(table.name)
        schema.ops.extend(table.get_drop_sql())

    def rename_table(schema, table, new_name, ignore_indexes=False):
        del schema.tables[table.name]
        new_table_name = new_name if isinstance(new_name, basestring) else new_name[1]
        schema.ops.extend(table.get_rename_sql(new_table_name))
        table.name = new_name
        schema.tables[new_name] = table
        for fk in table.foreign_keys:
            if provided_name(fk):
                continue  # name was provided
            fk_new_name = schema.get_default_fk_name(table, fk.cols_from)
            schema.rename_foreign_key(fk, fk_new_name)
        if not ignore_indexes:  # Upgrade case: m2m tables for 0.7 have wrong indexes, we update them manually
            for index in table.indexes:
                if provided_name(index):
                    continue
                index_new_name = schema.get_default_index_name(table, index.cols)
                schema.rename_index(index, index_new_name)
        for col in table.columns.values():
            unq = col.unique_constraint
            if unq:
                new_name = schema.get_default_key_name(table, [col])
                if new_name != unq.name:
                    schema.rename_key(unq, new_name)
            chk = col.check_constraint
            if chk:
                new_name = schema.get_default_check_name(table, col)
                if new_name != chk.name:
                    schema.rename_constraint(chk, new_name)

    def rename_constraint(schema, obj, new_name):
        if obj.name == new_name:
            return
        schema.ops.extend(obj.get_rename_sql(new_name))
        obj.name = new_name

    def rename_key(schema, key, new_name):
        schema.rename_constraint(key, new_name)

    def update_col_value(schema, col, old_value, new_value):
        schema.ops.extend(col.get_update_value_sql(old_value, new_value))

    def change_schema(schema, table, new_schema):
        def add_schema(obj):
            obj.name = (new_schema, obj.name)

        def update_schema(obj):
            obj.name = (new_schema, obj.name[1])

        def remove_schema(obj):
            obj.name = obj.name[1]

        if isinstance(table.name, tuple):
            if new_schema is None:
                func = remove_schema
            else:
                func = update_schema
        else:
            assert new_schema is not None
            func = add_schema

        for fk in table.foreign_keys:
            func(fk)

        for index in table.indexes:
            func(index)

        for key in table.keys:
            func(key)

        for cn in table.constraints:
            func(cn)

        if new_schema is None:
            new_schema = schema.provider.default_schema_name
        schema.ops.extend(table.get_change_schema_sql(new_schema))
        schema.tables.pop(table.name)
        func(table)
        schema.tables[table.name] = table

    def rename_column(schema, column, new_name, ignore_indexes=False):
        return schema.rename_columns([column], [new_name], ignore_indexes)

    def rename_columns(schema, columns, new_names, ignore_indexes=False):
        table = columns[0].table
        for i, new_name in enumerate(new_names):
            col = columns[i]
            if new_name != col.name:
                schema.ops.extend(col.get_rename_sql(new_name))
                table.columns.pop(col.name)
                col.name = new_name
                table.columns[col.name] = col

        table = columns[0].table
        for fk in table.foreign_keys:
            if provided_name(fk):
                continue
            new_fk_name = schema.get_default_fk_name(table, fk.cols_from)
            schema.rename_foreign_key(fk, new_fk_name)

        if not ignore_indexes:
            for index in table.indexes:
                if provided_name(index):
                    continue
                new_index_name = schema.get_default_index_name(table, index.cols)
                schema.rename_index(index, new_index_name)

        for col in columns:
            unq = col.unique_constraint
            if unq:
                new_name = schema.get_default_key_name(table, col)
                if new_name != unq.name:
                    schema.rename_key(unq, new_name)
            chk = col.check_constraint
            if chk:
                new_name = schema.get_default_check_name(table, col)
                if new_name != chk.name:
                    schema.rename_constraint(chk, new_name)

    def change_discriminator_value(schema, column, old_value, new_value):
        schema.ops.extend(column.get_update_value_sql(old_value, new_value))

    def change_attribute_class(schema, *args):
        throw(NotImplementedError)

    def add_composite_key(schema, table, cols):
        key = schema.key_cls(table, cols)
        schema.ops.extend(key.get_key_add_sql())

    def drop_composite_key(schema, table, columns):
        for key in table.keys:
            if not key.is_pk:
                if key.cols == columns:
                    break
        else:
            raise MigrationError

        schema.ops.extend(key.get_drop_sql())
        table.keys.remove(key)

    def rename_columns_by_attr(schema, attr, new_name):
        # is being used by RenameAttribute operation
        if 'column' in attr.provided.kwargs or 'columns' in attr.provided.kwargs:
            return
        entity = attr.entity
        if attr.reverse:
            resolved_pk = schema.resolve_pk(attr.reverse.entity, attr)
        for i, column in enumerate(attr.columns):
            if attr.reverse:
                new_column_name = resolved_pk[i][0]
            else:
                assert i == 0
                new_column_name = schema.get_default_column_name(new_name)
            if column.name == new_column_name:
                continue
            schema.ops.extend(column.get_rename_sql(new_column_name))
            column.name = new_column_name

            # foreign keys and indexes handling
            table = column.table
            assert table
            for fk in table.foreign_keys:
                if provided_name(fk):
                    continue
                if any(col in attr.columns for col in fk.cols_from):
                    fk_new_name = schema.get_default_fk_name(table, fk.cols_from)
                    schema.rename_foreign_key(fk, fk_new_name)
            for index in table.indexes:
                if provided_name(index):
                    continue
                if any(col in attr.columns for col in index.cols):
                    index_new_name = schema.get_default_index_name(table, index.cols)
                    schema.rename_index(index, index_new_name)
            for col in table.columns.values():
                unq = col.unique_constraint
                if unq:
                    new_name = schema.get_default_key_name(table, col)
                    if new_name != unq.name:
                        schema.rename_key(unq, new_name)
                chk = col.check_constraint
                if chk:
                    new_name = schema.get_default_check_name(table, col)
                    if new_name != chk.name:
                        schema.rename_constraint(chk, new_name)

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

        schema.ops.extend(fk.get_drop_sql())
        table_from.foreign_keys.remove(fk)
        schema.ops.extend(index.get_drop_sql())
        table_from.indexes.remove(index)

        for col in cols_from:
            schema.ops.extend(col.get_drop_sql())
            table_from.columns.pop(col.name)

        new_fk_name = schema.get_fk_name(attr, table_to, cols_to)
        new_fk = schema.fk_cls(table_to, table_from, cols_to, table_from_pk, new_fk_name)
        if attr.reverse.cascade_delete:
            new_fk.on_delete = 'CASCADE'
        elif isinstance(attr, Optional) and attr.nullable:
            new_fk.on_delete = 'SET NULL'

        index_name = schema.get_index_name(attr, table_to, cols_to)
        new_index = schema.index_cls(table_to, cols_to, index_name)

        schema.ops.extend(new_fk.get_create_sql())
        schema.ops.extend(new_index.get_create_sql())

    def change_column_type(schema, column, new_sql_type, cast):
        if column.sql_type != new_sql_type:
            column.sql_type = new_sql_type
            schema.ops.extend(column.get_change_type_sql(new_sql_type, cast))

    def change_sql_default(schema, column, new_sql_default):
        column.sql_default = new_sql_default
        if new_sql_default is None:
            schema.ops.extend(column.get_drop_default_sql())
        else:
            schema.ops.extend(column.get_set_default_sql())

    def change_nullable(schema, column, new_value):
        old_value = column.nullable
        if old_value == new_value:
            return
        column.nullable = new_value
        if new_value:
            schema.ops.extend(column.get_drop_not_null_sql())
        else:
            schema.ops.extend(column.get_set_not_null_sql())

    def add_unique_constraint(schema, cols):
        unq = schema.unique_cls(cols)
        schema.ops.extend(unq.get_add_sql())

    def drop_unique_constraint(schema, cols):
        table = cols[0].table
        unq = cols[0].unique_constraint
        assert unq in table.constraints
        for col in cols:
            col.unique_constraint = None
        table.constraints.remove(unq)
        schema.ops.extend(unq.get_drop_sql())

    def add_check_constraint(schema, col, check):
        if col.check_constraint:
            raise MigrationError
        chk = schema.check_cls(col, check)
        schema.ops.extend(chk.get_add_sql())

    def drop_check_constraint(schema, col):
        table = col.table
        chk = col.check_constraint
        if chk is None:
            raise MigrationError
        assert chk in table.constraints
        table.constraints.remove(chk)
        col.check_constraint = None
        schema.ops.extend(chk.get_drop_sql())

    def rename_index(schema, index, new_name):
        table = index.table
        cols = index.cols
        if new_name is True:
            new_index_name = schema.get_default_index_name(table, cols)
            if new_index_name != index.name:
                schema.ops.extend(index.get_rename_sql(new_index_name))
                index.name = new_index_name
        else:
            if new_name != index.name:
                schema.ops.extend(index.get_rename_sql(new_name))
                index.name = new_name

    def drop_index(schema, index):
        table = index.table
        table.indexes.remove(index)
        schema.ops.extend(index.get_drop_sql())

    def rename_foreign_key(schema, fk, new_fk_name):
        if new_fk_name is None:
            new_fk_name = schema.get_default_fk_name(fk.table, fk.cols_from)

        if fk.name != new_fk_name:
            schema.ops.extend(fk.get_rename_sql(new_fk_name))
            fk.name = new_fk_name

    def drop_fk(schema, fk):
        table = fk.table
        table.foreign_keys.remove(fk)
        schema.ops.extend(fk.get_drop_sql())

    # not sql_op
    def get_create_sql(self, using_obsolete_names=False):
        return '\n\n'.join(self.get_create_sql_commands(using_obsolete_names))

    def get_objects_to_create(self):
        result = []
        for table in self.tables.values():
            result.append(table)
            for index in table.indexes:
                result.append(index)
            for con in table.constraints:
                if (isinstance(con, UniqueConstraint) and (len(con.cols) > 1 or not con.inline_syntax)) or \
                        not isinstance(con, UniqueConstraint) and not isinstance(con, CheckConstraint):
                    result.append(con)
            seq = getattr(table, 'pk_sequence', None)
            if seq:
                result.append(seq)
            trigger = getattr(table, 'pk_trigger', None)
            if trigger:
                result.append(trigger)
        if not self.inline_reference:
            for table in self.tables.values():
                for fk in table.foreign_keys:
                    result.append(fk)
                for chk in table.constraints:
                    if isinstance(chk, CheckConstraint):
                        result.append(chk)
        return result

    def get_create_sql_commands(self, using_obsolete_names=False):
        result = []
        for obj in self.get_objects_to_create():
            result.extend([op.sql for op in obj.get_create_sql(using_obsolete_names)])
        return result

    def create_tables(self, connection):
        from pony.orm import core
        provider = self.provider
        cursor = connection.cursor()
        for obj in self.get_objects_to_create():
            base_name = provider.base_name(obj.name)
            name = obj.exists(provider, connection, case_sensitive=False)
            if name is None:
                for op in obj.get_create_sql():
                    if core.local.debug: core.log_sql(op.sql)
                    provider.execute(cursor, op.sql)
            elif name != base_name:
                quote_name = provider.quote_name
                n1, n2 = quote_name(obj.name), quote_name(name)
                tn1 = obj.typename
                throw(SchemaError, '%s %s cannot be created, because %s ' \
                                   '(with a different letter case) already exists in the database. ' \
                                   'Try to delete %s first.' % (tn1, n1, n2, n2))

    def check_tables(self, connection):
        from pony.orm import core
        provider = self.provider
        cursor = connection.cursor()
        split = provider.split_table_name
        for table in sorted(self.tables.values(), key=lambda table: split(table.name)):
            alias = provider.base_name(table.name)
            sql_ast = [ 'SELECT',
                        [ 'ALL', ] + [ [ 'COLUMN', alias, column.name ] for column in table.columns.values() ],
                        [ 'FROM', [ alias, 'TABLE', table.name ] ],
                        [ 'WHERE', [ 'EQ', [ 'VALUE', 0 ], [ 'VALUE', 1 ] ] ]
                      ]
            sql, adapter = provider.ast2sql(sql_ast)
            if core.local.debug: core.log_sql(sql)
            provider.execute(cursor, sql)

    @classmethod
    def from_vdb(cls, vdb, provider, obsolete=False):
        # vdb.init()
        schema = cls(vdb, provider)
        schema.obsolete = obsolete
        for entity in vdb.entities.values():
            # if entity.table: continue
            if entity.bases: continue
            table = cls.table_cls.from_entity(schema, entity)
            entity.table = table
            table.created = True
        for attr1, attr2 in schema.m2m_to_create:
            table = schema.create_m2m_table(attr1, attr2)
            table.created = True
        schema.m2m_to_create[:] = []
        return schema

    def create_m2m_table(self, attr1, attr2):
        name = self.get_m2m_table_name(attr1, attr2)
        if name in self.tables:
            table = self.tables[name]
            if table.is_m2m:
                return table
            throw(MappingError, 'Table name "%s" is already in use' % name)
            return self.tables[name]  # table was already created
        table = self.table_cls(self, name, is_m2m=True)
        attr1.m2m_table = attr2.m2m_table = table
        pk_cols = []

        def make_m2m_columns(table, attr, same_entity=False):
            fk_cols_from, fk_cols_to = [], []
            pk_cols = []
            symmetric = attr.reverse is attr and same_entity  # only last columns
            entity = attr.entity
            table_from = self.tables[self.get_table_name(attr.reverse.entity.get_root())]
            if len(table_from.primary_key.cols) == 1:
                kwarg = 'reverse_column' if symmetric else 'column'
                name = attr.provided.kwargs.get(kwarg)
                provided_col_names = [name] if name else []
            else:
                kwarg = 'reverse_columns' if symmetric else 'columns'
                provided_col_names = attr.provided.kwargs.get(kwarg) or []

            if provided_col_names:
                if len(provided_col_names) != len(table_from.primary_key.cols):
                    throw(MappingError, 'Invalid number of columns for %s.%s' % (entity.name, attr.name))
            elif same_entity:
                provided_col_names = [col.name + '_2' for col in attr.m2m_columns]

            for i, col in enumerate(table_from.primary_key.cols):
                if provided_col_names:
                    col_name = provided_col_names[i]
                else:
                    col_name = self.get_default_m2m_column_name(attr.reverse.entity.name, col.name,
                            composite_pk=len(table_from.primary_key.cols) > 1)

                fk_cols_to.append(col)
                converter = col.converter.make_fk_converter(attr)
                column = self.column_cls(table, col_name, converter, attr.sql_type)
                fk_cols_from.append(column)
                pk_cols.append(column)

            fk_name = table.schema.get_fk_name(attr, table, fk_cols_from)
            fk = table.schema.fk_cls(table, table_from, fk_cols_from, fk_cols_to, fk_name)
            fk.on_delete = 'CASCADE'
            if attr.index or attr.reverse_index or not self.provider.implicit_fk_indexes:
                index_name = self.get_index_name(
                    attr,
                    table,
                    fk_cols_from,
                    symmetric=(attr.reverse == attr and same_entity)
                )
                new_index = self.index_cls(table, fk_cols_from, index_name)
            return pk_cols

        def add_m2m_cols(table, attr, pk_cols, same_entity=False):
            # linking primary columns with links from m2m table
            cols = make_m2m_columns(table, attr.reverse, same_entity)
            for i, attrname in enumerate(attr.entity.primary_key):
                pk_attr = attr.entity.get_attr(attrname)
                for col in pk_attr.columns:
                    col.m2m_cols_links.append(cols[i])
            if attr.reverse is not attr:
                attr.m2m_columns.extend(cols)
                attr.converters.extend(col.converter for col in cols)
            else:
                if not same_entity:
                    attr.m2m_columns = cols
                else:
                    attr.reverse_m2m_columns = cols
            pk_cols.extend(cols)

        if attr1.entity.name > attr2.entity.name:
            attr1, attr2 = attr2, attr1
        add_m2m_cols(table, attr1, pk_cols)
        add_m2m_cols(table, attr2, pk_cols, same_entity=attr1.entity == attr2.entity)
        # self_reference flag should be specified only for second part of self referenced link
        self.key_cls(table, pk_cols, is_pk=True)
        return table

    def get_default_m2m_table_name(self, attr1, attr2):
        schema_name = None
        table1_name = attr1.entity.table_name
        table2_name = attr2.entity.table_name
        if isinstance(table1_name, tuple) and isinstance(table2_name, tuple):
            schema1 = table1_name[0]
            schema2 = table2_name[0]
            if schema1 != schema2:
                throw(NameError, 'Since %r and %r has different schemas you should provide schema'
                                 ' for %s.%s <-> %s.%s intermediate m2m table' %
                      (attr1.entity, attr2.entity, attr1.entity.name, attr1.name, attr2.entity.name, attr2.name))
            else:
                schema_name = schema1
        elif isinstance(table1_name, tuple) or isinstance(table2_name, tuple):
            throw(NameError, 'Since %r and %r has different schemas you should provide schema'
                             ' for %s.%s <-> %s.%s intermediate m2m table' %
                  (attr1.entity, attr2.entity, attr1.entity.name, attr1.name, attr2.entity.name, attr2.name))

        e1_name, e2_name = attr1.entity.name, attr2.entity.name
        if e1_name < e2_name:
            name = '%s_%s' % (e1_name.lower(), attr1.name)
        else:
            name = '%s_%s' % (e2_name.lower(), attr2.name)

        if attr1.symmetric:
            obsolete_name = attr1.entity.name.lower() + '_' + attr1.name
        else:
            obsolete_name = "%s_%s" % (min(e1_name, e2_name).lower(), max(e1_name, e2_name).lower())

        name = self.provider.normalize_name(Name(name, obsolete_name=obsolete_name))
        if schema_name:
            return schema_name, name
        return name

    def get_m2m_table_name(self, attr1, attr2):
        return attr1.m2m_table_name or attr2.m2m_table_name or self.get_default_m2m_table_name(attr1, attr2)

    def get_table_name(self, entity):
        return entity.table_name or self.get_default_table_name(entity.name)

    def get_default_table_name(self, e_name):
        return self.provider.normalize_name(e_name.lower())

    def get_column_name(self, attr, ignore_generated=False):
        if attr.columns and not ignore_generated:
            return attr.columns[0].name
        return attr.provided.kwargs.get('column') or self.get_default_column_name(attr.name)

    def get_default_column_name(self, a_name):
        return self.provider.normalize_name(a_name)

    def get_default_m2m_column_name(self, e_name, col_name, composite_pk=False):
        col_name = '%s_%s' % (e_name.lower(), col_name)
        if not composite_pk:
            obsolete_col_name = e_name.lower()
        else:
            obsolete_col_name = col_name
        name = Name(col_name, obsolete_name=obsolete_col_name)
        return self.provider.normalize_name(name)

    def get_index_name(self, attr, table, cols, symmetric=False):
        if symmetric and attr.reverse_index and isinstance(attr.reverse.index, (basestring, tuple)):
            return attr.reverse_index
        if attr.index and isinstance(attr.index, (basestring, tuple)):
            return attr.index
        return self.get_default_index_name(table, cols)

    def get_default_index_name(self, table, cols):
        table_name = table.name if isinstance(table.name, basestring) else table.name[1]
        name = 'idx_%s__' % table_name.lower()
        name += '__'.join(col.name for col in cols)
        if table.is_m2m:
            obs_name = 'idx_%s' % obsolete(table_name).lower()
        else:
            obs_name = 'idx_%s__' % obsolete(table_name).lower()
            obs_name += '_'.join(obsolete(col.name) for col in cols)
        name = self.provider.normalize_name(Name(name, obs_name))
        if isinstance(table.name, tuple):
            name = table.name[0], name
        return name

    def get_default_key_name(self, table, cols):
        table_name = table.name if isinstance(table.name, basestring) else table.name[1]
        name = 'unq_%s__' % table_name.lower()
        name += '__'.join(col.name for col in cols)
        obs_name = 'unq_%s__' % obsolete(table_name).lower()
        obs_name += '_'.join(obsolete(col.name) for col in cols)
        name = self.provider.normalize_name(Name(name, obs_name))
        if isinstance(table.name, tuple):
            name = table.name[0], name
        return name

    def get_fk_name(self, attr, table, cols):
        if isinstance(attr, (Optional, Required)) and attr.fk_name:
            return attr.fk_name
        return self.get_default_fk_name(table, cols)

    def get_default_fk_name(self, table, cols):
        table_name = table.name if isinstance(table.name, basestring) else table.name[1]
        name = 'fk_%s__' % table_name.lower()
        name += '__'.join(col.name for col in cols)
        obs_name = 'fk_%s__' % obsolete(table_name).lower()
        obs_name += '__'.join(obsolete(col.name) for col in cols)
        name = self.provider.normalize_name(Name(name, obs_name))
        if isinstance(table.name, tuple):
            name = table.name[0], name
        return name

    def get_default_check_name(self, table, col):
        table_name = table.name if isinstance(table.name, basestring) else table.name[1]
        name = 'chk_%s__%s' % (table_name.lower(), col.name.lower())
        name = self.provider.normalize_name(Name(name, name))
        if isinstance(table.name, tuple):
            name = table.name[0], name
        return name

    def resolve_pk(self, entity, attr):
        result = []
        for pk_attr_name in entity.primary_key:
            pk_attr = entity.get_attr(pk_attr_name)
            if pk_attr.reverse:
                r_entity = pk_attr.reverse.entity
                r_pk = self.resolve_pk(r_entity, pk_attr)
                for col_name, obs_col_name, a, col_path in r_pk:
                    name = '%s_%s' % (self.get_column_name(attr, True), col_name)
                    obs_name = '%s_%s' % (self.get_column_name(attr, True), obs_col_name)
                    result.append((name, obs_name, a, '%s-%s' % (attr.name, col_path)))
            else:
                name = '%s_%s' % (self.get_column_name(attr, True), self.get_column_name(pk_attr, True))
                if len(entity.primary_key) > 1:
                    obs_name = name
                else:
                    obs_name = self.get_column_name(attr, True)
                result.append((name, obs_name, pk_attr, pk_attr.name))
        return result

    def add_fk_refs(schema, attr, table):
        add_converters = not attr.converters
        if add_converters:
            attr.converters = []
        columns = []
        reverse = attr.reverse
        resolved_pk = schema.resolve_pk(reverse.entity, attr)
        cols_from, cols_to, table_to = [], [], None
        reverse_entity = reverse.entity
        if reverse.entity is None:
            return [], None, None
        table_to_name = schema.get_table_name(reverse_entity.get_root())
        if table_to_name not in schema.tables:
            schema.table_cls.from_entity(schema, reverse_entity.get_root())
        table_to = schema.tables[table_to_name]
        cols_to = table_to.primary_key.cols
        # validate
        columns_provided = 'columns' in attr.provided.kwargs
        column_provided = 'column' in attr.provided.kwargs
        provided_col_names = []
        if column_provided and columns_provided:
            throw(MappingError, 'Both `column` and `columns` options cannot be passed simultaneously')
        if len(resolved_pk) == 1:
            if columns_provided:
                throw(MappingError, 'Invalid number of columns specified for %r' % attr)
            elif column_provided:
                provided_col_names = [attr.provided.kwargs['column']]
        else:
            if column_provided:
                throw(MappingError, 'Invalid number of columns specified for %r' % attr)
            elif columns_provided:
                provided_col_names = attr.provided.kwargs['columns']
                if len(provided_col_names) != len(resolved_pk):
                    throw(MappingError, 'Invalid number of columns specified for %r' % attr)

        for i, (col_name, obs_name, pk_attr, col_path) in enumerate(resolved_pk):
            if provided_col_names:
                col_name = provided_col_names[i]
            name = Name(col_name, obs_name)
            converter = schema.provider.get_converter_by_attr(pk_attr)
            if add_converters:
                attr.converters.append(converter)
            new_converter = converter.make_fk_converter(attr)
            column = schema.column_cls(table, name, new_converter, attr.sql_type)
            if isinstance(attr, Optional) or attr.entity.bases or attr.nullable:
                column.nullable = True
            attr.columns.append(column)
            cols_from.append(column)
            columns.append(column)
            attr.col_paths.append(col_path)
        # assert table_to

        index_name = schema.get_index_name(attr, table, cols_from)
        index = schema.index_cls(table, cols_from, index_name)
        fk_name = schema.get_fk_name(attr, table, cols_from)
        fk = schema.fk_cls(table, table_to, cols_from, cols_to, fk_name)
        if attr.reverse.cascade_delete:
            fk.on_delete = 'CASCADE'
        elif isinstance(attr, Optional) and attr.nullable:
            fk.on_delete = 'SET NULL'
        return columns, fk, index

    def make_column(schema, attr, table):
        # TODO: IMPORTANT! If column is set by user - it chooses the side of the relation column
        # for example Required - Optional (column set here) will create column on the Optional side

        if attr.reverse:
            r_attr = attr.reverse
            if isinstance(attr, Required):
                if type(r_attr) in (Optional, Set):
                    return schema.add_fk_refs(attr, table)
            elif isinstance(attr, Optional):
                if isinstance(r_attr, Optional):
                    if attr.provided.kwargs.get('column') or attr.provided.kwargs.get('columns'):
                        # throw(NotImplementedError, 'Optional to Optional link with provided columns on both sides')
                        return schema.add_fk_refs(attr, table)
                    elif attr == min(attr, r_attr, key=lambda a: (a.entity.name, a.name)) and not (
                            r_attr.provided.kwargs.get('column') or r_attr.provided.kwargs.get('columns')
                    ):
                        return schema.add_fk_refs(attr, table)
                elif isinstance(r_attr, Set):
                    return schema.add_fk_refs(attr, table)
                elif isinstance(r_attr, Required):
                    if attr.provided.kwargs.get('column') or attr.provided.kwargs.get('columns'):
                        return schema.add_fk_refs(attr, table)
            elif isinstance(attr, Set):
                if isinstance(r_attr, Set):
                    r_table_name = schema.get_table_name(r_attr.entity.get_root())
                    if r_table_name in schema.tables:
                        if attr.entity == r_attr.entity and (r_attr, attr) in schema.m2m_to_create:
                            return [], None, None
                        schema.m2m_to_create.append((attr, r_attr))
            return [], None, None
        else:
            attr.converters = [schema.provider.get_converter_by_attr(attr)]
            attr.col_paths = [attr.name]
            column = schema.column_cls.from_attr(table, attr)
            attr.columns.append(column)
            if attr.unique:
                schema.unique_cls(attr.columns)
            if attr.check:
                schema.check_cls(column, attr.check)
            if attr.index:
                index_name = schema.get_index_name(attr, table, [column])
                new_index = schema.index_cls(table, [column], index_name)
            table.columns[column.name] = column
            return [column], None, None

    def prepare_sql(schema):
        sql_ops = []
        # for drop_op in schema.drop_ops:
        #     sql_ops.append(drop_op.get_sql())
        #
        # schema.drop_ops = []

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
            if schema.provider.dialect == 'Oracle':
                sql_ops.extend(table.create_trigger_and_sequence())
            sql_ops.extend(table.create_indexes())
            sql_ops.extend(table.create_constraints())
        for table in schema.tables_to_create:
            sql_ops.extend(table.create_fkeys())

        for attr in schema.attrs_to_create:
            for column in attr.columns:
                sql_ops.extend(column.get_add_sql())

        schema.tables_to_create = []

        for op in schema.ops:
            sql_ops.append(op)

        schema.ops = []
        return sql_ops

    def apply(schema, connection, verbose, sql_only):
        sql_ops = schema.prepare_sql()

        if sql_only:
            for op in sql_ops:
                print(op.get_sql())
            return

        last_sql = None
        try:
            cursor = connection.cursor()
            for op in sql_ops:
                last_sql = op.get_sql()
                last_obj = op.obj
                schema.provider.execute(cursor, op.sql)
                if verbose:
                    print(last_sql)
        except Exception as e:
            schema.errors += 1
            if last_sql:
                print('Last SQL: %s' % last_sql, file=sys.stderr)
            if last_obj:
                print('last object: %s %s' % (last_obj.typename, last_obj.name),
                      file=sys.stderr)
            raise

    @staticmethod
    def create_upgrade_table_sql():
        return 'CREATE TABLE "pony_version"("version" text not null)'

    def get_pony_version_sql(schema):
        return 'SELECT "version" from "pony_version"'

    def check_table_exists(schema, table_name, connection):
        return schema.provider.table_exists(connection, table_name, case_sensitive=False)

    def insert_pony_version_sql(schema, version):
        return 'INSERT INTO "pony_version" VALUES (%r)' % version

    def set_pony_version_sql(schema, version=None):
        from pony import __version__
        return 'UPDATE "pony_version" SET "version" = %r' % (version or __version__)

    def upgrade(schema, prev_version, connection):
        from pony import __version__, orm
        assert __version__.startswith('0.9')
        UpgradeError = orm.core.UpgradeError
        if not prev_version.startswith('0.7'):
            throw(UpgradeError, 'To upgrade Pony to version 0.9 you should have Pony 0.7 or lower.')
        provider = schema.provider
        # 0.7 -> 0.9
        #
        # New colnames for links, that also leads to fk and indexes renames
        #   Old way was `attr.name` for single column and `attr.column` + `pk_col[n]` for composite pk links
        #   New way always adds `pk_col` even if pk is not composite
        #
        # New m2m table and column names, same for fk and index
        #   Old way: min(entity1, entity2).lower() + max(entity1, entity2).lower()
        #   New way: min(entity1, entity2).lower() + reverse.name (of minimal entity)
        tables = list(sorted(schema.tables.values(), key=lambda x: x.is_m2m))
        for table in tables:
            cols = list(table.columns.values())
            if table.is_m2m:
                new_name = table.name
                old_name = obsolete(table.name)
                if new_name != old_name:
                    if isinstance(old_name, tuple):
                        schema.tables.pop(table.name)
                        table.name = old_name[1]
                        schema.tables[table.name] = table
                        schema.ops.extend(table.get_change_schema_sql(new_name[0]))
                    schema.tables.pop(table.name)
                    table.name = old_name
                    schema.tables[table.name] = table
                    schema.rename_table(table, new_name, ignore_indexes=True)
                    if table.exists(provider, connection):
                        throw(UpgradeError, 'Pony wants to rename table %r to %r but this name is already taken' %
                              (old_name, new_name))

            for col in cols:
                old_col_name = obsolete(col.name)
                new_col_name = col.name
                if old_col_name != new_col_name:
                    table.columns.pop(new_col_name)
                    col.name = old_col_name
                    table.columns[old_col_name] = col
                    schema.rename_column(col, new_col_name, ignore_indexes=table.is_m2m)
                    if col.exists(provider, connection):
                        throw(UpgradeError, 'Pony wants to rename column %r.%r to %r.%r but this name is already taken'%
                              (table.name, old_col_name, table.name, new_col_name))

            if not table.is_m2m:
                for index in table.indexes:
                    old_idx_name = obsolete(index.name)
                    new_idx_name = index.name
                    if old_idx_name != new_idx_name:
                        index.name = old_idx_name
                        schema.rename_index(index, new_idx_name)
                        if index.exists(provider, connection):
                            throw(UpgradeError,
                                  'Pony wants to rename index %r to %r but this name is already taken' %
                                  (old_idx_name, new_idx_name))

            if provider.dialect != 'SQLite':
                for fk in table.foreign_keys:
                    old_fk_name = obsolete(fk.name)
                    new_fk_name = fk.name
                    if old_fk_name != new_fk_name:
                        fk.name = old_fk_name
                        schema.rename_foreign_key(fk, new_fk_name)
                        if fk.exists(provider, connection):
                            throw(UpgradeError,
                                  'Pony wants to rename foreign key %r to %r but this name is already taken' %
                                  (old_fk_name, new_fk_name))

                for key in table.keys:
                    if key.is_pk:
                        continue
                    old_key_name = obsolete(key.name)
                    new_key_name = key.name
                    if old_key_name != new_key_name:
                        key.name = old_key_name
                        schema.rename_key(key, new_key_name)
                        # TODO check for these keys also?

                # In Pony 0.9+ we dont add UNIQUE keyword in column def, we create named UNIQUE constraint or index
                # We should rename old names
                for con in table.constraints:
                    if not con.typename == 'Unique constraint' or len(con.cols) != 1:
                        continue
                    old_name = con.dbms_name(connection)
                    if old_name is None:
                        throw(orm.core.UpgradeError, 'Expected UNIQUE constraint for column %r was not found'
                              % con.cols[0].name)
                    new_name = con.name
                    con.name = old_name
                    schema.ops.extend(con.get_rename_sql(new_name))
                    con.name = new_name
                    if con.exists(provider, connection):
                        throw(UpgradeError, 'Pony wants to rename constraint %r to %r but this name is already taken' %
                              (old_name, new_name))

        for table in tables:
            if table.is_m2m:
                # Pony 0.7 had a bug which creates 2 indexes with the same name for m2m tables
                # Since they were stored in dict there was a name overlap
                # Second index always erased the first
                old_index_names = {obsolete(index.name)[1] if isinstance(index.name, tuple) else obsolete(index.name)
                                   for index in table.indexes}
                assert len(old_index_names) == 1
                index = table.indexes[0]
                new_index_name = index.name
                index.name = tuple(old_index_names)[0]
                schema.ops.extend(index.get_drop_sql())
                index.name = new_index_name
                for index in table.indexes:
                    schema.ops.extend(index.get_create_sql())
                    if index.exists(provider, connection):
                        throw(UpgradeError,
                              'Pony wants to create index %r but this name is already taken' % new_index_name)

        return schema.prepare_sql(connection)

    def downgrade(schema, connection):
        tables = list(sorted(schema.tables.values(), key=lambda x: x.is_m2m))
        for table in tables:
            cols = list(table.columns.values())
            if table.is_m2m:
                new_name = table.name
                old_name = obsolete(table.name)
                if new_name != old_name:
                    if isinstance(old_name, tuple):
                        schema.ops.extend(table.get_change_schema_sql(old_name[0]))
                    schema.rename_table(table, old_name)

            for col in cols:
                old_col_name = obsolete(col.name)
                new_col_name = col.name
                if old_col_name != new_col_name:
                    schema.rename_column(col, old_col_name)

            if table.is_m2m:
                index1 = table.indexes[0]
                index2 = table.indexes[1]
                schema.drop_index(index1)
                schema.rename_index(index2, obsolete(index2.name))

            if not table.is_m2m:
                for index in table.indexes:
                    old_idx_name = obsolete(index.name)
                    new_idx_name = index.name
                    if old_idx_name != new_idx_name:
                        schema.rename_index(index, old_idx_name)

            for fk in table.foreign_keys:
                old_fk_name = obsolete(fk.name)
                new_fk_name = fk.name
                if old_fk_name != new_fk_name:
                    schema.rename_foreign_key(fk, old_fk_name)

            for key in table.keys:
                if key.is_pk:
                    continue
                old_key_name = obsolete(key.name)
                new_key_name = key.name
                if old_key_name != new_key_name:
                    schema.rename_key(key, old_key_name)

        return schema.ops
