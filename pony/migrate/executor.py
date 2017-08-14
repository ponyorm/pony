from __future__ import unicode_literals

import warnings
from collections import OrderedDict, defaultdict
from copy import copy

from pony.utils import cached_property
from pony.orm import sqlbuilding
from pony.orm.dbschema import Table, Column, DBIndex, ForeignKey

from .operations import Op, OperationBatch, alter_table
from .diagram_ops import RenameEntity, RenameAttr, AddAttr, ModifyAttr


class Executor(object):

    def __init__(self, schema, prev_schema, db, prev_db, entity_ops, operations):
        self.schema = schema
        self.prev_schema = prev_schema
        self.db = db
        self.prev_db = prev_db
        self.entity_ops = entity_ops
        self.operations = operations
        self.prev_objects = prev_schema.objects_to_create()
        self.new_objects = schema.objects_to_create()

        self.renamed_tables = renamed_tables = {}
        self.renamed_columns = renamed_columns = defaultdict(dict)
        renamed_entities = {}

        for op in self.entity_ops:
            if isinstance(op, RenameEntity):
                prev_entity = prev_db.entities[op.old_name]
                entity = db.entities[op.new_name]
                renamed_entities[prev_entity] = entity
                renamed_tables[entity._table_] = prev_entity._table_

        for op in self.entity_ops:
            if isinstance(op, RenameAttr):
                prev_entity = prev_db.entities[op.entity_name]

                if prev_entity in renamed_entities:
                    entity = renamed_entities[prev_entity]
                else:
                    entity = db.entities[op.entity_name]

                prev_attr = getattr(prev_entity, op.old_name)
                new_attr = entity._adict_[op.new_name]
                for prev_col, col in zip(prev_attr.columns, new_attr.columns):
                    if prev_col != col:
                        renamed_columns[prev_attr.entity._table_][col] = prev_col

    def generate(self):
        ops = list(self._generate_ops())
        ops = self._sorted(ops)
        return ops

    @cached_property
    def defaults(self):
        result = {}
        for op in self.entity_ops:
            if isinstance(op, (AddAttr, ModifyAttr)):
                entity = self.db.entities[op.entity_name]
                attr = entity._adict_[op.attr_name]
                if attr.initial is not None:
                    result[attr] = attr.initial
        return result

    def handle_defaults(self, ops):
        schema = self.schema
        provider = schema.provider
        quote_name = provider.quote_name

        extra_ops = []

        def find_attr(column):
            for entity in column.table.entities:
                for attr in entity._attrs_:
                    if len(attr.columns) > 1: raise NotImplementedError
                    if column.name == attr.columns[0]:
                        return attr
            raise NotImplementedError

        columns = {op.obj for op in ops if isinstance(op.obj, Column)}
        col2attrs = {column: find_attr(column) for column in columns}

        for op in ops:
            col = op.obj
            cond = isinstance(col, Column)
            if not cond:
                continue
            if op.type == 'alter':
                is_required = col2attrs[op.obj].is_required
                entity = col2attrs[col].entity
                entity_name = self.renamed_tables.get(entity.__name__, entity.__name__)
                was_required = self.prev_db.entities[entity_name]
                cond = is_required and not was_required
            else:
                cond = cond and op.type == 'create'
            if not cond:
                continue
            if col.is_not_null and not col.sql_default:
                for attr, value in self.defaults.items():
                    if attr.get_declaration_id() != col2attrs[col].get_declaration_id():
                        continue
                    value_class = provider.sqlbuilder_cls.value_class
                    value = value_class(provider.paramstyle, value)
                    # hack
                    _default = 'DEFAULT {}'.format(value)
                    if provider.dialect == 'Oracle':
                        for sub in ('NOT NULL', 'NULL'):
                            ind = op.sql.rfind(sub)
                            if ind == -1:
                                continue
                            op.sql = ' '.join(
                                (op.sql[:ind], _default, op.sql[ind:])
                            )
                            break
                    else:
                        op.sql = ' '.join((op.sql, _default))
                    del self.defaults[attr]
                    schema = col.table.schema
                    if provider.dialect == 'Oracle':
                        sql = schema.case('{} {} DEFAULT NULL').format(
                            schema.MODIFY_COLUMN, quote_name(col.name),
                        )
                    else:
                        sql = schema.case('{} {} DROP DEFAULT').format(
                            schema.MODIFY_COLUMN, quote_name(col.name),
                        )
                    op = Op(sql, col, type='alter', prefix=alter_table(col.table))
                    extra_ops.append(op)
                    break

        for obj, value in self.defaults.items():
            [col] = obj.columns
            table = obj.entity._table_
            col_is_null = ['IS_NULL', ['COLUMN', table, col]]
            builder = sqlbuilding.SQLBuilder(self.schema.provider, col_is_null)

            value = builder.VALUE(value)
            ctx = {
                'table': quote_name(table),
                'col': quote_name(col),
                'col_is_null': ''.join(builder.result),
                'value': value,
            }
            sql = 'UPDATE {table} SET {col} = {value} WHERE {col_is_null}'.format(**ctx)

            extra_ops.append(
                Op(sql, obj, 'set_defaults')
            )

        return ops + extra_ops

    def _sorted(self, ops):
        schema = self.schema
        drop_primary = [
            op for op in ops if op.type == 'drop'
            if isinstance(op.obj, DBIndex) and op.obj.is_pk
        ]

        exclude, include = [], []

        for op in ops:
            if not isinstance(op.obj, Column):
                continue
            for index_op in drop_primary:
                if op.obj.name in index_op.obj.col_names:
                # TODO if op.obj in index_op.obj.columns:
                    break
            else:
                continue
            include.append(op + index_op)
            exclude.extend((op, index_op))

        ops = include + [op for op in ops if op not in exclude]

        # handle renames
        renamed_tables = self.renamed_tables
        renamed_columns = self.renamed_columns
        extra_ops = []
        if renamed_tables or renamed_columns:
            for table in self.new_objects:
                prev_name = renamed_tables.get(table.name)
                if prev_name:
                    extra_ops.extend(table.get_rename_ops(prev_name))
            if self.schema.provider.dialect != 'SQLite':
                for table in self.new_objects:
                    for name, prev_name in renamed_columns.get(table.name, {}).items():
                        col = table.column_dict[name]
                        extra_ops.extend(col.get_rename_ops(prev_name))
        ops = extra_ops + ops

        ops = self.handle_defaults(ops)

        def is_instance(obj, klass):
            if not isinstance(obj, (list, tuple)):
                return isinstance(obj, klass)
            return any(isinstance(o, klass) for o in obj)

        def keyfunc(op):
            if op.type == 'rename':
                return 3
            if isinstance(op.type, list):
                cond = 'drop' in op.type
            else:
                cond = op.type == 'drop'
            if cond:
                if self.db.provider.dialect == 'Oracle':
                    from pony.orm.dbproviders import oracle
                    if is_instance(op.obj, oracle.OraTrigger):
                        return 0
                if is_instance(op.obj, ForeignKey):
                    return 1
                if is_instance(op.obj, DBIndex):
                    return 2
                if is_instance(op.obj, Table):
                    return 5
            return 10

        result = sorted(ops, key=keyfunc)

        def flatten(ops):
            li = []
            for op in ops:
                if isinstance(op, OperationBatch):
                    li.extend(op)
                else:
                    li.append(op)
            return li

        return flatten(result)

    def _generate_ops(self):
        # by table
        prev_objects = OrderedDict()
        prev_tables = {}
        for t, objects in self.prev_objects.items():
            prev_tables[t.name] = t
            prev_objects[t.name] = copy(objects)
        new_objects = OrderedDict(
            ((t.name, copy(objects)) for t, objects in self.new_objects.items())
        )
        renamed_tables = self.renamed_tables

        for table_name, objects in new_objects.items():
            for obj_name, obj in objects.items():
                sql = obj.get_create_command()
                if table_name in renamed_tables:
                    table_name = renamed_tables[table_name]
                if obj_name in renamed_tables:
                    obj_name = renamed_tables[obj_name]
                prev_obj = prev_objects.get(table_name, {}).pop(obj_name, None)
                renamed_columns = self.renamed_columns.get(table_name, {})

                if prev_obj is not None:
                    if prev_obj.get_create_command() == sql:
                        continue
                    # obj is altered
                    for item in obj.get_alter_ops(prev_obj, new_objects, executor=self,
                                                    renamed_columns=renamed_columns):
                        yield item
                    continue
                op = Op(sql, obj, type='create')
                yield op

        for table_name, objects in prev_objects.items():
            for obj in objects.values():
                table = prev_tables[table_name]
                kw = {}
                if not isinstance(obj, Table):
                    kw['table'] = table
                for item in obj.get_drop_ops(inside_table=False, **kw):
                    yield item
