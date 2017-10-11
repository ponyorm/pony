from collections import OrderedDict, defaultdict

from pony.orm.dbschema import Table, Column, Trigger, DBIndex, ForeignKey

from .operations import Op, OperationBatch, alter_table
from .diagram_ops import RenameEntity, RenameAttr, AddAttr, ModifyAttr

class Executor(object):

    def __init__(self, prev_db, new_db, entity_ops):
        self.prev_db = prev_db
        self.new_db = new_db
        self.entity_ops = entity_ops
        self.prev_schema = prev_db.generate_schema()
        self.new_schema = new_db.generate_schema()

        entities = prev_db.entities.copy()
        new_entities = new_db.entities

        for op in self.entity_ops:
            if isinstance(op, RenameEntity):
                assert op.new_name not in entities
                new_entity = new_entities[op.new_name]
                prev_entity = entities.pop(op.old_name)
                entities[op.new_name] = prev_entity
                prev_entity._new_entity_ = new_entity
                new_entity._prev_entity_ = prev_entity

        for entity_name, prev_entity in entities.items():
            if prev_entity._new_entity_ is None:
                new_entity = new_entities.get(entity_name)
                if new_entity is not None and new_entity._prev_entity_ is None:
                    prev_entity._new_entity_ = new_entity
                    new_entity._prev_entity_ = prev_entity
            new_entity = prev_entity._new_entity_
            if new_entity is not None:
                prev_entity._table_object_.new = new_entity._table_object_
                new_entity._table_object_.prev = prev_entity._table_object_

        entities = prev_db.entities.copy()

        for op in self.entity_ops:
            if isinstance(op, RenameEntity):
                prev_entity = entities.pop(op.old_name)
                entities[op.new_name] = prev_entity
            elif isinstance(op, RenameAttr):
                prev_entity = entities[op.entity_name]
                new_entity = new_entities[op.entity_name]  # fixme
                assert prev_entity._table_object_.new is new_entity._table_object_
                assert new_entity._table_object_.prev is prev_entity._table_object_
                prev_attr = prev_entity._adict_[op.old_name]
                new_attr = new_entity._adict_[op.new_name]
                prev_attr.new_attr = new_attr
                new_attr.prev_attr = prev_attr

        for entity_name, prev_entity in entities.items():
            new_entity = prev_entity._new_entity_
            if new_entity is not None:
                for prev_attr in prev_entity._new_attrs_:
                    if prev_attr.new_attr is None:
                        new_attr = new_entity._adict_.get(prev_attr.name)
                        if new_attr is not None and new_attr.prev_attr is None:
                            prev_attr.new_attr = new_attr
                            new_attr.prev_attr = prev_attr

        for entity_name, prev_entity in entities.items():
            new_entity = prev_entity._new_entity_
            if new_entity is not None:
                assert prev_entity._table_object_.new is new_entity._table_object_
                assert new_entity._table_object_.prev is prev_entity._table_object_
                for prev_attr in prev_entity._new_attrs_:
                    new_attr = prev_attr.new_attr
                    if new_attr is not None and not (prev_attr.is_collection or new_attr.is_collection):
                        assert len(prev_attr.columns) == len(new_attr.columns)
                        assert len(prev_attr.column_objects) == len(new_attr.column_objects)
                        for prev_col, new_col in zip(prev_attr.column_objects, new_attr.column_objects):
                            prev_col.new = new_col
                            new_col.prev = prev_col

        for prev_constraint in self.prev_schema.constraints.values():
            new_constrant = self.new_schema.constraints.get(prev_constraint.name)
            if new_constrant is not None:
                prev_constraint.new = new_constrant
                new_constrant.prev = prev_constraint

        for prev_table in self.prev_schema.tables.values():
            if prev_table.m2m:
                prev_attr = next(iter(prev_table.m2m))
                new_attr = prev_attr.new_attr
                if new_attr is not None:
                    for new_table in self.new_schema.tables.values():
                        if new_attr in new_table.m2m:
                            prev_table.new = new_table
                            new_table.prev = prev_table
                            for prev_col, new_col in zip(prev_table.column_list, new_table.column_list):
                                prev_col.new = new_col
                                new_col.prev = prev_col

    def generate(self):
        ops = []

        created_new_tables = set()
        for new_table in self.new_schema.order_tables_to_create():
            prev_table = new_table.prev
            if prev_table is None:
                for new_obj in new_table.get_objects_to_create(created_new_tables):
                    sql = new_obj.get_create_command()
                    ops.append(Op(sql, obj=new_obj, type='create'))
            else:
                ops.extend(new_table.get_alter_ops())

        created_prev_tables = set()
        for table in self.prev_schema.order_tables_to_create():
            for prev_obj in reversed(table.get_objects_to_create(created_prev_tables)):
                if prev_obj.new is None:
                    ops.extend(prev_obj.get_drop_ops())

        # handle renames
        extra_ops = []
        for prev_name, prev_table in self.prev_schema.tables.items():
            new_table = prev_table.new
            if new_table is not None and prev_name != new_table.name:
                extra_ops.extend(prev_table.get_rename_ops())
            if self.new_schema.provider.dialect != 'SQLite':
                for prev_col in prev_table.column_list:
                    new_col = prev_col.new
                    if new_col is not None and prev_col.name != new_col.name:
                        extra_ops.extend(prev_col.get_rename_ops())
        ops = extra_ops + ops

        schema = self.new_schema
        provider = schema.provider
        quote_name = provider.quote_name

        # handle initials
        for op in ops[:]:
            col = op.obj
            if not isinstance(col, Column): continue
            new_attr = col.attr
            prev_attr = new_attr.prev_attr

            if (op.type == 'create' or op.type == 'alter' and new_attr.is_required and not prev_attr.is_required) \
                    and col.is_not_null and not col.sql_default and new_attr.initial is not None:

                value_class = provider.sqlbuilder_cls.value_class
                op.sql += ' DEFAULT %s' % value_class(provider.paramstyle, new_attr.initial)  # fix Oracle?

                sql, _ = provider.ast2sql([ 'ALTER_COLUMN_DEFAULT', col.name ])
                ops.append(Op(sql, obj=col, type='alter', prefix=alter_table(col.table)))

        for op in self.entity_ops:
            if isinstance(op, ModifyAttr):
                new_entity = self.new_db.entities[op.entity_name]
                new_attr = new_entity._adict_[op.attr_name]
                if new_attr.initial is not None and not (new_attr.is_required and not new_attr.prev_attr.is_required):
                    assert len(new_attr.columns) == 1
                    col_name = new_attr.columns[0]
                    table_name = new_attr.entity._table_
                    sql_ast = [ 'UPDATE', table_name, [ [ 'COLUMN', None, col_name ], [ 'VALUE', new_attr.initial ] ],
                                [ 'WHERE', [ 'IS_NULL', [ 'COLUMN', None, col_name ] ] ] ]
                    sql = provider.ast2sql(sql_ast)[0]
                    ops.append(Op(sql, obj=new_attr, type='set_defaults'))

        def keyfunc(op):
            if op.type == 'rename':
                return 3
            if op.type == 'drop' or isinstance(op.type, list) and 'drop' in op.type:
                if isinstance(op.obj, Trigger):
                    return 0
                if isinstance(op.obj, ForeignKey):
                    return 1
                if isinstance(op.obj, DBIndex):
                    return 2
                if isinstance(op.obj, Table):
                    return 5
            return 10

        result = []
        for op in sorted(ops, key=keyfunc):
            if isinstance(op, OperationBatch):
                result.extend(op)
            else:
                result.append(op)
        return result
