from collections import OrderedDict, defaultdict

from pony.orm.dbschema import Table, Column, DBIndex, ForeignKey

from .operations import Op, OperationBatch, alter_table
from .diagram_ops import RenameEntity, RenameAttr, AddAttr, ModifyAttr


class Executor(object):

    def __init__(self, prev_db, new_db, entity_ops):
        self.prev_db = prev_db
        self.new_db = new_db
        self.entity_ops = entity_ops
        self.prev_schema = prev_db.generate_schema()
        self.new_schema = new_db.generate_schema()
        self.prev_objects = self.prev_schema.objects_to_create()
        self.new_objects = self.new_schema.objects_to_create()

        self.renamed_tables = renamed_tables = {}
        self.renamed_columns = renamed_columns = defaultdict(dict)
        renamed_entities = {}

        for op in self.entity_ops:
            if isinstance(op, RenameEntity):
                prev_entity = prev_db.entities[op.old_name]
                new_entity = new_db.entities[op.new_name]
                prev_entity._new_entity_ = new_entity
                new_entity._prev_entity_ = prev_entity

                renamed_entities[prev_entity] = new_entity
                renamed_tables[new_entity._table_] = prev_entity._table_

        for op in self.entity_ops:
            if isinstance(op, RenameAttr):
                prev_entity = prev_db.entities[op.entity_name]
                prev_table_name = prev_entity._table_
                entity = renamed_entities.get(prev_entity, new_db.entities.get(op.entity_name))
                prev_attr = prev_entity._adict_[op.old_name]
                new_attr = entity._adict_[op.new_name]
                prev_attr.new_attr = new_attr
                new_attr.prev_attr = prev_attr
                assert len(prev_attr.columns) == len(new_attr.columns)
                assert len(prev_attr.column_objects) == len(new_attr.column_objects)
                for prev_col, new_col in zip(prev_attr.column_objects, new_attr.column_objects):
                    prev_col.new = new_col
                    new_col.prev = prev_col

                for prev_col, col in zip(prev_attr.columns, new_attr.columns):
                    if prev_col != col:
                        renamed_columns[prev_table_name][col] = prev_col

        self.initials = initials = {}
        for op in self.entity_ops:
            if isinstance(op, (AddAttr, ModifyAttr)):
                entity = self.new_db.entities[op.entity_name]
                attr = entity._adict_[op.attr_name]
                if attr.initial is not None:
                    initials[attr] = attr.initial

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
        ops = list(self._generate_ops())
        ops = self._sorted(ops)
        return ops

    def handle_initials(self, ops):
        schema = self.new_schema
        provider = schema.provider
        quote_name = provider.quote_name

        extra_ops = []

        for op in ops:
            col = op.obj

            if not isinstance(col, Column):
                continue

            if op.type != 'create' and not (
                    op.type == 'alter' and col.attr.is_required and not col.attr.prev_attr.is_requried):
                continue

            if not col.is_not_null or col.sql_default:
                continue

            attr = col.attr
            if attr not in self.initials: continue
            value = self.initials.pop(attr)

            value_class = provider.sqlbuilder_cls.value_class
            op.sql += ' DEFAULT %s' % value_class(provider.paramstyle, value)  # fix Oracle?



            schema = col.table.schema
            drop_default_clause = 'DROP DEFAULT' if provider.dialect != 'Oracle' else 'DEFAULT NULL'
            sql = '{} {} {}'.format(
                schema.MODIFY_COLUMN, quote_name(col.name), schema.case(drop_default_clause))

            extra_ops.append(Op(sql, col, type='alter', prefix=alter_table(col.table)))

        for attr, value in self.initials.items():
            assert len(attr.columns) == 1
            col_name = attr.columns[0]
            table_name = attr.entity._table_
            sql_ast = [ 'UPDATE', table_name, [ [ 'COLUMN', None, col_name ], [ 'VALUE', value ] ],
                        [ 'WHERE', [ 'IS_NULL', [ 'COLUMN', None, col_name ] ] ] ]
            sql = provider.ast2sql(sql_ast)[0]
            extra_ops.append(Op(sql, attr, type='set_defaults'))

        return ops + extra_ops

    def _sorted(self, ops):
        # handle renames
        renamed_tables = self.renamed_tables
        renamed_columns = self.renamed_columns
        extra_ops = []
        if renamed_tables or renamed_columns:
            for table in self.new_objects:
                prev_name = renamed_tables.get(table.name)
                if prev_name:
                    extra_ops.extend(table.get_rename_ops(prev_name))
            if self.new_schema.provider.dialect != 'SQLite':
                for table in self.new_objects:
                    for name, prev_name in renamed_columns.get(table.name, {}).items():
                        col = table.column_dict[name]
                        extra_ops.extend(col.get_rename_ops(prev_name))
        ops = extra_ops + ops

        ops = self.handle_initials(ops)

        def keyfunc(op):
            if op.type == 'rename':
                return 3
            if op.type == 'drop' or isinstance(op.type, list) and 'drop' in op.type:
                if self.new_db.provider.dialect == 'Oracle':
                    from pony.orm.dbproviders import oracle
                    if isinstance(op.obj, oracle.OraTrigger):
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

    def _generate_ops(self):
        # by table
        prev_objects = OrderedDict()
        prev_tables = {}
        for t, objects in self.prev_objects.items():
            prev_tables[t.name] = t
            prev_objects[t.name] = objects.copy()
        new_objects = OrderedDict(
            ((t.name, objects.copy()) for t, objects in self.new_objects.items())
        )
        renamed_tables = self.renamed_tables

        for table_name, objects in new_objects.items():
            for obj_name, new_obj in objects.items():
                sql = new_obj.get_create_command()
                if table_name in renamed_tables:
                    table_name = renamed_tables[table_name]
                if obj_name in renamed_tables:
                    obj_name = renamed_tables[obj_name]
                prev_obj = prev_objects.get(table_name, {}).pop(obj_name, None)
                renamed_columns = self.renamed_columns.get(table_name, {})

                if prev_obj is None:
                    yield Op(sql, new_obj, type='create')
                elif sql != prev_obj.get_create_command():
                    for item in new_obj.get_alter_ops(
                            prev_obj, new_objects, executor=self, renamed_columns=renamed_columns):
                        yield item

        for table_name, objects in prev_objects.items():
            for prev_obj in objects.values():
                table = prev_tables[table_name]
                kw = {}
                if not isinstance(prev_obj, Table):
                    kw['table'] = table
                for item in prev_obj.get_drop_ops(inside_table=False, **kw):
                    yield item
