from pony.py23compat import basestring

from pony.orm.migrations import virtuals as v
from pony.orm.migrations.serialize import serialize

from pony.orm import core
from pony.utils import throw


class NotProvided:
    def __getitem__(self, item):
        pass

    def __len__(self):
        pass

    def __bool__(self):
        return False


NOT_PROVIDED = NotProvided()


class BaseOperation(object):
    not_implemented = []

    def serialize(self, imports):
        imports['pony.orm.migrations.operations'].add(self.__class__.__name__)

    def apply(self, vdb):
        throw(NotImplementedError, self.__class__.__name__)

    def get_entity_attr(self, vdb):
        entity = vdb.entities[self.entity_name]
        attr = entity.new_attrs[self.attr_name]
        return entity, attr


def NotImplemented(*providers):
    def decorator(op, providers=providers):
        if not providers:
            providers = ['sqlite', 'postgres', 'mysql', 'oracle']
        op.not_implemented = providers
        return op

    return decorator


class AddAttribute(BaseOperation):
    def __init__(self, entity_name, attr, sql=None):
        self.entity_name = entity_name
        self.attr = attr
        self.sql = sql

    def apply(self, vdb):
        entity = vdb.entities.get(self.entity_name)
        attr = self.attr
        if entity is None:
            throw(core.MigrationError, 'Entity %s does not exist' % self.entity_name)
        if attr.name in entity.new_attrs:
            throw(core.MigrationError, 'Attribute %s is already defined' % attr.name)

        if isinstance(attr, v.Required):
            if attr.is_required and attr.provided.initial is None:
                throw(core.MigrationError, 'initial option should be specified in case of adding the Required attribute')
            attr.initial = attr.provided.initial

        if isinstance(attr, v.Discriminator):
            if not attr.provided.initial or not attr.initial:
                attr.initial = attr.provided.initial = self.entity_name

        attr.entity = entity
        entity.add_attr(self.attr)
        if attr.reverse:
            r_entity = vdb.entities.get(attr.py_type)
            if r_entity is None:
                return
            r_attr = r_entity.get_attr(getattr(attr.reverse, 'name', attr.reverse))
            if r_attr is None:
                return
            r_attr.entity = r_entity
            r_attr.reverse = attr
            attr.reverse = r_attr
            attr.resolve_cascade(r_attr)
            r_attr.resolve_cascade(attr)

        if isinstance(attr, v.Optional) and attr.py_type is basestring:
            attr.sql_default = ''

        if not vdb.vdb_only:
            if self.sql:
                vdb.schema.add_sql(self.sql)
            else:
                self.apply_to_schema(vdb, attr)

    @staticmethod
    def apply_to_schema(vdb, attr):
        schema = vdb.schema
        root = attr.entity.get_root()
        table = schema.tables.get(schema.get_table_name(root))
        if table is None:
            return
        columns, fk, index = schema.make_column(attr, table)
        if table not in schema.tables_to_create:
            schema.add_columns(columns)
            if fk:
                schema.add_fk(fk)
            if index:
                schema.add_index(index)
        r_attr = attr.reverse
        if r_attr in schema.attrs_to_create:
            r_table = schema.attrs_to_create.pop(r_attr)
            columns, fk, index = schema.make_column(r_attr, r_table)
            schema.add_columns(columns)
            if fk:
                schema.add_fk(fk)
            if index:
                schema.add_index(index)

        if attr.initial is not None:
            schema.drop_initial(attr.columns)

        for attr, r_attr in schema.m2m_to_create:
            table_name = schema.get_m2m_table_name(attr, r_attr)
            if table_name in schema.tables:
                continue
            table = schema.create_m2m_table(attr, r_attr)
            table.created = False
            schema.tables_to_create.append(table)

        schema.m2m_to_create[:] = []

    def serialize(self, imports):
        super(AddAttribute, self).serialize(imports)
        return "AddAttribute(entity_name=%r, attr=%s%s)" % \
               (self.entity_name, self.attr.serialize(imports), '' if not self.sql else ', sql=%r' % self.sql)


class AddRelation(BaseOperation):
    def __init__(self, entity1_name, attr1, entity2_name, attr2, sql=None):
        self.entity1_name = entity1_name
        self.attr1 = attr1
        self.entity2_name = entity2_name
        self.attr2 = attr2
        self.sql = sql

    def apply(self, vdb):
        attr1 = self.attr1
        attr2 = self.attr2
        if attr1.py_type != self.entity2_name:
            throw(core.MigrationError, 'Inconsistent relation attribute type %r' % attr1.name)
        if attr2.py_type != self.entity1_name:
            throw(core.MigrationError, 'Inconsistent relation attribute type %r' % attr2.name)
        entity1 = vdb.entities.get(self.entity1_name)
        entity2 = vdb.entities.get(self.entity2_name)
        if not entity1:
            throw(core.MigrationError, "Entity %r was not found" % self.entity1_name)
        if not entity2:
            throw(core.MigrationError, "Entity %r was not found" % self.entity2_name)

        if isinstance(attr1, v.Required):
            if entity1.name not in vdb.new_entities:
                if attr1.provided.initial is None:
                    throw(core.MigrationError,
                          'initial option should be specified in case of adding the Required attribute')
                attr1.initial = attr1.provided.initial

        if isinstance(attr2, v.Required):
            if entity2.name not in vdb.new_entities:
                if attr2.provided.initial is None:
                    throw(core.MigrationError,
                          'initial option should be specified in case of adding the Required attribute')
                attr2.initial = attr2.provided.initial

        if attr1.name in entity1.new_attrs:
            throw(core.MigrationError, 'Attribute %r is already defined' % attr1.name)
        if attr2.name in entity2.new_attrs:
            throw(core.MigrationError, 'Attribute %r is already defined' % attr2.name)

        attr1.entity = entity1
        attr2.entity = entity2
        entity1.add_attr(attr1)
        entity2.add_attr(attr2)
        attr1.reverse = attr2
        attr2.reverse = attr1
        attr1.resolve_cascade(attr2)
        attr2.resolve_cascade(attr1)

        if not vdb.vdb_only:
            if self.sql:
                vdb.schema.add_sql(self.sql)
            else:
                self.apply_to_schema(vdb, attr1, attr2)

    @staticmethod
    def apply_to_schema(vdb, attr1, attr2):
        AddAttribute.apply_to_schema(vdb, attr1)
        AddAttribute.apply_to_schema(vdb, attr2)

    def serialize(self, imports):
        super(AddRelation, self).serialize(imports)
        return 'AddRelation(entity1_name=%r, attr1=%s, entity2_name=%r, attr2=%s%s)' % (
            self.entity1_name, self.attr1.serialize(imports, with_reverse=False),
            self.entity2_name, self.attr2.serialize(imports, with_reverse=False),
            '' if not self.sql else ', sql=%r' % self.sql
        )


class AddSymmetricRelation(BaseOperation):
    def __init__(self, entity_name, attr, sql=None):
        self.entity_name = entity_name
        self.attr = attr
        self.sql = sql

    def apply(self, vdb):
        entity = vdb.entities.get(self.entity_name)
        if entity is None:
            throw(core.MigrationError, 'Entity %r was not found' % self.entity_name)
        attr = self.attr
        attr.entity = entity
        entity.add_attr(attr)
        attr.symmetric = True
        attr.reverse = self.attr
        attr.resolve_cascade(attr)

        if not vdb.vdb_only:
            if self.sql:
                vdb.schema.add_sql(self.sql)
            else:
                self.apply_to_schema(vdb, self.attr)

    @staticmethod
    def apply_to_schema(vdb, attr):
        schema = vdb.schema
        table_name = schema.get_m2m_table_name(attr, attr)
        if table_name in schema.tables:
            return
        table = schema.create_m2m_table(attr, attr)
        table.created = False
        schema.tables_to_create.append(table)

    def serialize(self, imports):
        super(AddSymmetricRelation, self).serialize(imports)
        return 'AddSymmetricRelation(entity_name=%r, attr=%s%s)' % (
            self.entity_name, self.attr.serialize(imports), '' if not self.sql else ', sql=%r' % self.sql
        )


class RemoveAttribute(BaseOperation):
    def __init__(self, entity_name, attr_name, sql=None):
        self.entity_name = entity_name
        self.attr_name = attr_name
        self.sql = sql

    def apply(self, vdb):
        entity = vdb.entities.get(self.entity_name)
        if entity is None:
            throw(core.MigrationError, 'Entity %s does not exist' % self.entity_name)
        if self.attr_name not in entity.new_attrs:
            throw(core.MigrationError, 'Attribute %s not found' % self.attr_name)
        attr = entity.new_attrs[self.attr_name]
        if isinstance(attr, v.PrimaryKey) or self.attr_name in entity.primary_key:
            throw(core.MigrationError, 'Cannot change primary key for entity %s' % entity.name)

        entity.remove_attr(self.attr_name)

        if not vdb.vdb_only:
            if self.sql:
                vdb.schema.add_sql(self.sql)
            else:
                self.apply_to_schema(vdb, attr)

    @staticmethod
    def apply_to_schema(vdb, attr):
        vdb.schema.drop_columns(attr.columns)
        if isinstance(attr, v.Set) and isinstance(attr.reverse, v.Set):
            m2m_table_name = vdb.schema.get_m2m_table_name(attr, attr.reverse)
            vdb.schema.drop_table(vdb.schema.tables[m2m_table_name])

    def serialize(self, imports):
        super(RemoveAttribute, self).serialize(imports)
        return 'RemoveAttribute(entity_name=%r, attr_name=%r%s)' % \
               (self.entity_name, self.attr_name, '' if not self.sql else ', sql=%r' % self.sql)


class AddEntity(BaseOperation):
    def __init__(self, entity, sql=None):
        self.entity = entity
        self.sql = sql

    def apply(self, vdb):
        entity = self.entity
        if entity.name in vdb.entities:
            throw(core.MigrationError, 'Entity %s already exists' % entity.name)

        vdb.entities[entity.name] = entity
        vdb.new_entities.add(entity.name)
        entity.db = vdb
        new_attrs = list(entity.new_attrs.values())
        relations_attrs = []
        for attr in new_attrs:
            attr.entity = entity
            if isinstance(attr, v.PrimaryKey):
                if entity.primary_key is not None:
                    if attr.name != entity.primary_key[0]:
                        throw(core.MappingError, 'Cannot specify more than one PrimaryKey attribute')
                else:
                    entity.primary_key = (attr.name,)
            if attr.reverse:
                if isinstance(attr.reverse, tuple):
                    assert attr.reverse == (attr.entity.name, attr.name)
                    attr.reverse = attr
                else:
                    r_entity_name = attr.py_type
                    r_entity = vdb.entities.get(r_entity_name)
                    if r_entity is None:
                        attr.serializable = False
                        continue
                    if isinstance(attr.reverse, basestring):
                        r_attr = r_entity.attrs.get(attr.reverse)
                        if r_attr is not None:
                            r_attr.reverse = attr
                            attr.reverse = r_attr
                        elif entity.name not in vdb.new_entities:
                            # send this attr to AddRelation case
                            entity.new_attrs.pop(attr.name)
                    else:
                        r_attr = attr.reverse
                        if r_entity is entity:
                            continue
                        if r_attr.name not in r_entity.new_attrs:
                            r_entity.add_attr(r_attr)
                            relations_attrs.append(r_attr)
                        r_attr.entity = r_entity
                        r_attr.reverse = attr
                        attr.resolve_cascade(r_attr)
                        r_attr.resolve_cascade(attr)

                if attr.py_type in vdb.entities and attr.py_type in vdb.new_entities:
                    attr.serializable = False

        entity.resolve_inheritance()
        root = entity.get_root()
        if root.subclasses:
            for attr in root.new_attrs.values():
                if isinstance(attr, v.Discriminator):
                    break
            else:
                op = AddAttribute(root.name, v.Discriminator('classtype', str, initial=root.name, column='classtype'))
                op.apply(vdb)

        if not vdb.vdb_only:
            if self.sql:
                vdb.schema.add_sql(self.sql)
            else:
                self.apply_to_schema(vdb, entity, relations_attrs)

    @staticmethod
    def apply_to_schema(vdb, entity, relation_attrs):
        schema = vdb.schema
        if not entity.bases:
            table = schema.create_entity_table(entity)
            schema.tables_to_create.append(table)
            for attr, r_attr in schema.m2m_to_create:
                table_name = schema.get_m2m_table_name(attr, r_attr)
                if table_name in schema.tables:
                    continue
                table = schema.create_m2m_table(attr, r_attr)
                table.created = False
                schema.tables_to_create.append(table)
            schema.m2m_to_create[:] = []
        else:
            for attr in entity.new_attrs.values():
                AddAttribute.apply_to_schema(vdb, attr)
        for attr in relation_attrs:
            AddAttribute.apply_to_schema(vdb, attr)

    def serialize(self, imports):
        super(AddEntity, self).serialize(imports)
        imports['pony.orm.migrations.virtuals'].add('VirtualEntity as Entity')
        return 'AddEntity(%s%s)' % (self.entity.serialize(imports), '' if not self.sql else ', sql=%r' % self.sql)


class RemoveEntity(BaseOperation):
    def __init__(self, entity_name, new_classtype_value=None, sql=None):
        self.entity_name = entity_name
        self.new_classtype_value = new_classtype_value
        self.sql = sql

    def apply(self, vdb):
        entity = vdb.entities.get(self.entity_name)
        if entity is None:
            raise core.MigrationError('Entity %s not found' % self.entity_name)
        if entity.subclasses:
            raise core.MigrationError('Cannot remove entity %s because it has subclasses' % self.entity_name)
        vdb.new_entities.discard(self.entity_name)
        root = entity.get_root()
        removed_attrs = []
        if root is not entity:
            for disc in root.new_attrs.values():
                if isinstance(disc, v.Discriminator):
                    break
            else:
                throw(core.MigrationError, 'Discriminator attribute was not found')

            if len(root.subclasses) == 1 and root.subclasses[0] is entity:
                removed_attrs.append(disc)
                root.remove_attr(disc.name)

            for attr in list(entity.new_attrs.values()):
                removed_attrs.append(attr)
                entity.remove_attr(attr.name)
        else:
            disc = None
            for attr in entity.new_attrs.values():
                if attr.reverse:
                    r_attr = attr.reverse
                    removed_attrs.append(r_attr)
                    r_entity = r_attr.entity
                    r_entity.remove_attr(r_attr.name)

        for base in entity.bases:
            base.subclasses.remove(entity)
        for base in entity.all_bases:
            base.all_subclasses.remove(entity)
        del vdb.entities[self.entity_name]

        if not vdb.vdb_only:
            if self.sql:
                vdb.schema.add_sql(self.sql)
            else:
                self.apply_to_schema(vdb, entity, removed_attrs, disc, self.new_classtype_value)

    @staticmethod
    def apply_to_schema(vdb, entity, removed_attrs, disc, new_classtype_value):
        schema = vdb.schema
        if not entity.bases:
            for attr in removed_attrs:
                RemoveAttribute.apply_to_schema(vdb, attr)
            table_name = schema.get_table_name(entity)
            table = schema.tables[table_name]
            schema.drop_table(table)
        else:
            root = entity.get_root()
            root_table = schema.tables.get(schema.get_table_name(root))
            if not root_table:
                assert False  # attempt to find this case
                return  # assume that parent table was already dropped earlier

            for attr in removed_attrs:
                cols = attr.columns
                schema.drop_columns(cols)

            if root.subclasses:
                if len(entity.bases) > 1 and not new_classtype_value:
                    throw(core.MigrationError,
                          'In order to remove entity %r you should provide `new_classtype_value` '
                          'to cast records to the new type')  # TODO better explanation probably?
                col = disc.columns[0]
                old_value = entity.name
                new_name = new_classtype_value or entity.bases[0].name
                schema.change_discriminator_value(col, old_value, new_name)

    def serialize(self, imports):
        super(RemoveEntity, self).serialize(imports)
        return 'RemoveEntity(%r%s)' % (self.entity_name, '' if not self.sql else ', sql=%r' % self.sql)


class RenameTable(BaseOperation):
    def __init__(self, entity_name, new_table_name, sql=None):
        self.entity_name = entity_name
        self.new_table_name = new_table_name
        self.sql = sql

    def apply(self, vdb):
        vdb.entities[self.entity_name].table_name = self.new_table_name
        if not vdb.vdb_only:
            if self.sql:
                vdb.schema.add_sql(self.sql)
            else:
                table = vdb.entities[self.entity_name].table
                self.apply_to_schema(vdb, table, self.new_table_name)

    @staticmethod
    def apply_to_schema(vdb, table, new_name):
        old_name = table.name
        if isinstance(old_name, tuple):
            old_schema, old_table = old_name
            if isinstance(new_name, tuple):
                new_schema, new_table = new_name
                if old_schema != new_schema:
                    vdb.schema.change_schema(table, new_schema)
                if old_table != new_table:
                    vdb.schema.rename_table(old_name, new_name)
            else:
                vdb.schema.change_schema(table, None)
                if old_table != new_name:
                    vdb.schema.rename_table(old_table, new_name)
        else:
            if isinstance(new_name, tuple):
                new_schema, new_table = new_name
                vdb.schema.change_schema(table, None, new_schema)
                if old_name != new_table:
                    vdb.schema.rename_table(table, new_table)
            else:
                if old_name != new_name:
                    vdb.schema.rename_table(table, new_name)
                else:
                    assert False, (old_name, new_name)

    def serialize(self, imports):
        super(RenameTable, self).serialize(imports)
        return 'RenameTable(entity_name=%r, new_table_name=%r%s)' %\
               (self.entity_name, self.new_table_name, '' if not self.sql else ', sql=%r' % self.sql)


class RenameColumns(BaseOperation):
    def __init__(self, entity_name, attr_name,
                 new_columns_names=NOT_PROVIDED, new_reverse_columns_names=NOT_PROVIDED, sql=None):
        self.entity_name = entity_name
        self.attr_name = attr_name
        self.new_columns_names = new_columns_names
        self.new_reverse_columns_names = new_reverse_columns_names
        self.sql = sql

    def apply(self, vdb):
        entity, attr = self.get_entity_attr(vdb)
        col_names = self.new_columns_names
        r_col_names = self.new_reverse_columns_names
        if attr.reverse is attr:
            if col_names and len(col_names) != len(entity.primary_key):
                throw(core.MigrationError,
                      'new_columns_names for symmetric attribute should have exactly %d values, got %d'
                      % (len(entity.primary_key), len(col_names)))
            if r_col_names and len(r_col_names) != len(entity.primary_key):
                throw(core.MigrationError,
                      'reverse_new_columns_names for symmetric attribute should have exactly %d values, got %d'
                      % (len(entity.primary_key), len(r_col_names)))
        elif attr.columns and len(attr.columns) != len(self.new_columns_names):
            throw(core.MigrationError,
                 'Columns option should contain exactly same number or names as columns for attribute')
        if self.new_columns_names is None:
            attr.provided.kwargs.pop('columns', None)
            attr.provided.kwargs.pop('column', None)
        elif not isinstance(self.new_columns_names, NotProvided):
            if len(self.new_columns_names) == 1:
                attr.provided.kwargs['column'] = self.new_columns_names[0]
            else:
                attr.provided.kwargs['columns'] = self.new_columns_names

        if self.new_reverse_columns_names is None:
            attr.provided.kwargs.pop('reverse_columns', None)
            attr.provided.kwargs.pop('reverse_column', None)
        elif not isinstance(self.new_reverse_columns_names, NotProvided):
            if len(self.new_reverse_columns_names) == 1:
                attr.provided.kwargs['reverse_column'] = self.new_reverse_columns_names[0]
            else:
                attr.provided.kwargs['reverse_columns'] = self.new_reverse_columns_names

        if not vdb.vdb_only:
            schema = vdb.schema
            if self.sql:
                schema.append(self.sql)
            else:
                self.apply_to_schema(vdb, entity, attr, self.new_columns_names, self.new_reverse_columns_names)

    @staticmethod
    def apply_to_schema(vdb, entity, attr, new_names, new_reverse_names):
        schema = vdb.schema
        if attr.reverse and isinstance(attr, v.Set) and isinstance(attr.reverse, v.Set):
            r_attr = attr.reverse
            if new_names is None and new_names is not NOT_PROVIDED:
                new_names = [
                    schema.get_default_m2m_column_name(r_attr.entity.name, col.name)
                    for col in entity.table.primary_key.cols
                ]
            if new_names is not NOT_PROVIDED:
                if len(r_attr.m2m_columns) != len(new_names):
                    throw(core.MigrationError,
                      'Incorrent number of columns. Expected %d, but got %d' % (len(r_attr.m2m_columns), len(new_names)))
                schema.rename_columns(r_attr.m2m_columns, new_names)
            if new_reverse_names is None and new_reverse_names is not NOT_PROVIDED:
                new_reverse_names = [col.name + '_2' for col in r_attr.m2m_columns]
            if new_reverse_names is not NOT_PROVIDED:
                if len(r_attr.reverse_m2m_columns) != len(new_reverse_names):
                    throw(core.MigrationError, 'Incorrent number of columns. Expected %d, but got %d' %
                          (len(r_attr.reverse_m2m_columns), len(new_reverse_names)))
                schema.rename_columns(r_attr.reverse_m2m_columns, new_reverse_names)
        else:
            if len(attr.columns) != len(new_names):
                throw(core.MigrationError, 'Incorrent number of columns. Expected %d, but got %d' %
                      (len(attr.columns), len(new_names)))
            schema.rename_columns(attr.columns, new_names)

    def serialize(self, imports):
        super(RenameColumns, self).serialize(imports)
        attrs = [('entity_name', self.entity_name), ('attr_name', self.attr_name)]
        if self.new_columns_names is not NOT_PROVIDED:
            attrs.append(('new_columns_names', self.new_columns_names))
        if self.new_reverse_columns_names is not NOT_PROVIDED:
            attrs.append(('new_reverse_columns_names', self.new_reverse_columns_names))
        res = ', '.join('%s=%r' % elem for elem in attrs)
        return 'RenameColumns(%s%s)' % (res, '' if not self.sql else ', sql=%r' % self.sql)


class ChangeDiscriminator(BaseOperation):
    def __init__(self, entity_name, new_discriminator, sql=None):
        self.entity_name = entity_name
        self.new_discriminator = new_discriminator
        self.sql = sql

    def apply(self, vdb):
        entity = vdb.entities[self.entity_name]
        entity.discriminator = self.new_discriminator
        disc = None
        for attr in entity.new_attrs.values():
            if isinstance(attr, v.Discriminator):
                disc = attr
                break
        else:
            throw(core.MigrationError, "Discriminator attr was not found")

        if not vdb.vdb_only:
            if self.sql:
                vdb.schema.add_sql(self.sql)
            else:
                self.apply_to_schema(vdb, disc, self.new_discriminator)

    @staticmethod
    def apply_to_schema(vdb, disc, new_value):
        col = disc.columns[0]
        vdb.schema.change_discriminator_value(col, disc.default, new_value)

    def serialize(self, imports):
        super(ChangeDiscriminator, self).serialize(imports)
        return 'ChangeDiscriminator(entity_name=%r, new_discriminator=%r%s)' % \
               (self.entity_name, self.new_discriminator, '' if not self.sql else ', sql=%r' % self.sql)


class ChangeAttributeClass(BaseOperation):
    def __init__(self, entity_name, attr_name, new_class, sql=None):
        self.entity_name = entity_name
        self.attr_name = attr_name
        self.new_class = new_class
        self.sql = sql

    def apply(self, vdb):
        new_class = self.new_class
        if isinstance(new_class, basestring):
            if new_class == 'Optional':
                new_class = v.Optional
            elif new_class == 'Required':
                new_class = v.Required
        if new_class not in (v.Optional, v.Required):
            throw(core.MigrationError, 'Attribute class can only be changed to Optional or Required. Got: %s' % new_class.__name__)

        entity, attr = self.get_entity_attr(vdb)
        if isinstance(attr, new_class):
            throw(core.MigrationError, 'Cannot change attribute of type %s to the same type' % new_class.__name__)

        if attr.reverse and new_class is v.Required and attr.reverse.__class__ is v.Required:
            throw(core.MigrationError, 'Cannot change %s.%s class to Required because it has Required relation with %s.%s' %
                  (entity.name, attr.name, attr.reverse.entity.name, attr.reverse.name))

        attr.__class__ = new_class

        is_string = type(attr.py_type) is type and issubclass(attr.py_type, basestring)
        type_has_empty_value = is_string or hasattr(attr.py_type, 'default_empty_value')
        attr.nullable = not isinstance(attr, v.Required) and not type_has_empty_value

        if not vdb.vdb_only:
            if self.sql:
                vdb.schema.add_sql(self.sql)
            else:
                self.apply_to_schema(vdb, attr)
        else:
            if self.new_class is v.Required:
                print("Warning: Changing attribute from Optional to Required assumes that you do not have empty values"
                      " for attribute `%s.%s`" % (entity.name, attr.name))

    @staticmethod
    def apply_to_schema(vdb, attr):
        def need_to_move(attr):
            if not attr.reverse:
                return False
            r_attr = attr.reverse
            if isinstance(r_attr, v.Set):
                return False
            col_provided = lambda a: a.provided.kwargs.get('column') or a.provided.kwargs.get('columns')
            if col_provided(attr) or col_provided(r_attr):
                return False
            new_class = attr.__class__
            reverse_class = r_attr.__class__
            if new_class is v.Required:
                if reverse_class is v.Optional:
                    return bool(r_attr.columns)
                else:
                    assert False, 'Required - Required relation is not possible'
            else:
                assert reverse_class is not v.Required, 'Required - Required relation is not possible'
                return r_attr == min(attr, r_attr, key=lambda a: (a.name, a.entity.name))

        schema = vdb.schema
        if need_to_move(attr):
            schema.move_column_with_data(attr)

        for col in attr.columns:
            nullable = attr.nullable or len(attr.entity.bases) != 0
            schema.change_nullable(col, nullable)

    def serialize(self, imports):
        super(ChangeAttributeClass, self).serialize(imports)
        return 'ChangeAttributeClass(entity_name=%r, attr_name=%r, new_class=%r%s)' \
               % (self.entity_name, self.attr_name, self.new_class,
                  '' if not self.sql else ', sql=%r' % self.sql)


class DropCompositeKey(BaseOperation):
    def __init__(self, entity_name, attr_names, sql=None):
        self.entity_name = entity_name
        self.attr_names = attr_names
        self.sql = sql

    def apply(self, vdb):
        entity = vdb.entities[self.entity_name]
        attrs = [entity.get_attr(attrname) for attrname in self.attr_names]
        entity.composite_keys.remove(self.attr_names)

        if not vdb.vdb_only:
            if self.sql:
                vdb.schema.add_sql(self.sql)
            else:
                self.apply_to_schema(vdb, attrs)

    @staticmethod
    def apply_to_schema(vdb, attrs):
        schema = vdb.schema
        cols = []
        for attr in attrs:
            cols.extend(attr.columns)
        table = cols[0].table
        try:
            schema.drop_composite_key(table, cols)
        except core.MigrationError:
            throw(core.MigrationError,
                  'Composite key for attributes %s was not found' % (', '.join(attr.name for attr in attrs)))

    def serialize(self, imports):
        super(DropCompositeKey, self).serialize(imports)
        return 'DropCompositeKey(entity_name=%r, attr_names=%r%s)' % \
               (self.entity_name, self.attr_names, '' if not self.sql else ', sql=%r' % self.sql)


class AddCompositeKey(BaseOperation):
    def __init__(self, entity_name, attr_names, sql=None):
        self.entity_name = entity_name
        self.attr_names = attr_names
        self.sql = sql

    def apply(self, vdb):
        entity = vdb.entities[self.entity_name]
        if not isinstance(self.attr_names, tuple):
            throw(core.MappingError, 'Composite key should be defined as tuple')

        attrs = []
        for attr_name in self.attr_names:
            attrs.append(entity.get_attr(attr_name))

        entity.composite_keys.append(self.attr_names)

        if not vdb.vdb_only:
            if self.sql:
                vdb.schema.add_sql(self.sql)
            else:
                self.apply_to_schema(vdb, attrs)

    @staticmethod
    def apply_to_schema(vdb, attrs):
        cols = []
        for attr in attrs:
            cols.extend(attr.columns)
        table = cols[0].table
        vdb.schema.add_composite_key(table, cols)

    def serialize(self, imports):
        super(AddCompositeKey, self).serialize(imports)
        return 'AddCompositeKey(entity_name=%r, attr_names=%r%s)' %\
               (self.entity_name, self.attr_names, '' if not self.sql else ', sql=%r' % self.sql)


class AddCompositeIndex(BaseOperation):
    def __init__(self, entity_name, attr_names, sql=None):
        self.entity_name = entity_name
        self.attr_names = attr_names
        self.sql = sql

    def apply(self, vdb):
        entity = vdb.entities[self.entity_name]
        if not isinstance(self.attr_names, tuple):
            throw(core.MappingError, 'Composite index should be defined as tuple')

        attrs = []
        for attr_name in self.attr_names:
            attrs.append(entity.get_attr(attr_name))

        entity.composite_indexes.append(self.attr_names)

        if not vdb.vdb_only:
            if self.sql:
                vdb.schema.add_sql(self.sql)
            else:
                self.apply_to_schema(vdb, attrs)

    @staticmethod
    def apply_to_schema(vdb, attrs):
        schema = vdb.schema
        cols = []
        for attr in attrs:
            cols.extend(attr.columns)
        prev_index = vdb.schema.get_index(cols)
        if prev_index is not None:
            throw(core.MigrationError,
                  'Composite index for attributes %s already exists' % (', '.join(attr.name for attr in attrs)))
        table = cols[0].table
        index_name = schema.get_default_index_name(table, cols)
        index = schema.index_cls(table, cols, index_name)
        schema.add_index(index)

    def serialize(self, imports):
        super(AddCompositeIndex, self).serialize(imports)
        return 'AddCompositeIndex(entity_name=%r, attr_names=%r%s)' %\
               (self.entity_name, self.attr_names, '' if not self.sql else ', sql=%r' % self.sql)


class DropCompositeIndex(BaseOperation):
    def __init__(self, entity_name, attr_names, sql=None):
        self.entity_name = entity_name
        self.attr_names = attr_names
        self.sql = sql

    def apply(self, vdb):
        entity = vdb.entities[self.entity_name]
        attrs = [entity.get_attr(attrname) for attrname in self.attr_names]
        entity.composite_indexes.remove(self.attr_names)

        if not vdb.vdb_only:
            if self.sql:
                vdb.schema.add_sql(self.sql)
            else:
                self.apply_to_schema(vdb, attrs)

    @staticmethod
    def apply_to_schema(vdb, attrs):
        cols = []
        for attr in attrs:
            cols.extend(attr.columns)
        index = vdb.schema.get_index(cols)
        if index is None:
            throw(core.MigrationError,
                  'Composite index for attributes %s was not found' % (', '.join(attr.name for attr in attrs)))

        vdb.schema.drop_index(index)


    def serialize(self, imports):
        super(DropCompositeIndex, self).serialize(imports)
        return 'DropCompositeIndex(entity_name=%r, attr_names=%r%s)' % \
               (self.entity_name, self.attr_names, '' if not self.sql else ', sql=%r' % self.sql)


class RenameEntity(BaseOperation):
    def __init__(self, entity_name, new_entity_name, sql=None):
        self.entity_name = entity_name
        self.new_entity_name = new_entity_name
        self.sql = sql

    def apply(self, vdb):
        entity = vdb.entities[self.entity_name]
        entity.name = self.new_entity_name

        if self.entity_name in vdb.new_entities:
            vdb.new_entities.remove(self.entity_name)
            vdb.new_entities.add(self.new_entity_name)

        name = self.entity_name
        # release bases
        for e_name, e in vdb.entities.items():
            if name in e.bases:
                e.bases[e.bases.index(name)] = self.new_entity_name

        # reverses
        for attr in entity.new_attrs.values():
            if attr.reverse:
                attr.reverse.py_type = self.new_entity_name

        vdb.entities.pop(self.entity_name)
        vdb.entities[self.new_entity_name] = entity

        if not vdb.vdb_only:
            if self.sql:
                if not entity.table_name:
                    vdb.schema.add_sql(self.sql)
            else:
                self.apply_to_schema(vdb, entity, name)

    @staticmethod
    def apply_to_schema(vdb, entity, old_name):
        schema = vdb.schema
        if entity.bases:
            root = entity.get_root()
            disc_attr = root.get_discriminator_attr()
            col = disc_attr.columns[0]
            schema.change_discriminator_value(col, old_name, entity.name)
            return

        for attr in entity.new_attrs.values():
            if attr.reverse and isinstance(attr, v.Set) and isinstance(attr.reverse, v.Set):
                m2m_table = attr.m2m_table
                new_m2m_table_name = schema.get_m2m_table_name(attr, attr.reverse)
                if m2m_table.name != new_m2m_table_name:
                    vdb.schema.rename_table(m2m_table, new_m2m_table_name)

            if 'column' not in attr.provided.kwargs or 'columns' not in attr.provided.kwargs:
                pk_cols = entity.table.primary_key.cols
                changed = False
                new_names = []
                for i, col in enumerate(attr.m2m_columns):
                    pk_col = pk_cols[i]
                    new_col_name = schema.get_default_m2m_column_name(entity.name, pk_col.name)
                    new_names.append(new_col_name)
                    if col.name != new_col_name:
                        changed = True

                if changed:
                    schema.rename_columns(attr.m2m_columns, new_names)

        table = entity.table
        new_name = schema.get_table_name(entity)
        if table.name != new_name:
            vdb.schema.rename_table(table, new_name)

    def serialize(self, imports):
        super(RenameEntity, self).serialize(imports)
        return 'RenameEntity(entity_name=%r, new_entity_name=%r%s)' % \
               (self.entity_name, self.new_entity_name, '' if not self.sql else ', sql=%r' % self.sql)


class RenameAttribute(BaseOperation):
    def __init__(self, entity_name, attr_name, new_attr_name, sql=None):
        self.entity_name = entity_name
        self.attr_name = attr_name
        self.new_attr_name = new_attr_name
        self.sql = sql

    def apply(self, vdb):
        entity = vdb.entities[self.entity_name]
        if self.attr_name in entity.primary_key:
            i = entity.primary_key.index(self.attr_name)
            entity.primary_key = entity.primary_key[:i] + (self.new_attr_name,) + entity.primary_key[i+1:]
        attr = entity.new_attrs[self.attr_name]
        entity.remove_attr(self.attr_name)
        attr.name = self.new_attr_name
        entity.add_attr(attr)

        for i, ci in enumerate(entity.composite_indexes[:]):
            if self.attr_name in ci:
                pos = ci.index(self.attr_name)
                new_ci = ci[:pos] + (self.new_attr_name,) + ci[pos+1:]
                entity.composite_indexes[i] = new_ci

        for i, ck in enumerate(entity.composite_keys[:]):
            if self.attr_name in ck:
                pos = ck.index(self.attr_name)
                new_ck = ck[:pos] + (self.new_attr_name,) + ck[pos+1:]
                entity.composite_keys[i] = new_ck

        if not vdb.vdb_only:
            if self.sql:
                vdb.schema.add_sql(self.sql)
            else:
                self.apply_to_schema(vdb, attr, self.new_attr_name)

    @staticmethod
    def apply_to_schema(vdb, attr, new_name):
        schema = vdb.schema
        entity = attr.entity
        if isinstance(attr, v.Optional) and isinstance(attr.reverse, v.Optional):
            min_attr = min(attr, attr.reverse, key=lambda a: (a.name, a.entity.name))
            if not min_attr.columns:
                schema.move_column_with_data(min_attr)
                return
        schema.rename_columns_by_attr(attr, new_name)
        # m2m table renaming
        m2m_table = attr.m2m_table
        if m2m_table:
            new_m2m_name = schema.get_default_m2m_table_name(attr, attr.reverse)
            if new_m2m_name != m2m_table.name:
                schema.rename_table(m2m_table.name, new_m2m_name)
        # m2m columns that uses this pk
        if attr.name in entity.primary_key:
            for col in attr.columns:
                for m2m_col in col.m2m_cols_links:
                    new_m2m_col_name = schema.get_default_m2m_column_name(entity.name, col.name)
                    if new_m2m_col_name == m2m_col.name:
                        continue
                    schema.rename_column(m2m_col, new_m2m_col_name)

    def serialize(self, imports):
        super(RenameAttribute, self).serialize(imports)
        return 'RenameAttribute(entity_name=%r, attr_name=%r, new_attr_name=%r%s)' % (
            self.entity_name, self.attr_name, self.new_attr_name, '' if not self.sql else ', sql=%r' % self.sql
        )


class ChangeColumnType(BaseOperation):
    all_options = {'py_type', 'sql_type', 'max_len', 'precision', 'scale', 'size', 'unsigned'}
    kwarg_names = {'max_len', 'precision', 'scale', 'size', 'unsigned'}
    default_values = {'unsigned': False, 'size': 32}

    def __init__(self, entity_name, attr_name, py_type, options, cast_sql=None, sql=None):
        self.entity_name = entity_name
        self.attr_name = attr_name
        self.py_type = py_type
        self.options = options
        self.sql = sql
        self.cast_sql = cast_sql

    def apply(self, vdb):
        entity, attr = self.get_entity_attr(vdb)
        assert not attr.reverse

        attr.py_type = self.py_type
        attr.provided.kwargs = self.options.copy()
        attr.kwargs = {key: val for key, val in self.options.items() if key in self.kwarg_names}
        for key, val in self.options.items():
            if key not in self.kwarg_names:
                setattr(attr, key, val)

        # attr.sql_type = self.new_options.get('sql_type', None)
        attr.args = attr.provided.args = []

        is_string = type(attr.py_type) is type and issubclass(attr.py_type, basestring)
        type_has_empty_value = is_string or hasattr(attr.py_type, 'default_empty_value')
        attr.nullable = not isinstance(attr, v.Required) and not type_has_empty_value

        cast_sql = self.cast_sql
        if cast_sql is None and 'py_type' in self.options:
            cast_sql = vdb.provider.cast_sql

        if not vdb.vdb_only:
            if self.sql:
                vdb.schema.add_sql(self.sql)
            else:
                self.apply_to_schema(vdb, attr, cast_sql)

    @staticmethod
    def apply_to_schema(vdb, attr, cast_sql):
        schema = vdb.schema
        if len(attr.columns) != 1:
            throw(core.MigrationError, 'Incorrect number of columns for ChangeColumnType operation')
        column = attr.columns[0]
        new_converter = schema.provider.get_converter_by_attr(attr)
        new_sql_type = attr.sql_type or new_converter.get_sql_type()
        schema.change_column_type(column, new_sql_type, cast_sql)
        nullable = attr.nullable or len(attr.entity.bases) != 0
        if nullable != column.nullable:
            schema.change_nullable(column, nullable)

    def serialize(self, imports):
        super(ChangeColumnType, self).serialize(imports)
        serialize(self.py_type, imports)
        options = []
        for k, v in self.options.items():
            options.append('%r: %r' % (k, v))
            serialize(v, imports)
        options = ', '.join(options)
        py_type = repr(self.py_type) if not isinstance(self.py_type, type) else self.py_type.__name__
        return 'ChangeColumnType(entity_name=%r, attr_name=%r, py_type=%s, options={%s}%s%s)' % (
            self.entity_name, self.attr_name, py_type, options,
            '' if not self.cast_sql else ', cast_sql=%r' % self.cast_sql,
            '' if not self.sql else ', sql=%r' % self.sql
        )


class ChangeSQLDefault(BaseOperation):
    def __init__(self, entity_name, attr_name, sql_default, sql=None):
        self.entity_name = entity_name
        self.attr_name = attr_name
        self.sql_default = sql_default
        self.sql = sql

    def apply(self, vdb):
        entity, attr = self.get_entity_attr(vdb)
        if attr.sql_default == self.sql_default:
            return
        assert not attr.reverse, 'Cannot change `sql_default` option for a link'
        default = self.sql_default
        # if not default.lower() == 'null' or not isinstance(default, attr.py_type):
        #     throw(core.MigrationError,
        #     'sql_default option for attribute %s.%s should be "null" or type of %s, got %r' %
        #           attr.entity.name, attr.name,  attr.py_type, type(default).__name__
        #     )

        if default is None:
            attr.provided.kwargs.pop('sql_default', None)
        else:
            attr.provided.kwargs['sql_default'] = default

        attr.sql_default = default

        if not vdb.vdb_only:
            if self.sql:
                vdb.schema.add_sql(self.sql)
            else:
                self.apply_to_schema(vdb, attr, self.sql_default)

    @staticmethod
    def apply_to_schema(vdb, attr, sql_default):
        column = attr.columns[0]
        vdb.schema.change_sql_default(column, sql_default)

    def serialize(self, imports):
        super(ChangeSQLDefault, self).serialize(imports)
        return 'ChangeSQLDefault(entity_name=%r, attr_name=%r, sql_default=%r%s)' % (
            self.entity_name, self.attr_name, self.sql_default, '' if not self.sql else ', sql=%r' % self.sql
        )


class ChangeNullable(BaseOperation):
    def __init__(self, entity_name, attr_name, nullable, sql=None):
        self.entity_name = entity_name
        self.attr_name = attr_name
        self.nullable = nullable
        self.sql = sql

    def apply(self, vdb):
        entity, attr = self.get_entity_attr(vdb)
        assert not attr.reverse, 'Cannot set `nullable` option for a link'
        if attr.nullable == self.nullable:
            if 'nullable' not in attr.provided.kwargs:
                attr.provided.kwargs['nullable'] = self.nullable
            return  # user input validation

        if self.nullable:
            attr.nullable = attr.provided.kwargs['nullable'] = True
        else:
            attr.nullable = False
            attr.provided.kwargs.pop('nullable', None)

        if not vdb.vdb_only:
            if self.sql:
                vdb.schema.add_sql(self.sql)
            else:
                self.apply_to_schema(vdb, attr, self.nullable)

    @staticmethod
    def apply_to_schema(vdb, attr, nullable):
        new_value = nullable or len(attr.entity.bases) != 0
        for column in attr.columns:
            vdb.schema.change_nullable(column, new_value)

    def serialize(self, imports):
        super(ChangeNullable, self).serialize(imports)
        return 'ChangeNullable(entity_name=%r, attr_name=%r, nullable=%r%s)' % (
            self.entity_name, self.attr_name, self.nullable, '' if not self.sql else ', sql=%r' % self.sql
        )


class RenameM2MTable(BaseOperation):
    def __init__(self, entity_name, attr_name, new_name, sql=None):
        self.entity_name = entity_name
        self.attr_name = attr_name
        self.new_name = new_name
        self.sql = sql

    def apply(self, vdb):
        entity, attr = self.get_entity_attr(vdb)
        # TODO validate if user created this migration by himself and forgot to rename `table` for reverse attr
        attr.m2m_table_name = self.new_name
        if self.new_name is None:
            attr.provided.kwargs.pop('table', None)
        else:
            attr.provided.kwargs['table'] = self.new_name
        if attr.reverse.m2m_table_name:
            attr.revere.m2m_table_name = self.new_name

        if not vdb.vdb_only:
            if self.sql:
                vdb.schema.add_sql(self.sql)
            else:
                self.apply_to_schema(vdb, attr, self.new_name)

    @staticmethod
    def apply_to_schema(vdb, attr, new_name):
        m2m_table = attr.m2m_table
        if new_name is None:
            new_name = vdb.schema.get_default_m2m_table_name(attr, attr.reverse)
        vdb.schema.rename_table(m2m_table, new_name)

    def serialize(self, imports):
        super(RenameM2MTable, self).serialize(imports)
        return 'RenameM2MTable(entity_name=%r, attr_name=%r, new_name=%r%s)' % (
            self.entity_name, self.attr_name, self.new_name, '' if not self.sql else ', sql=%r' % self.sql
        )


class AddUniqueConstraint(BaseOperation):
    def __init__(self, entity_name, attr_name, sql=None):
        self.entity_name = entity_name
        self.attr_name = attr_name
        self.sql = sql

    def apply(self, vdb):
        entity, attr = self.get_entity_attr(vdb)
        unique = True
        attr.unique = unique
        attr.provided.kwargs['unique'] = unique

        if not vdb.vdb_only:
            if self.sql:
                vdb.schema.add_sql(self.sql)
            else:
                self.apply_to_schema(vdb, attr)

    @staticmethod
    def apply_to_schema(vdb, attr):
        cols = attr.columns
        if isinstance(attr, v.Optional) and issubclass(attr.py_type, basestring):
            assert len(cols) == 1
            col = cols[0]
            # attr.nullable = True
            vdb.schema.change_nullable(col, True)
            vdb.schema.update_col_value(col, '', None)
        vdb.schema.add_unique_constraint(cols)

    def serialize(self, imports):
        super(AddUniqueConstraint, self).serialize(imports)
        return 'AddUniqueConstraint(entity_name=%r, attr_name=%r%s)' % (
            self.entity_name, self.attr_name, '' if not self.sql else ', sql=%r' % self.sql
        )


class DropUniqueConstraint(BaseOperation):
    def __init__(self, entity_name, attr_name, sql=None):
        self.entity_name = entity_name
        self.attr_name = attr_name
        self.sql = sql

    def apply(self, vdb):
        entity, attr = self.get_entity_attr(vdb)
        attr.unique = None
        attr.provided.kwargs.pop('unique')

        if not vdb.vdb_only:
            if self.sql:
                vdb.schema.add_sql(self.sql)
            else:
                self.apply_to_schema(vdb, attr)

    @staticmethod
    def apply_to_schema(vdb, attr):
        cols = attr.columns
        if isinstance(attr, v.Optional) and issubclass(attr.py_type, basestring):
            assert len(cols) == 1
            col = cols[0]
            vdb.schema.change_nullable(col, False)
            vdb.schema.update_col_value(col, None, '')
        vdb.schema.drop_unique_constraint(attr.columns)

    def serialize(self, imports):
        super(DropUniqueConstraint, self).serialize(imports)
        return 'DropUniqueConstraint(entity_name=%r, attr_name=%r%s)' % (
            self.entity_name, self.attr_name, '' if not self.sql else ', sql=%r' % self.sql
        )


class AddCheckConstraint(BaseOperation):
    def __init__(self, entity_name, attr_name, check, sql=None):
        self.entity_name = entity_name
        self.attr_name = attr_name
        self.check = check
        self.sql = sql

    def apply(self, vdb):
        entity, attr = self.get_entity_attr(vdb)
        attr.check = attr.provided.kwargs['check'] = self.check
        assert not attr.reverse, 'Cannot add check constraint to a link'

        if not vdb.vdb_only:
            if self.sql:
                vdb.schema.add_sql(self.sql)
            else:
                self.apply_to_schema(vdb, attr, self.check)

    @staticmethod
    def apply_to_schema(vdb, attr, check):
        column = attr.columns[0]
        try:
            vdb.schema.add_check_constraint(column, check)
        except core.MigrationError:
            throw(core.MigrationError, "attribute '%s.%s' already has check constraint" % (attr.entity.name, attr.name))

    def serialize(self, imports):
        super(AddCheckConstraint, self).serialize(imports)
        return 'AddCheckConstraint(entity_name=%r, attr_name=%r, check=%r%s)' % (
            self.entity_name, self.attr_name, self.check, '' if not self.sql else ', sql=%r' % self.sql
        )


class DropCheckConstraint(BaseOperation):
    def __init__(self, entity_name, attr_name, sql=None):
        self.entity_name = entity_name
        self.attr_name = attr_name
        self.sql = sql

    def apply(self, vdb):
        entity, attr = self.get_entity_attr(vdb)
        if not attr.check:
            throw(core.MigrationError, "attribute '%s.%s' doesn't have check constraint" %
                  (self.entity_name, self.attr_name))
        attr.check = None
        attr.provided.kwargs.pop('check', None)

        if not vdb.vdb_only:
            if self.sql:
                vdb.schema.add_sql(self.sql)
            else:
                self.apply_to_schema(vdb, attr)

    @staticmethod
    def apply_to_schema(vdb, attr):
        column = attr.columns[0]
        try:
            vdb.schema.drop_check_constraint(column)
        except core.MigrationError:
            throw(core.MigrationError, 'attribute %r doesn\'t have check constraint' % attr)

    def serialize(self, imports):
        super(DropCheckConstraint, self).serialize(imports)
        return 'DropCheckConstraint(entity_name=%r, attr_name=%r%s)' % (
            self.entity_name, self.attr_name, '' if not self.sql else ', sql=%r' % self.sql
        )


class AddIndex(BaseOperation):
    def __init__(self, entity_name, attr_name, index, sql=None):
        self.entity_name = entity_name
        self.attr_name = attr_name
        self.index = index
        self.sql = sql

    def apply(self, vdb):
        entity, attr = self.get_entity_attr(vdb)
        attr.index = attr.provided.kwargs['index'] = self.index

        if not vdb.vdb_only:
            if self.sql:
                vdb.schema.add_sql(self.sql)
            else:
                self.apply_to_schema(vdb, attr)

    @staticmethod
    def apply_to_schema(vdb, attr):
        schema = vdb.schema
        columns = attr.columns
        prev_index = schema.get_index(columns)
        if prev_index is not None:
            throw(core.MigrationError, 'Index for attribute %r already exists' % attr)
        table = columns[0].table
        index_name = schema.get_index_name(attr, table, columns)
        index = schema.index_cls(table, columns, index_name)
        schema.add_index(index)

    def serialize(self, imports):
        super(AddIndex, self).serialize(imports)
        return 'AddIndex(entity_name=%r, attr_name=%r, index=%r%s)' % (
            self.entity_name, self.attr_name, self.index, '' if not self.sql else ', sql=%r' % self.sql
        )


class RenameIndex(BaseOperation):
    def __init__(self, entity_name, attr_name, index, sql=None):
        self.entity_name = entity_name
        self.attr_name = attr_name
        self.index = index
        self.sql = sql

    def apply(self, vdb):
        entity, attr = self.get_entity_attr(vdb)
        attr.index = attr.provided.kwargs['index'] = self.index

        if not vdb.vdb_only:
            if self.sql:
                vdb.schema.add_sql(self.sql)
            else:
                self.apply_to_schema(vdb, attr)

    @staticmethod
    def apply_to_schema(vdb, attr):
        cols = attr.columns
        index = vdb.schema.get_index(cols)
        if index is None:
            throw(core.MigrationError, 'Index was not found for attribute %r' % attr)
        vdb.schema.rename_index(index, attr.index)

    def serialize(self, imports):
        super(RenameIndex, self).serialize(imports)
        return 'RenameIndex(entity_name=%r, attr_name=%r, index=%r%s)' % (
            self.entity_name, self.attr_name, self.index, '' if not self.sql else ', sql=%r' % self.sql
        )


class DropIndex(BaseOperation):
    def __init__(self, entity_name, attr_name, sql=None):
        self.entity_name = entity_name
        self.attr_name = attr_name
        self.sql = sql

    def apply(self, vdb):
        entity, attr = self.get_entity_attr(vdb)
        attr.index = None
        attr.provided.kwargs.pop('index', None)

        if not vdb.vdb_only:
            if self.sql:
                vdb.schema.add_sql(self.sql)
            else:
                self.apply_to_schema(vdb, attr)

    @staticmethod
    def apply_to_schema(vdb, attr):
        columns = attr.columns
        index = vdb.schema.get_index(columns)
        if index is None:
            throw(core.MigrationError, 'Index was not found for attribute %r' % attr)
            return
        vdb.schema.drop_index(index)

    def serialize(self, imports):
        super(DropIndex, self).serialize(imports)
        return 'DropIndex(entity_name=%r, attr_name=%r%s)' % (
            self.entity_name, self.attr_name, '' if not self.sql else ', sql=%r' % self.sql
        )


class RenameForeignKey(BaseOperation):
    def __init__(self, entity_name, attr_name, fk_name, sql=None):
        self.entity_name = entity_name
        self.attr_name = attr_name
        self.fk_name = fk_name
        self.sql = sql

    def apply(self, vdb):
        entity, attr = self.get_entity_attr(vdb)
        attr.fk_name = self.fk_name
        if self.fk_name is None:
            attr.provided.kwargs.pop('fk_name', None)
        else:
            attr.provided.kwargs['fk_name'] = self.fk_name

        if not vdb.vdb_only:
            if self.sql:
                vdb.schema.add_sql(self.sql)
            else:
                self.apply_to_schema(vdb, attr, self.fk_name)

    @staticmethod
    def apply_to_schema(vdb, attr, new_fk_name):
        cols = attr.columns
        assert cols
        fk = vdb.schema.get_fk(cols)
        if fk is None:
            throw(core.MigrationError, "Foreign key wasn't found for attribute %r" % attr)
            return
        vdb.schema.rename_foreign_key(fk, new_fk_name)

    def serialize(self, imports):
        super(RenameForeignKey, self).serialize(imports)
        return 'RenameForeignKey(entity_name=%r, attr_name=%r, fk_name=%r%s)' % (
            self.entity_name, self.attr_name, self.fk_name, '' if not self.sql else ', sql=%r' % self.sql
        )


class ChangeCascadeDeleteOption(BaseOperation):
    def __init__(self, entity_name, attr_name, cascade_delete, sql=None):
        self.entity_name = entity_name
        self.attr_name = attr_name
        self.cascade_delete = cascade_delete
        self.sql = sql

    def apply(self, vdb):
        entity, attr = self.get_entity_attr(vdb)
        attr.cascade_delete = self.cascade_delete
        if self.cascade_delete is None:
            attr.provided.kwargs.pop('cascade_delete', None)
        else:
            attr.provided.kwargs['cascade_delete'] = self.cascade_delete

        if not vdb.vdb_only:
            if self.sql:
                vdb.schema.add_sql(self.sql)
            else:
                self.apply_to_schema(vdb, attr, self.cascade_delete)

    @staticmethod
    def apply_to_schema(vdb, attr, cascade_delete):
        schema = vdb.schema
        reverse = attr.reverse
        r_cols = reverse.columns or reverse.m2m_columns
        assert r_cols
        r_fk = schema.get_fk(r_cols)
        if r_fk is None:
            throw(core.MigrationError, "Foreign key wasn't found for attribute %r" % attr)
            return

        schema.drop_fk(r_fk)
        new_r_fk_name = schema.get_fk_name(attr, r_fk.table, r_fk.cols_from)
        new_r_fk = schema.fk_cls(r_fk.table, r_fk.table_to, r_fk.cols_from, r_fk.cols_to, new_r_fk_name)
        if cascade_delete:
            new_r_fk.on_delete = 'CASCADE'
        elif isinstance(reverse, v.Optional) and reverse.nullable:
            new_r_fk.on_delete = 'SET NULL'

        schema.add_fk(new_r_fk)

    def serialize(self, imports):
        super(ChangeCascadeDeleteOption, self).serialize(imports)
        return 'ChangeCascadeDeleteOption(entity_name=%r, attr_name=%r, cascade_delete=%r%s)' % (
            self.entity_name, self.attr_name, self.cascade_delete, '' if not self.sql else ', sql=%r' % self.sql
        )


class RawSQL(BaseOperation):
    def __init__(self, sql):
        self.sql = sql

    def apply(self, vdb):
        if not vdb.vdb_only:
            vdb.schema.add_sql(self.sql)

    def serialize(self, imports):
        super(RawSQL, self).serialize(imports)
        return 'RawSQL(sql=%r)' % self.sql
