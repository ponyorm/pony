import os
import datetime
from collections import defaultdict
from pony.orm.migrations.operations import *
from pony.orm.migrations.virtuals import PrimaryKey, Discriminator, Set
from pony.py23compat import *
from pony.utils import throw


class Migration(object):
    def __init__(self, folder, dependencies=None):
        self.folder = folder
        self.operations = []
        self.dependencies = dependencies or []
        self.parents = []
        self.name = None
        self.data = False
        self.based_on = []

    @classmethod
    def make(cls, vdb1, vdb2, rename_map=None):
        migration = cls(vdb2.migrations_dir)
        ops = make_difference(vdb1, vdb2, rename_map)
        migration.operations = ops
        return migration

    def save(self, num, name=None, data=False):
        self.name = Migration.generate_name(num, name)
        imports = defaultdict(set)
        text = ['']
        add = text.append

        # dependencies
        if not self.dependencies:
            add('\ndependencies = []\n\n')
        else:
            add('\ndependencies = [\n')
            for dep in self.dependencies:
                add('    %r\n' % dep)
            add(']\n\n')

        if self.based_on:
            add('based_on = [%s\n]\n\n' % ('\n    '.join('%r,' % name for name in self.based_on)))

        # ops
        if not data:
            add('operations = [')
            for op in self.operations:
                add('\n    %s,\n' % op.serialize(imports))
            text[-1] = text[-1].rstrip()
            if text[-1].endswith(','): text[-1] = text[-1][:-1]
            add('%s]\n' % ('' if len(self.operations) == 0 else '\n'))
        else:
            imports['pony.orm'] = {'db_session'}
            self.data = True
            add('@db_session\ndef data_migration(db):\n    pass')

        for k, v in imports.items():
            text[0] += 'from %s import %s\n' % (k, ', '.join(str(i) for i in v))

        result = ''.join(text)

        with open(os.path.join(self.folder, '%s.py' % self.name), 'w') as file:
            file.write(result.strip() + '\n')

    @classmethod
    def load(cls, dir, filename):
        m = cls(dir)
        m.name = filename[:-3]
        with open(os.path.join(dir, filename), 'r') as file:
            src = file.read()
        objects = {}
        exec(src, objects)
        m.dependencies = objects.get('dependencies', None)
        if m.dependencies is None:
            from pony.orm.core import MigrationError
            throw(MigrationError, 'Corrupted migration file %r' % filename)
        m.operations = objects.get('operations', [])
        m.based_on = objects.get('based_on', [])
        data_func = objects.get('data_migration', None)
        if data_func:
            m.data = True
            m.data_migration = data_func
        return m

    @staticmethod
    def generate_name(number, name=None):
        return '%05d_%s' % (
                number,
                name or datetime.datetime.now().isoformat('_').replace(':', '-')[:19]
        )

    def save_applied(migration, db, errors=None):
        from pony.orm import db_session
        vdb = db.vdb
        with db_session:
            schema = vdb.schema
            cache = db._get_cache()
            connection = cache.prepare_connection_for_query_execution()
            cursor = connection.cursor()
            insert_sql = schema.get_migration_insert_sql()
            if schema.provider.dialect in ('SQLite', 'MySQL'):
                data = (migration.name, datetime.datetime.now())
            else:
                data = {'name': migration.name, 'applied': datetime.datetime.now()}
            vdb.provider.execute(cursor, insert_sql, data)
            print('Migration %s has been applied%s' % (migration.name, '' if not errors else '(errors: %d)' % errors))


def make_difference(vdb1, vdb2, rename_map=None):
    if rename_map is None:
        rename_map = {}
    operations = []
    op = models_difference(vdb1, vdb2, rename_map)
    while op is not None:
        operations.append(op)
        op.apply(vdb1)
        op = models_difference(vdb1, vdb2, rename_map)
    # for entity in vdb1.entities.values():
    #     entity.init(vdb1)
    return operations


def pk_is_ready_to_define(entity, vdb):
    if entity.bases:
        for base in entity.bases:
            base_name = base.name if not isinstance(base, basestring) else base
            if base_name not in vdb.entities:
                return False
        return True

    for pk_attr_name in entity.primary_key:
        pk_attr = entity.attrs[pk_attr_name]
        if pk_attr.reverse:
            if pk_attr.py_type not in vdb.entities:
                return False
    return True


def models_difference(vdb1, vdb2, rename_map):
    for entity_name, entity1 in vdb1.entities.items():
        if entity_name not in vdb2.entities:
            if entity_name in rename_map:
                if rename_map[entity_name] in vdb2.entities:
                    new_entity_name = rename_map.pop(entity_name)
                    return RenameEntity(entity_name, new_entity_name)
            return RemoveEntity(entity_name)
    for entity2_name, entity2 in vdb2.entities.items():
        if entity2_name not in vdb1.entities:
            if pk_is_ready_to_define(entity2, vdb1):
                cloned_entity = entity2.clone()
                for attr in cloned_entity.new_attrs.values():
                    if attr.reverse:
                        r_entity_name = attr.py_type
                        r_attr_name = attr.reverse
                        r_entity = vdb2.entities.get(r_entity_name)
                        if r_entity is None:
                            break
                        r_attr = r_entity.attrs.get(r_attr_name)
                        if r_attr is not None:
                            attr.reverse = r_attr
                            r_attr.reverse = attr
                return AddEntity(cloned_entity)
    for entity_name, entity1 in vdb1.entities.items():
        assert entity_name in vdb2.entities
        op = entity_difference(entity1, vdb2.entities[entity_name], vdb1, vdb2, rename_map)
        if op is not None:
            return op


def entity_difference(entity1, entity2, vdb1, vdb2, rename_map):
    if entity1.table_name != entity2.table_name:
        return RenameTable(entity1.name, entity2.table_name)

    if entity1.discriminator != entity2.discriminator:
        return ChangeDiscriminator(entity1.name, entity2.discriminator)

    if entity1.primary_key != entity2.primary_key:
        if len(entity1.primary_key) != len(entity2.primary_key):
            pass
        from pony.orm.core import MigrationError
        throw(MigrationError, 'Cannot change primary key')

    for attr_name, attr1 in entity1.new_attrs.items():
        if attr_name not in entity2.new_attrs:
            a = (entity1.name, attr_name)
            if a in rename_map:
                val = rename_map[a]
                if val[1] in entity2.new_attrs:
                    return RenameAttribute(entity1.name, attr_name, val[1])
                rename_map.pop(val)

            if attr1.reverse and attr1.py_type in vdb1.entities or not attr1.reverse:
                if isinstance(attr1, PrimaryKey) or isinstance(attr1, Discriminator) and entity1.subclasses:
                    throw(NotImplementedError, 'Cannot remove attribute which type is %s' % type(attr1).__name__)

                # for ck in attr1.composite_keys:
                #     return DropCompositeKey(entity1.name, ck)

            return RemoveAttribute(entity1.name, attr_name)
        else:
            if not (attr1.reverse or attr1.converters):
                attr1.apply_converters(vdb1)  # converters should be added to convert args to kwargs
            op = attr_difference(attr1, entity2.new_attrs[attr_name])
            if op is not None:
                return op

    for attr_name, attr2 in entity2.new_attrs.items():
        if attr_name not in entity1.new_attrs:
            if isinstance(attr2, Discriminator):
                if attr2.provided.initial is None:
                    attr2.provided.initial = entity2.name
            if attr2.reverse:
                reverse = attr2.reverse
                r_entity_name = reverse.entity.name
                prev_r_entity = vdb1.entities.get(r_entity_name)
                if reverse.name not in prev_r_entity.attrs:
                    if attr2 is reverse:
                        return AddSymmetricRelation(entity2.name, attr2)
                    else:
                        return AddRelation(entity2.name, attr2, r_entity_name, reverse)
            else:
                return AddAttribute(entity2.name, attr2.clone())

    for ck in entity1.composite_keys:
        if ck not in entity2.composite_keys:
            return DropCompositeKey(entity2.name, ck)

    for ci in entity1.composite_indexes:
        if ci not in entity2.composite_indexes:
            return DropCompositeIndex(entity2.name, ci)

    for ck in entity2.composite_keys:
        if ck not in entity1.composite_keys:
            return AddCompositeKey(entity1.name, ck)

    for ci in entity2.composite_indexes:
        if ci not in entity1.composite_indexes:
            return AddCompositeIndex(entity1.name, ci)

def attr_difference(attr1, attr2):
    from pony.orm.core import MigrationError
    non_changeable_types = (PrimaryKey, Set, Discriminator)
    if type(attr1) != type(attr2):
        if type(attr1) in non_changeable_types:
            throw(MigrationError, 'Attribute of type %s cannot be changed' % type(attr1).__name__)
        if type(attr2) in non_changeable_types:
            throw(MigrationError, 'Attribute cannot be changed to type %s' % type(attr2).__name__)

        return ChangeAttributeClass(attr1.entity.name, attr1.name, type(attr2).__name__)

    # if attr1.py_type != attr2.py_type:
    #     return ChangeAttributeType(attr1.entity.name, attr1.name, attr2.py_type)

    # if attr1.provided.args != attr2.provided.args:
    #     throw(NotImplementedError, 'ChangeAttributeOptions(args)')
    #     return 'ChangeAttributeOptions'

    useless_options = {'lazy', 'py_check', 'volatile', 'default'}
    for option in useless_options:
        attr1.provided.kwargs.pop(option, None)
        attr2.provided.kwargs.pop(option, None)

    if attr1.provided.kwargs != attr2.provided.kwargs or attr1.py_type != attr2.py_type:
        kw1 = attr1.provided.kwargs
        kw2 = attr2.provided.kwargs

        def option_diff(option, def_value=None, cast_to=None):
            a = kw1.get(option, def_value)
            b = kw2.get(option, def_value)
            if cast_to:
                a = cast_to(a)
                b = cast_to(b)
            return a != b

        if option_diff('column') and option_diff('reverse_column'):
            new_name = kw2.get('column')
            if new_name is not None:
                new_name = [new_name]
            new_r_name = kw2.get('reverse_column')
            if new_r_name is not None:
                new_r_name = [new_r_name]
            return RenameColumns(attr1.entity.name, attr1.name, new_name, new_r_name)
        elif option_diff('column'):
            new_name = kw2.get('column')
            if new_name is not None:
                new_name = [new_name]
            return RenameColumns(attr1.entity.name, attr1.name, new_name)
        elif option_diff('reverse_column'):
            new_r_name = kw2.get('reverse_column')
            if new_r_name is not None:
                new_r_name = [new_r_name]
            return RenameColumns(attr1.entity.name, attr1.name, new_reverse_columns_names=new_r_name)
        elif option_diff('columns', (), list) and option_diff('reverse_columns', (), list):
            return RenameColumns(attr1.entity.name, attr1.name, kw2.get('columns'), kw2.get('reverse_columns'))
        elif option_diff('columns', (), list):
            return RenameColumns(attr1.entity.name, attr1.name, new_columns_names=kw2.get('columns'))
        elif option_diff('reverse_columns', (), list):
            return RenameColumns(attr1.entity.name, attr1.name, new_reverse_columns_names=kw2.get('reverse_columns'))
        elif attr1.py_type != attr2.py_type:
            cast_sql = None
            if attr2.columns:
                col = attr2.columns[0]
                provider = col.provider
                cast_sql = provider.cast_sql.format(colname='{colname}', sql_type=col.converter.get_sql_type())
            new_options = {'py_type': attr2.py_type}
            if option_diff('max_len') or option_diff('precision') or option_diff('scale') or \
                option_diff('size') or option_diff('unsigned') or option_diff('sql_type'):
                new_options.update(kw2)
            return ChangeColumnType(attr1.entity.name, attr1.name, new_options, cast_sql=cast_sql)
        elif option_diff('max_len') or option_diff('precision') or option_diff('scale') or \
                option_diff('size') or option_diff('unsigned') or option_diff('sql_type'):
            new_options = kw2.copy()
            return ChangeColumnType(attr1.entity.name, attr1.name, new_options)
        elif option_diff('sql_default'):
            return ChangeSQLDefault(attr1.entity.name, attr1.name, kw2.get('sql_default', None))
        elif option_diff('nullable'):
            return ChangeNullable(attr1.entity.name, attr1.name, attr2.nullable)
        elif option_diff('table'):
            reverse = attr1.reverse
            if reverse.m2m_table_name:
                key_attr = min(attr1, reverse, key=lambda a: (a.name, a.entity.name))
                if attr1.name is key_attr:
                    return RenameM2MTable(attr1.entity.name, attr1.name, kw2.get('table', None))
            else:
                return RenameM2MTable(attr1.entity.name, attr1.name, kw2.get('table', None))
        elif option_diff('unique', cast_to=bool):
            unq = kw2.get('unique')
            if unq:
                return AddUniqueConstraint(attr1.entity.name, attr1.name)
            else:
                return DropUniqueConstraint(attr1.entity.name, attr1.name)
        elif option_diff('check'):
            chk = kw2.get('check')
            if chk:
                return AddCheckConstraint(attr1.entity.name, attr1.name, chk)
            else:
                return DropCheckConstraint(attr1.entity.name, attr1.name)
        elif option_diff('index', bool):
            new_index = kw2.get('index', None)
            old_index = kw1.get('index', None)
            if None not in (new_index, old_index):
                return RenameIndex(attr1.entity.name, attr1.name, new_index)
            elif new_index is not None:
                return AddIndex(attr1.entity.name, attr1.name, new_index)
            else:
                return DropIndex(attr1.entity.name, attr1.name)
        elif option_diff('fk_name'):
            return RenameForeignKey(attr1.entity.name, attr1.name, kw2.get('fk_name', None))
        elif option_diff('cascade_delete'):
            return ChangeCascadeDeleteOption(attr1.entity.name, attr1.name, kw2.get('cascade_delete', None))


        throw(NotImplementedError, 'ChangeAttributeOptions(kw1=%r, kw2=%r)' % (kw1, kw2))
        # return 'ChangeAttributeOptions'
