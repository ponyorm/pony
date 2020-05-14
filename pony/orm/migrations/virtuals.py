from collections import OrderedDict, defaultdict
from pony.utils import throw
from pony.py23compat import basestring
from pony.orm.ormtypes import Json, Array
from pony.orm.migrations.serialize import serialize


class VirtualDB(object):
    def __init__(self, migrations_dir=None, provider=None):
        self.entities = OrderedDict()
        self.unresolved_links = set()
        self.migrations_dir = migrations_dir or ''
        self.schema = None
        self.provider = provider
        self.vdb_only = True
        self.new_entities = set()

    @classmethod
    def from_db(cls, db):
        self = cls(db.migrations_dir)
        for name in sorted(db.entities, key=lambda x: db.entities[x]._id_):
            self.entities[name] = VirtualEntity.from_entity(self, db.entities[name])

        self.provider = db.provider
        for entity in self.entities.values():
            entity.init(self)

        return self

    def to_db(self, db):
        from pony.orm import core
        def define_entity(entity_name, bases, attrs):
            return core.EntityMeta(entity_name, bases, attrs)

        defined_entities = {}
        classes = {
            Required: core.Required,
            Optional: core.Optional,
            Set: core.Set,
            Discriminator: core.Discriminator,
            PrimaryKey: core.PrimaryKey
        }
        for entity in sorted(self.entities.values(), key=lambda x: x._id_):
            if not entity.bases:
                bases = (db.Entity,)
            else:
                bases = tuple(defined_entities[base.name] for base in entity.bases)

            attrs = OrderedDict()
            for attr in entity.new_attrs.values():
                a_class = classes[type(attr)]
                attrs[attr.name] = a_class(attr.py_type, *attr.provided.args, **attr.provided.kwargs)

            if not entity.bases and not entity.primary_key is None:
                if len(entity.primary_key) > 1:
                    pk_attrs = tuple(attrs[name] for name in entity.primary_key)
                    core.PrimaryKey(*pk_attrs, cls_dict=attrs)

            for comp_key in entity.composite_keys:
                ck_attrs = []
                for name in comp_key:
                    if '.' not in name:
                        ck_attr = name
                    else:
                        ck_e_name, ck_a_name = name.split('.')
                        ck_entity = defined_entities[ck_e_name]
                        ck_attr = ck_a_name
                    ck_attrs.append(ck_attr)
                ck_attrs = tuple(ck_attrs)
                core.composite_key(*ck_attrs, cls_dict=attrs)

            if entity.table_name:
                attrs['_table_'] = entity.table_name

            if entity.discriminator:
                attrs['_discriminator_'] = entity.discriminator

            e = define_entity(entity.name, bases, attrs)
            e.migrations_dir = self.migrations_dir
            defined_entities[entity.name] = e

    def init(self):
        for entity in self.entities.values():
            entity.init(self)

    def validate(db):
        from pony.orm.core import MigrationException
        for entity in db.entities.values():
            for attr in entity.new_attrs.values():
                if attr.reverse:
                    r_entity_name = attr.py_type
                    r_entity = db.entities.get(r_entity_name)
                    if r_entity is None:
                        throw(MigrationException, 'Reverse attr for %r is invalid: entity %s is not found' %
                              (attr, r_entity_name))
                    r_attr = r_entity.get_attr(getattr(attr.reverse, 'name', attr.reverse))
                    if r_attr is None:
                        throw(MigrationException, 'Reverse attr for %r is invalid: attribute %s is not found' %
                              (attr, attr.reverse.name))
                    assert attr.reverse == r_attr


class VirtualEntity(object):
    def __init__(self, name, bases=None, _table_=None, _discriminator_=None, attrs=None, primary_key=None,
                 composite_keys=None, composite_indexes=None, _id_=None):
        if _id_ is None:
            from pony.orm import core
            _id_ = next(core.entity_id_counter)
        self._id_ = _id_
        self.db = None
        self.name = name
        self.bases = bases or []
        self.all_bases = set()
        self.root = None
        self.subclasses = []
        self.all_subclasses = set()
        self.table = None
        self.table_name = _table_
        self.discriminator = _discriminator_
        if isinstance(attrs, OrderedDict):
            self.new_attrs = attrs.copy()
        else:
            self.new_attrs = OrderedDict((attr.name, attr) for attr in attrs)

        self.attrs = self.new_attrs.copy()
        self.all_attrs = []

        self.primary_key = primary_key
        self.composite_keys = composite_keys or []
        self.composite_indexes = composite_indexes or []
        self.real_entity = None

    def clone(self):
        return VirtualEntity(
            self.name,
            [base.name for base in self.bases],
            self.table_name,
            self.discriminator,
            [attr.clone() for attr in self.new_attrs.values()],
            self.primary_key,
            self.composite_keys[:],
            self.composite_indexes[:],
            self._id_
        )

    def __repr__(self):
        return self.name

    def get_root(self):
        if self.root is None:
            if not self.bases:
                self.root = self
            else:
                assert self.db is not None
                self.root = self.db.entities[self.bases[0].name].get_root()
        return self.root

    def get_attr(self, attr_name):
        attr = self.attrs.get(attr_name)
        if attr is None:
            raise AttributeError(attr_name)
        return attr

    def init(self, db):
        self.db = db
        self.resolve_attrs()
        self.resolve_inheritance()
        self.resolve_keys()

    def resolve_attrs(self):
        for attr in self.new_attrs.values():
            attr.init(self, self.db)
            attr.apply_converters(self.db)

    def resolve_inheritance(self):
        for i, base in enumerate(self.bases[:]):
            if isinstance(base, basestring):
                if base not in self.db.entities:
                    throw(TypeError, 'Entity %r is not defined yet' % base)
                base = self.db.entities[base]
                self.bases[i] = base
            base.subclasses.append(self)
            self.all_bases.update(base.all_bases)
            self.all_bases.add(base)
            for attr_name, attr in base.attrs.items():
                if attr_name not in self.attrs:
                    self.attrs[attr_name] = attr
        for base in self.all_bases:
            base.all_subclasses.add(self)
        root = self.get_root()
        for attr in self.new_attrs.values():
            if attr not in root.all_attrs:
                root.all_attrs.append(attr)
        # root.all_attrs.extend(self.new_attrs.values())
        if self.bases:
            self.primary_key = root.primary_key

    def resolve_keys(self):
        if self.primary_key is None:
            if self.bases:
                entity = self.db.entities[self.bases[0].name]
                self.primary_key = entity.primary_key
            else:
                for attr in self.new_attrs.values():
                    if isinstance(attr, PrimaryKey):
                        self.primary_key = (attr.name,)
                        break

    @classmethod
    def from_entity(cls, db, entity):
        name = entity.__name__
        bases = [b.__name__ for b in entity._direct_bases_]
        for base in bases:
            if base not in db.entities:
                throw(ValueError, 'Entity %s has a parent %s that is not defined' % (entity.__name__, base))

        table_name = entity._given_table_name
        discriminator = entity._given_discriminator
        attrs = OrderedDict()
        for attr in entity._new_attrs_:
            v_attr = VirtualAttribute.from_attribute(attr)
            if discriminator and isinstance(v_attr, Discriminator):
                v_attr.default = discriminator
            attrs[v_attr.name] = v_attr
        # attrs = OrderedDict([(attr.name, VirtualAttribute.from_attribute(attr)) for attr in entity._new_attrs_])
        all_attrs = set(attr.name for attr in entity._attrs_)
        composite_pk = tuple(attr.name for attr in entity._pk_attrs_) if entity._pk_is_composite_ else None
        if composite_pk:
            for attrname in composite_pk:
                if attrname not in all_attrs:
                    throw(ValueError, "attribute %s does not exist so it can't be used as primary key" % attrname)
        composite_keys = []
        composite_indexes = []
        for index in entity._indexes_:
            if index.entity is not entity:
                continue
            if len(index.attrs) > 1 and not index.is_pk:
                if index.is_unique:
                    composite_keys.append(tuple(attr.name for attr in index.attrs))
                else:
                    composite_indexes.append(tuple(attr.name for attr in index.attrs))

        ventity = cls(name, bases, table_name, discriminator, attrs, composite_pk, composite_keys, composite_indexes, entity._id_)
        ventity.real_entity = entity
        entity.meta = ventity
        return ventity

    def add_attr(self, attr):
        from pony.orm import core
        attr_name = attr.name
        if attr_name in self.new_attrs:
            raise core.MigrationError('Attribute with name %s already exists in entity %s' % (attr.name, self.name))
        base_attr = self.attrs.get(attr_name)
        for subclass in self.all_subclasses:
            sub_attr = subclass.attrs.get(attr_name)
            if sub_attr is None or sub_attr is base_attr:
                subclass.attrs[attr.name] = attr

        self.new_attrs[attr.name] = attr
        self.attrs[attr.name] = attr
        self.get_root().all_attrs.append(attr)

    def remove_attr(self, attr_name):
        from pony.orm import core
        if attr_name not in self.new_attrs:
            raise core.MigrationError('Attribute %s not found in entity %s' % (attr_name, self.name))
        base_attr = None
        for base in self.bases:
            base_attr = base.attrs.get(attr_name)
            if base_attr is not None: break
        attr = self.new_attrs.pop(attr_name)
        for subclass in self.all_subclasses:
            if subclass.attrs[attr_name] is attr:
                if base_attr is None:
                    del subclass.attrs[attr_name]
                else:
                    subclass.attrs[attr_name] = base_attr
        self.get_root().all_attrs.remove(attr)

    def get_discriminator_attr(self):
        if self.bases:
            return None
        for attr in self.new_attrs.values():
            if isinstance(attr, Discriminator):
                return attr
        else:
            assert False, 'Root entity should have discriminator attr'

    def serialize(self, imports):
        imports['pony.orm.migrations.virtuals'].add('VirtualEntity as Entity')
        ident = '\n        '
        return 'Entity(%r, %s attrs=%s%s%s%s%s%s)' % (
            self.name,
            ('bases=[%s],' % (', '.join('%r' % base.name for base in self.bases))) if self.bases else '',
            '[%s]' % ', '.join([ident + attr.serialize(imports)
                for attr in self.new_attrs.values() if attr.serializable]),
            (',%sprimary_key=%r' % (ident, self.primary_key))
                if self.primary_key and len(self.primary_key) > 1 else '',
            (',%scomposite_keys=%r' % (ident, self.composite_keys)) if self.composite_keys else '',
            (',%scomposite_indexes=%s' % (ident, self.composite_indexes)) if self.composite_indexes else '',
            (',%s_table_=%r' % (ident, self.table_name)) if self.table_name else '',
            (',%s_discriminator_=%r' % (ident, self.discriminator)) if self.discriminator else ''
        )


class Provided(object):
    args = None
    kwargs = None
    initial = None

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class VirtualAttribute(object):
    def __init__(self, name, py_type, *args, **kwargs):
        self.name = name
        self.py_type = py_type
        self.is_string = type(py_type) is type and issubclass(py_type, basestring)
        self.type_has_empty_value = self.is_string or hasattr(py_type, 'default_empty_value')
        self.is_pk = isinstance(self, PrimaryKey)
        self.is_required = isinstance(self, Required) or self.is_pk
        self.entity = None
        self.reverse = kwargs.pop('reverse', None)
        self.converters = []
        self.is_relation = False
        self.columns = []
        self.col_paths = []
        self.m2m_columns = []
        self.reverse_m2m_columns = []
        self.m2m_table = None
        self.symmetric = False
        initial = kwargs.pop('initial', None)

        from pony.orm import core
        if initial and self.reverse:
            throw(core.MappingError, "initial option cannot be used in relation")

        if initial and not (isinstance(initial, self.py_type) or self.is_string and isinstance(initial, basestring)):
            throw(TypeError, 'initial value should be of type %s. Got: %s' %
                  (self.py_type.__name__, type(initial).__name__))

        self.provided = Provided(args=args[:], kwargs=kwargs.copy(), reverse=self.reverse, initial=initial)

        self.auto = kwargs.pop('auto', None)
        if self.auto and not isinstance(self, PrimaryKey):
            throw(TypeError, 'auto option cannot be set for non-PrimaryKey attribute')
        self.nullable = kwargs.pop('nullable', None)
        if self.nullable and not isinstance(self, Optional):
            throw(TypeError, 'nullable option can be set only for Optional attribute')
        if self.nullable is None:
            self.nullable = not self.is_required and not self.type_has_empty_value
        # self.nullable = self.nullable or not self.type_has_empty_value or not self.is_required
        self.sql_type = kwargs.pop('sql_type', None)
        self.unique = kwargs.pop('unique', None)
        if isinstance(self, Optional) and self.is_string and self.unique:
            self.nullable = True  # empty strings can't be unique, but null can
        kwargs.pop('column', None)
        kwargs.pop('columns', None)
        self.cascade_delete = kwargs.pop('cascade_delete', None)
        self.default = kwargs.pop('default', None)
        self.sql_default = kwargs.pop('sql_default', None)
        self.initial = None
        kwargs.pop('lazy', None)
        kwargs.pop('py_check', None)
        kwargs.pop('volatile', None)
        self.index = kwargs.pop('index', None)
        self.reverse_index = kwargs.pop('reverse_index', None)
        self.fk_name = kwargs.pop('fk_name', None)
        self.m2m_table_name = kwargs.pop('table', None)
        self.check = kwargs.pop('check', None)  # todo validate isinstance(check, basestring) and len(cols) == 1
        self.args = args
        self.kwargs = kwargs
        self.real_attr = None

        self.full_reverse = False
        self.serializable = True

        if not isinstance(self, (Required, Set, Optional, Discriminator, PrimaryKey)):
            throw(TypeError, 'VirtualAttribute is abstract base class, '
                             'use Required, Optional, Set, PrimaryKey or Discriminator')

    def clone(self):
        cls = self.__class__
        reverse = self.provided.reverse or self.reverse
        if isinstance(reverse, tuple):
            reverse = reverse[1]
        elif isinstance(reverse, VirtualAttribute):
            reverse = reverse.name
        return cls(
            self.name,
            self.py_type,
            initial=self.provided.initial,
            reverse=reverse,
            *self.provided.args,
            **self.provided.kwargs
        )

    def serialize(self, imports, without_reverse=False):
        imports['pony.orm.migrations.virtuals'].add(self.__class__.__name__)
        options = ''

        if self.provided.args:
            options += ', ' + ', '.join(str(item) for item in self.provided.args)

        if self.provided.kwargs:
            options += ', ' + ', '.join('%s=%r' % (k, v) for k, v in self.provided.kwargs.items())

        if self.provided.initial is not None:
            options += ', initial=%s' % serialize(self.provided.initial, imports)

        if self.reverse and not without_reverse:
            if self.reverse == self:
                options += ', reverse=%r' % self.name
            elif self.full_reverse:
                options += ', reverse=%s' % self.reverse.serialize(imports, without_reverse=True)
            else:
                options += ', reverse=%r' % (self.reverse.name
                                             if not isinstance(self.reverse, basestring) else self.reverse)

        return "%s(%r, %s%s)" % (
            type(self).__name__, self.name, serialize(self.py_type, imports), options
        )

    @classmethod
    def from_attribute(cls, attr):
        from pony.orm import core
        name = attr.name
        if isinstance(attr, core.PrimaryKey):
            attr_class = PrimaryKey
        elif isinstance(attr, core.Discriminator):
            attr_class = Discriminator
        elif isinstance(attr, core.Required):
            attr_class = Required
        elif isinstance(attr, core.Optional):
            attr_class = Optional
        elif isinstance(attr, core.Set):
            attr_class = Set
        else:
            assert False

        py_type = attr.py_type if not isinstance(attr.py_type, core.EntityMeta) else attr.py_type.__name__

        if attr.reverse:
            r_entity_name = py_type
            r_attr_name = attr.reverse.name
            reverse = (r_entity_name, r_attr_name)
        else:
            reverse = attr.given_args['kwargs'].get('reverse', None)
        attr.given_args['kwargs'].pop('reverse', None)

        vattr = attr_class(name, py_type, *attr.given_args['args'], reverse=reverse, **attr.given_args['kwargs'])
        vattr.real_attr = attr
        attr.meta = vattr
        if 'table' in attr.given_args['kwargs']:
            if attr.table is not None:
                vattr.provided.kwargs['table'] = attr.table
            vattr.m2m_table_name = attr.table
        return vattr

    def apply_converters(self, vdb):
        if self.reverse:
            return
        self.converters = [vdb.provider.get_converter_by_attr(self)]

    def init(self, entity, db):
        def resolve_cascade(attr1, attr2):
            if attr1.cascade_delete is None:
                attr1.cascade_delete = isinstance(attr1, Set) and attr2.is_required
            elif attr1.cascade_delete:
                if attr2.cascade_delete: throw(TypeError,
                    "'cascade_delete' option cannot be set for both sides of relationship "
                    "(%s and %s) simultaneously" % (attr1, attr2))
                if isinstance(attr2, Set): throw(TypeError,
                    "'cascade_delete' option cannot be set for attribute %s, "
                    "because reverse attribute %s is collection" % (attr1, attr2))

        if self.entity is not None:
            return  # link that was already resolved

        self.entity = entity
        if isinstance(self.py_type, str):
            self.is_relation = True

            if self.reverse:
                if self.reverse in db.unresolved_links:
                    db.unresolved_links.discard(self.reverse)
                r_attr = None
                if isinstance(self.reverse, tuple):
                    r_entity_name, r_attr_name = self.reverse
                    r_entity = db.entities.get(r_entity_name)
                    if r_entity is not None:
                        r_attr = r_entity.get_attr(r_attr_name)
                elif isinstance(self.reverse, str):
                    r_entity = db.entities.get(self.py_type)
                    r_attr = r_entity.get_attr(self.reverse)
                else:
                    r_entity = db.entities.get(self.py_type)
                    r_attr = self.reverse

                if r_attr is None:
                    if self.py_type not in db.entities:
                        db.unresolved_links.add(self.reverse)
                        return

                    throw(ValueError, 'Reverse attribute for %r should be specified' % self)

                if r_attr.entity is None:
                    r_attr.entity = r_entity

                from pony.orm import core
                if r_attr.is_required and self.is_required:
                    throw(core.MappingError, 'Relation required-to-required is not possible')

                self.reverse = r_attr
                r_attr.reverse = self
                self.symmetric = self is r_attr
                if (self.m2m_table_name or r_attr.m2m_table_name) and not (isinstance(self, Set) or isinstance(r_attr, Set)):
                    throw(TypeError, 'table_name parameter can be only specified for m2m relations')
                if self.m2m_table_name and r_attr.m2m_table_name and self.m2m_table_name != r_attr.m2m_table_name:
                    # throw(ValueError, 'Attributes %r and %r provide different m2m table names' % (self, r_attr))
                    throw(core.MappingError, "Parameter 'table' for %r and %r do not match" % (self, r_attr))
                resolve_cascade(self, r_attr)
                resolve_cascade(r_attr, self)

            else:
                throw(ValueError, 'Reverse attribute should be specified')

        if self.auto and self.py_type is not int:
            throw(TypeError, 'Attribute %r provides option `auto` that can only be specified for int attributes' % self)

    def __repr__(self):
        if self.entity is not None:
            return '%s.%s' % (self.entity.name, self.name)
        return 'Unknown.%s' % self.name


class Required(VirtualAttribute):
    pass


class Optional(VirtualAttribute):
    pass


class Set(VirtualAttribute):
    pass


class PrimaryKey(VirtualAttribute):
    pass


class Discriminator(VirtualAttribute):
    pass


class VirtualIndex(object):
    def __init__(self, attrs, is_pk=False, is_unique=False, name=None):
        assert attrs
        self.attrs = attrs
        self.entity = attrs[0].entity
        self.is_composite = len(attrs) > 1
        self.is_pk = is_pk
        self.is_unique = is_unique
        self.name = name
