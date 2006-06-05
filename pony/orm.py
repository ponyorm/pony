# -*- coding: cp1251 -*-

import threading

from sys import _getframe
from itertools import count
from operator import attrgetter

import utils
from utils import FrozenDict, NameMapMixin

__all__ = ('database',
           'PrimaryKey', 'Unique', 'Required', 'Optional',
           'Set', 'List', 'Persistent',
           'Table', 'Column')

################################################################################

_local = threading.local()

class DatabaseError(Exception): pass

def database(*args, **kwargs):
    return DatabaseInfo(*args, **kwargs)

class DatabaseInfo(object):
    _lock = threading.Lock()
    _cache = {}

    def __new__(cls, provider, *args, **kwargs):
        self = object.__new__(cls, provider, *args, **kwargs)
        self._lock.acquire()
        try: return self._cache.setdefault(self, self)
        finally: self._lock.release()

    def __init__(self, provider, *args, **kwargs):
        kwargs = FrozenDict(kwargs)
        self._hash = hash(provider) ^ hash(args) ^ hash(kwargs)
        self._provider_name = provider
        self._args = args
        self._kwargs = kwargs

    provider = property(lambda self: self._provider)
    args     = property(lambda self: self._args)
    kwargs   = property(lambda self: self._kwargs)

    def __eq__(self, other):
        if other.__class__ is not DatabaseInfo: return NotImplemented
        return ((self._hash, self._provider_name, self._args, self._kwargs)
                 == (other._hash, other._provider, other._args, other._kwargs))

    def __hash__(self):
        return hash(self._provider) ^ hash(self._args) ^ hash(self._kwargs)
        
    def open(self):
        if isinstance(self._provider, basestring):
              provider = utils.import_module('pony.dbproviders.'
                                             + self._provider)
        else: provider = self._provider
        if getattr(_local, 'connection', None) is None:
            raise DatabaseError('Connection already opened')
        _local.database = self
        _local.connection = provider.connect(*self._args, **self._kwargs)

    def close(self):
        self._get_connection().close()
        _local.connection = (None, None)
        
    def _get_connection(self):
        if getattr(_local, 'database', None) is not self:
            raise DatabaseError, 'The connection is not opened'
        return _local.connection
    connection = property(_get_connection)

################################################################################

class Table(NameMapMixin):
    __slots__ = 'name', 'primary_key', 'keys', 'foreign_keys'
    def __init__(self, table_name):
        NameMapMixin.__init__(self)
        self.name = table_name
        self.primary_key = None
        self.keys = []
        self.foreign_keys = {} # map(columns -> (table, columns))
    def __setitem__(self, col_name, column):
        assert isinstance(column, Column)
        NameMapMixin.__setitem__(self, col_name, column)
        column._init_(col_name, self)
        fk = column.foreign_key
        if fk: self.set_foreign_key((column,), (fk,))
    def set_primary_key(self, *columns):
        self.primary_key = self._normalize_columns(columns)
    def set_key(self, *columns):
        if columns not in self.keys: self.keys.append(columns)
    def set_foreign_key(self, columns, table2_columns):
        columns = self._normalize_columns(columns)
        table2 = table2_columns[0].table
        table2_columns = table2._normalize_columns(table2_columns)
        self.foreign_keys[columns] = table2_columns
    def _normalize_columns(self, columns):
        columns = list(columns)
        for i, c in enumerate(columns):
            if isinstance(c, basestring): columns[i] = self[c]
            else: assert c.name in self
        return tuple(columns)
    def __repr__(self):
        return '<%s(%s) at 0x%08X>' % (
            self.__class__.__name__, self.name, id(self))

class Column(object):
    __slots__ = ('table', 'name', 'type', 'size', 'prec',
                 'unique', 'not_null', 'foreign_key')
    def __init__(self, type, size=None, prec=None,
                 not_null=None, unique=None, foreign_key=None):
        self.name = None
        self.type = type
        self.size = size
        self.prec = prec
        self.unique = bool(unique)
        self.not_null = bool(unique or not_null)
        self.foreign_key = foreign_key
    def _init_(self, col_name, table):
        self.name = col_name
        self.table = table
    def make_reference(self):
        return Column(self.type, self.size, self.prec,
                      self.not_null, self.unique)
    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, str(self))
    def __str__(self):
        if self.table is None: return '?.%s' % self.name or '?'
        return '%s.%s' % (self.table.name, self.name or '?')

################################################################################

next_id = count().next

class Attribute(object):
    __slots__ = ('_id_', '_init_phase_', 'py_type', 'name', 'owner', 'column',
                 'options', 'reverse', 'column', 'table',
                 '_columns_')
    def __init__(self, py_type, **options):
        self._id_ = next_id()
        self._init_phase_ = 0
        self.py_type = py_type
        self.name = self.owner = self.column = None
        self.options = options
        self.reverse = options.pop('reverse', None)
        self.column = options.pop('column', None)
        self.table = options.pop('table', None)
        self._columns_ = {} # map(table -> column_list)
    def _init_1_(self):
        assert self._init_phase_ == 0
        if (isinstance(self.py_type, type)
            and issubclass(self.py_type, Persistent)): self._init_reverse_()
        self._init_phase_ = 1
        for attr in self.owner.attrs:
            if attr._init_phase_ == 0: break
        else: self.owner.init_2()
    def _init_2_(self):
        assert self._init_phase_ == 1
        table_name = self.table
        is_part_of_pk = self in self.owner.keys[0].attrs
        if is_part_of_pk and table_name is not None: raise TypeError(
                "'table' option cannot be specified for primary key attributes")
        tables = self.owner.tables
        if is_part_of_pk: pass  # add columns to all tables
        elif len(tables) > 1:
            if table_name is None: raise TypeError(
                'Table name not specified for column %s' % self)
            tables = [ t for t in tables if t.name == table_name ]
            if not tables: raise TypeError('Unknown table name: %s'%table_name)
            assert len(tables) == 1
        elif table_name is not None and table_name != tables[0].name:
            raise TypeError("Inconsistent table name for attribute %s" % self)
        for i, table in enumerate(tables): self._add_columns_(table, i==0)
        self._init_phase_ = 2
    def _add_columns_(self, table, set_fk):
        not_null = isinstance(self, Required)
        py_type = self.py_type
        if issubclass(py_type, Persistent):
            class_data = py_type._class_data_
            source_table = class_data.tables[0]
            pk = source_table.primary_key
            assert pk
            prefix = self.column or self.name + '_'
            columns = []
            for source_column in pk:
                if len(pk) == 1: col_name = self.name
                else: col_name = prefix + source_column.name
                column = source_column.make_reference()
                column.not_null = not_null
                columns.append(column)
                self._add_column_(table, col_name, column)
            if set_fk: table.set_foreign_key(columns, pk)
        else:
            col_name = self.column or self.name
            column = Column(self.py_type, not_null=not_null)
            self._add_column_(table, col_name, column)
    def _add_column_(self, table, col_name, column):
        if col_name in table: raise TypeError(
            'Column name %s.%s already in use' % (table.name, col_name))
        table[col_name] = column
        self._columns_.setdefault(table, []).append(column)
    def _init_reverse_(self):
        t = self.py_type
        reverse = self.reverse
        if isinstance(reverse, str):
            reverse = getattr(t, reverse, None)
            if reverse is None: raise AttributeError(
                'Reverse attribute for %s not found' % self)
        if reverse is self: raise TypeError(
            'Attribute %s cannot be reverse attribute for itself' % self)
        if reverse is None:
            candidates = [ attr for attr in t._class_data_.attrs
                                if attr.py_type is self.owner.cls
                                and attr.reverse in (self, self.name, None)
                                and attr is not self ]
            for attr in candidates:
                if attr.options.get('reverse') in (self, self.name):
                    if reverse is not None: raise AttributeError(
                        'Ambiguous reverse attribute for %s' % self)
                    reverse = attr
            if reverse is None:
                if len(candidates) == 0: raise AttributeError(
                    'Reverse attribute for %s not found' % self)
                if len(candidates) > 1: raise AttributeError(
                    'Ambiguous reverse attribute for %s' % self)
                reverse = candidates[0]
        if not isinstance(reverse, Attribute): raise AttributeError(
            'Incorrect reverse attribute type for %s' % self)
        if (reverse.py_type is not self.owner.cls
            or reverse.options.get('reverse') not in (self, self.name, None)):
            raise AttributeError('Inconsistent attributes %s and %s'
                                 % (self, reverse))
        self.reverse = reverse
        assert reverse.reverse in (self, self.name, None)
        reverse.reverse = self
    def __str__(self):
        if self.owner is None: return '?.%s' % (self.name or '?')
        return '%s.%s' % (self.owner.cls.__name__, self.name or '?')
    def __repr__(self):
        return '<%s : %s>' % (self, self.__class__.__name__)
    def __get__(self, obj, type=None):
        return self
    def __set__(self, obj, value):
        pass
    def __delete__(self, obj):
        pass

class Optional(Attribute):
    __slots__ = ()
    
class Required(Attribute):
    __slots__ = ()

class Key(object):
    __slots__ = 'owner', 'attrs', 'is_primary'
    def __init__(self, is_primary, attrs):
        self.owner = None
        self.is_primary = bool(is_primary)
        self.attrs = attrs
        for attr in attrs:
            if isinstance(attr, Collection): raise TypeError(
                'Collection attribute cannot be part of unique key')
    def __repr__(self):
        items = ', '.join(attr.name for attr in self.attrs)
        return '<%s(%s), %s>' % (
            self.__class__.__name__, items, self.is_primary)

class Unique(Required):
    def __new__(cls, *args, **options):
        if not args: raise TypeError('Invalid count of positional arguments')
        attrs = [ a for a in args if isinstance(a, Attribute) ]
        non_attrs = [ a for a in args if not isinstance(a, Attribute) ]
        if attrs and non_attrs: raise TypeError('Invalid arguments')
        if attrs:
            result = key = Key(issubclass(cls, PrimaryKey), args, **options)
        else:
            result = Required.__new__(cls, *args, **options)
            key = Key(issubclass(cls, PrimaryKey), (result,), **options)
        cls_dict = _getframe(1).f_locals
        cls_dict.setdefault('_keys_', []).append(key)
        return result

class PrimaryKey(Unique):
    __slots__ = ()

class Collection(Attribute):
    __slots__ = 'table_def',
    def _init_2_(self):
        assert self._init_phase_ == 1
        reverse = self.reverse
        assert reverse
        if not isinstance(reverse, Collection):
            if self.table is not None: raise TypeError(
                "Table name cannot be specified for one-to-many relationship")
        elif reverse._init_phase_ > 1:
            self._add_many_to_many_table_()
        self._init_phase_ = 2
    def _add_many_to_many_table_(self):
        reverse = self.reverse
        pair = [ self, reverse ]
        pair.sort(key=lambda x: x.owner.cls.__name__)
        if (self.table is not None and reverse.table is not None
            and self.table != reverse.table): raise TypeError(
               'Inconsistent table names for attributes %s and %s: %s and %s'
               % (self, reverse, self.table, reverse.table))
        table_name = (self.table or reverse.table
                      or '_'.join(x.owner.cls.__name__ for x in pair))
        tables = self.owner.local.tables
        if table_name in tables: raise TypeError(
            'Table name %s already in use' % table_name)
        table = Table(table_name)
        tables[table_name] = self.table_def = reverse.table_def = table
        for x in pair: x._add_columns_(table)
        table.set_primary_key(*list(table))
    def _add_columns_(self, table):
        source_table = self.owner.tables[0]
        pk = source_table.primary_key
        assert pk
        prefix = self.column or self.owner.cls.__name__.lower() + '_'
        columns = []
        for source_column in pk:
            if len(pk) == 1: col_name = self.owner.cls.__name__.lower()
            else: col_name = prefix + source_column.name
            column = source_column.make_reference()
            column.not_null = True
            columns.append(column)
            self._add_column_(table, col_name, column)
        table.set_foreign_key(columns, pk)
    
class Set(Collection):
    __slots__ = ()
    
class List(Collection):
    __slots__ = ()

################################################################################

class PersistentMeta(type):
    def __init__(cls, name, bases, dict):
        super(PersistentMeta, cls).__init__(name, bases, dict)
        if 'Persistent' not in globals(): return
        outer_dict = _getframe(1).f_locals
        local = outer_dict.get('_pony_')
        if local is None: local = outer_dict['_pony_'] = LocalData()
        cls._class_init_(local)

class Persistent(object):
    __metaclass__ = PersistentMeta
    @classmethod
    def _class_init_(cls, local):
        class_data = ClassData(cls, local)
        cls._class_data_ = local.classes[cls.__name__] = class_data
        class_data.init_1()

class LocalData(object):
    __slots__ = 'tables', 'classes', 'reverse_attrs'
    def __init__(self):
        self.tables = {}        # map(table_name -> table) 
        self.classes = {}       # map(class_name -> class_data)
        self.reverse_attrs = {} # map(referenced_class_name -> attr_list)

class ClassData(object):
    __slots__ = ('cls', 'local', 'bases', 'root', 'tables', 'attrs', 'keys',
                 'init_phase', 'waiting_classes', 'wait_counter', )
    def __init__(self, cls, local):
        self.cls = cls
        self.local = local
        self.waiting_classes = []
        self.wait_counter = 0
        self.init_phase = 0
        self.tables = []
        self.init_bases_and_root()
    def init_bases_and_root(self):
        cls = self.cls
        self.bases = [ c._class_data_ for c in cls.__bases__
                                      if issubclass(c, Persistent)
                                         and c is not Persistent ]
        if self.bases:
            roots = set(c.root for c in self.bases)
            if len(roots) > 1: raise TypeError(
                'With multiple inheritance of Persistent classes, '
                'inheritance graph must be diamond-like')
            self.root = iter(roots).next()
        else: self.root = self
    def init_1(self):
        # Class just created, and some reference attributes can point
        # to non-existant classes. In this case, attribute initialization
        # is deferred until those classes creation
        assert self.init_phase == 0
        self.init_phase = 1
        self.init_tables()
        self.init_attrs()
    def init_2(self):
        # All related classes created successfully, and reverse attribute
        # has been finded successfully for each reference attribute,
        # but primary keys are not properly initialized yet
        assert self.init_phase == 1
        self.init_phase = 2
        classes = [ t for t in map(attrgetter('py_type'), self.keys[0].attrs)
                      if issubclass(t, Persistent)
                         and t._class_data_.init_phase < 3 ]
        if classes:
            for c in classes: c._class_data_.waiting_classes.append(self)
            self.wait_counter = len(classes)
        else: self.init_3()
    def init_3(self):
        assert self.init_phase == 2
        cls = self.cls
        for attr in self.attrs:
            if not isinstance(attr, Collection) and attr.py_type is not cls:
                attr._init_2_()
        for t in self.tables:
            for i, key in enumerate(self.keys):
                columns = []
                for attr in key.attrs: columns.extend(attr._columns_.get(t, ()))
                if i == 0: t.set_primary_key(*columns)
                else: t.set_key(*columns)
        first = self.tables[0]
        for other in self.tables[1:]:
            other.set_foreign_key(other.primary_key, first.primary_key)
        for attr in self.attrs:
            if attr._init_phase_ < 2 and not isinstance(attr, Collection):
                assert attr.py_type is cls
                attr._init_2_()            
        for attr in self.attrs:
            if attr._init_phase_ < 2:
                assert isinstance(attr, Collection)
                attr._init_2_()
        self.init_phase = 3
        for c in self.waiting_classes:
            assert c.wait_counter > 0
            c.wait_counter -= 1
            if not c.wait_counter: c.init_3()
    def init_tables(self):
        cls = self.cls
        if hasattr(cls, '_table_'):
            if hasattr(cls, '_tables_'): raise TypeError(
                "You can not include both '_table_' and '_tables_' attributes "
                "in persistent class definition")
            table_names = [ cls._table_ ]
        elif hasattr(cls, '_tables_'):
            if isinstance(cls._tables_, basestring): raise TypeError(
                "'_tables_' must be sequence of table names")
            table_names = cls._tables_
        else: table_names = [ cls.__name__ ]
        local = self.local
        for table_name in table_names:
            if not isinstance(table_name, basestring):
                raise TypeError('Table name must be string')
            if table_name in local.tables:
                raise TypeError('Table name %s already in use' % table_name)
            table = Table(table_name)
            self.tables.append(table)
            local.tables[table_name] = table
    def init_attrs(self):
        cls = self.cls
        attrs = self.attrs = []
        for attr_name, x in cls.__dict__.items():
            if isinstance(x, Attribute):
                attrs.append(x)
                x.name = attr_name
                x.owner = self
        attrs.sort(key=attrgetter('_id_'))
        self.keys = getattr(cls, '_keys_', [])
        if hasattr(cls, '_keys_'): del cls._keys_
        for key in self.keys: key.owner = self

        pk_list = [ key for key in self.keys if key.is_primary ]
        if not pk_list:
            if hasattr(cls, 'id'): raise TypeError("Name 'id' alredy in use")
            _keys_ = []
            id = PrimaryKey(int) # this line modifies '_keys_' local variable
            id.name = 'id'
            id.owner = self
            cls.id = id
            self.attrs.insert(0, id)
            self.keys.insert(0, _keys_[0])
        elif len(pk_list) > 1: raise TypeError(
            'Only one primary key may be defined in each data model class')
        else:
            pk = pk_list[0]
            self.keys.remove(pk)
            self.keys.insert(0, pk)

        local = self.local
        reverse_attrs = local.reverse_attrs.get(cls.__name__, [])
        for attr in reverse_attrs:
            attr.py_type = cls
            if attr.reverse is not None:
                reverse_attrs.remove(attr)
                attr._init_1_()
        for attr in attrs:
            if isinstance(attr.py_type, str):
                other_name = attr.py_type
                other_cls_data = local.classes.get(other_name)
                if other_cls_data is None:
                    local.reverse_attrs.setdefault(other_name, []).append(attr)
                    continue
                elif other_cls_data is self and isinstance(attr, Required):
                    raise TypeError('Self-reference may be only optional')
                else: attr.py_type = other_cls_data.cls
            if attr.reverse is not None: attr._init_1_()

        for attr in reverse_attrs: attr._init_1_()
        for attr in attrs:
            if not isinstance(attr.py_type, str) and attr._init_phase_ == 0:
                attr._init_1_()





























