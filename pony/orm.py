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
        return (self._hash, self._provider_name, self._args, self._kwargs) \
               == (other._hash, other._provider, other._args, other._kwargs)

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
        if fk: self.set_foreign_key((column,), fk.table, (fk,))
    def set_primary_key(self, *columns):
        self.primary_key = self._normalize_columns(columns)
    def set_foreign_key(self, columns, table2, table2_columns):
        columns = self._normalize_columns(columns)
        table2_columns = table2._normalize_columns(table2_columns)
        self.foreign_keys[columns] = (table2, table2_columns)
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
        if isinstance(self.py_type, type) and \
           issubclass(self.py_type, Persistent): self._init_reverse_()
        self._init_phase_ = 1
        for attr in self.owner._attrs_:
            if attr._init_phase_ == 0: break
        else: self.owner._cls_init_2_()
    def _init_2_(self):
        assert self._init_phase_ == 1
        tables = self.owner._table_defs_
        if len(tables) > 1:
            table_name = self.table
            if table_name is None: raise TypeError(
                'Table name not specified for column %s' % self)
            tables = [ t for t in tables if t.name == table_name ]
            if not tables: raise TypeError(
                'Unknown table name: %s' % table_name)
            assert len(tables) == 1
        table = tables[0]
        self._add_columns_(table)
        self._init_phase_ = 2
    def _add_columns_(self, table):
        not_null = isinstance(self, Required)
        if issubclass(self.py_type, Persistent):
            source_table = self.py_type._table_defs_[0]
            pk = source_table.primary_key
            assert pk
            prefix = self.column or self.name + '_'
            columns = []
            for source_column in pk:
                if len(pk) == 1: col_name = self.name
                else: col_name = prefix + source_column.name
                column = source_column.make_reference()
                columns.append(column)
                self._add_column_(table, col_name, column)
            table.set_foreign_key(columns, source_table, pk)
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
        if reverse is None:
            candidates = [ attr for attr in t._attrs_
                                if attr.py_type is self.owner
                                and attr.reverse in (self, self.name, None)  ]
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
        if reverse.py_type is not self.owner \
           or reverse.options.get('reverse') not in (self, self.name, None):
            raise AttributeError('Inconsistent attributes %s and %s'
                                 % (self, reverse))
        self.reverse = reverse
        assert reverse.reverse in (self, self.name, None)
        reverse.reverse = self
    def __str__(self):
        if self.owner is None: return '?.%s' % (self.name or '?')
        return '%s.%s' % (self.owner.__name__, self.name or '?')
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
    def __init__(self, *args, **options):
        if 'table' in options: raise TypeError(
            "'table' option cannot be specified for PrimaryKey attribute")
        Unique.__init__(self, *args, **options)
    def _init_2_(self):
        assert self._init_phase_ == 1
        for table in self.owner._table_defs_: self._add_columns_(table)
        self._init_phase_ = 2

class Collection(Attribute):
    __slots__ = ()
    def _init_2_(self):
        assert self._init_phase_ == 1
        self._init_phase_ = 2
    
class Set(Collection):
    __slots__ = ()
    
class List(Collection):
    __slots__ = ()

################################################################################

class PonyInfo(object):
    __slots__ = 'tables', 'classes', 'reverse_attrs'
    def __init__(self):
        self.tables = {}        # map(table_name -> table) 
        self.classes = {}       # map(class_name -> class)
        self.reverse_attrs = {} # map(referenced_class_name -> attr_list)

class PersistentMeta(type):
    def __init__(cls, cls_name, bases, cls_dict):
        super(PersistentMeta, cls).__init__(cls_name, bases, dict)
        outer_dict = _getframe(1).f_locals
        info = outer_dict.get('_pony_')
        if info is None:
            info = outer_dict['_pony_'] = PonyInfo()
        if cls_name != 'Persistent': cls._cls_init_1_(info)

class Persistent(object):
    __metaclass__ = PersistentMeta
    @classmethod
    def _cls_init_1_(cls, info):
        # Class just created, and some reference attributes can point
        # to non-existant classes. In this case, attribute initialization
        # is deferred until those classes creation
        info.classes[cls.__name__] = cls
        cls._table_defs_ = []
        cls._waiting_classes_ = []
        cls._wait_counter_ = 0
        cls._init_phase_ = 1
        cls._init_tables_(info)
        cls._init_attrs_(info)
    @classmethod
    def _cls_init_2_(cls):
        # All related classes created successfully, and reverse attribute
        # has been finded successfully for each reference attribute,
        # but primary keys are not properly initialized yet
        assert cls._init_phase_ == 1
        cls._init_phase_ = 2
        classes = [ t for t in map(attrgetter('py_type'), cls._keys_[0].attrs)
                      if issubclass(t, Persistent) and t._init_phase_ < 3 ]
        if classes:
            for c in classes: c._waiting_classes_.append(cls)
            cls._wait_counter_ = len(classes)
        else: cls._cls_init_3_()
    @classmethod
    def _cls_init_3_(cls):
        assert cls._init_phase_ == 2
        for attr in cls._attrs_: attr._init_2_()
        for t in cls._table_defs_:
            pk = []
            for attr in cls._keys_[0].attrs:
                pk.extend(attr._columns_.get(t, ()))
            t.set_primary_key(*pk)
        cls._init_phase_ = 3
        for c in cls._waiting_classes_:
            assert c._wait_counter_ > 0
            c._wait_counter_ -= 1
            if not c._wait_counter_: c._cls_init_3_()
    @classmethod
    def _init_tables_(cls, info):
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
        for table_name in table_names:
            if not isinstance(table_name, basestring):
                raise TypeError('Table name must be string')
            if table_name in info.tables:
                raise TypeError('Table name %s already in use' % table_name)
            table = Table(table_name)
            cls._table_defs_.append(table)
            info.tables[table_name] = table
    @classmethod
    def _init_attrs_(cls, info):
        attrs = cls._attrs_ = []
        for attr_name, x in cls.__dict__.items():
            if isinstance(x, Attribute):
                attrs.append(x)
                x.name = attr_name
                x.owner = cls
        attrs.sort(key = attrgetter('_id_'))

        if not hasattr(cls, '_keys_'): cls._keys_ = []
        for key in cls._keys_: key.owner = cls
        pk_list = [ key for key in cls._keys_ if key.is_primary ]
        if not pk_list:
            if hasattr(cls, 'id'): raise TypeError("Name 'id' alredy in use")
            _keys_ = []
            id = PrimaryKey(int) # this line modifies '_keys_' variable
            id.name = 'id'
            id.owner = cls
            cls.id = id
            cls._attrs_.insert(0, id)
            cls._keys_[0:0] = _keys_
        elif len(pk_list) > 1: raise TypeError(
            'Only one primary key may be defined in each data model class')
        else:
            pk = pk_list[0]
            cls._keys_.remove(pk)
            cls._keys_.insert(0, pk)

        reverse_attrs = info.reverse_attrs.get(cls.__name__, [])
        for attr in reverse_attrs:
            attr.py_type = cls
            if attr.reverse is not None:
                reverse_attrs.remove(attr)
                attr._init_1_()
        for attr in attrs:
            if isinstance(attr.py_type, str):
                other_name = attr.py_type
                other_cls = info.classes.get(other_name)
                if other_cls is None:
                    info.reverse_attrs.setdefault(other_name, []).append(attr)
                    continue
                else: attr.py_type = other_cls
            if attr.reverse is not None: attr._init_1_()

        for attr in reverse_attrs: attr._init_1_()
        for attr in attrs:
            if not isinstance(attr.py_type, str): attr._init_1_()





























