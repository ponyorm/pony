import sys, operator, thread, threading
from operator import attrgeter
from itertools import count, izip

try: import lxml.etree as ET
except ImportError:
    try: from xml.etree import ElementTree as ET
    except ImportError:
        try: import cElementTree as ET
        except ImportError:
            try: from elementtree import ElementTree as ET
            except: pass

def _error_method(self, *args, **keyargs):
    raise TypeError

class TransactionError(Exception): pass
class TableError(Exception): pass
class CacheError(Exception): pass
class ConcurrencyError(Exception): pass
class UniqueConstraintError(Exception): pass

class RowAlreadyLoaded(Exception):
    pass

class _Local(threading.local):
    def __init__(self):
        self.transaction = None

_local = _Local()

class DataSource(object):
    _lock = threading.Lock() # threadsafe access to cache of datasources
    _cache = {}
    def __new__(cls, provider, *args, **kwargs):
        self = object.__new__(cls, provider, *args, **kwargs)
        self._init_(provider, *args, **kwargs)
        cls._lock.acquire()  # because __hash__ and __eq__ are not atomic
        try: return cls._cache.setdefault(self, self)
        finally: cls._lock.release()
    def _init_(self, provider, *args, **kwargs):
        self.provider = provider
        self.args = args
        self.kwargs = kwargs
        kwargs_hash = hash(tuple(sorted(self.iteritems())))
        self.hash = hash(provider) ^ hash(args) ^ kwargs_hash
        self.mapping = Mapping(kwargs.get('mapping', None))
    def __eq__(self, other):
        if other.__class__ is not DataSource: return NotImplemented
        return (self.hash == other.hash and self.provider == other.provider
                and self.args == other.args and self.kwargs == other.kwargs
                and self.mapping == other.mapping)
    def __ne__(self, other):
        return not self.__eq__(other)
    def __hash__(self):
        return self.hash
    def begin(self):
        if _local.transaction is not None: raise TransactionError(
            'Transaction already started in thread %d' % thread.get_ident())
        transaction = Transaction(self.get_connection(), self.mapping)
        _local.transaction = transaction
        return transaction
    def get_connection(self):
        provider = self.provider
        if isinstance(provider, basestring):
            provider = utils.import_module('pony.dbproviders.' + provider)
        return provider.connect(*self.args, **self.kwargs)

def get_transaction():
    return _local.transaction

class Transaction(object):
    def __init__(self, connection, mapping):
        self.connection = connection
        self.mapping = mapping
        self.caches = {} # Table -> TableCache
        self.next_rownum = count().next
    def get_cache(self, table):
        try: return self.caches[table]
        except KeyError:
            # when TableCache is created, the table becomes freezed
            return self.caches.setdefault(table, TableCache(table, self))
    def commit(self):
        pass
    def rollback(self):
        pass
    def detach(self):
        pass
    def attach(self):
        pass

class Mapping(object):
    _cache = {}
    def __new__(cls, filename):
        self = object.__new__(cls, filename)
        self._init_(filename)
        return cls._cache.setdefault(filename, self)
    @classmethod
    def reload(cls, filename):
        self = object.__new__(cls, filename)
        self._init_(filename)
        cls._cache[filename] = self
        return self
    def _init_(self, filename):
        self.lock = threading.RLock()
        self.classes = {}
        self.tables = {}
    def table(self, table_name, *items):
        self.lock.acquire()
        try:
            table = Table(self, table_name)
            for item in items: table.add(item)
            return table
        finally: self.lock.release()

class Table(dict):
    def __init__(self, mapping, name):
        # From application code you must not create tables directly.
        # Use method Mapping.table(name, ...) instead.
        self._freezed = False
        self.mapping = None
        self.name = name
        self.version = 0
        self.columns = []
        self.primary_key = None
        self.indexes = set()
        self.foreign_keys = {}
    def __iter__(self):
        return iter(self.columns)
    def add(self, item):
        self.mapping.lock.acquire()
        try:
            if not self._freezed: self.__error_freezed()
            if isinstance(item, Column): self._add_column(item)
            elif isinstance(item, Index): self._add_index(item)
            elif isinstance(item, ForeignKey): self._add_foreign_key(item)
            else: assert False
        finally: self.mapping.lock.release()
    def _add_column(self, column):
        if column.table is not None: raise TableError(
            'Column %s already assigned to table' % column)
        if column.name in self: raise TableError(
            'Column %s already exists' % self[column.name])
        column.table = self
        column.offset = len(self.columns)
        self.columns.append(column)
        if column.pk:
            if self.primary_key is None: pk_columns = (column,)
            else: pk_columns = self.primary_key.columns + (column,)
            self.add(Index(pk_columns, primary_key=True))
        if column.unique: self._add_index(Index([column]))
        if column.fk: self._add_foreign_key(
            ForeignKey((column,), column.fk.table, (column.fk,)))
    def _add_index(self, index):
        if index.table is not None: raise TableError(
            'Index %s already assigned to table' % index)
        index.columns = self._normalize_columns(index.columns)
        for column in index.columns: column.indexes.add(index)
        index.table = self
        if not index.primary_key: self.indexes.add(index)
        else:
            if self.primary_key: self._remove_index(self.primary_key)
            self.primary_key = index
    def _add_foreign_key(self, foreign_key):
        if foreign_key.table is not None: raise TableError(
            'Foreign key %s already assigned to table' % foreign_key)
        if foreign_key.reference_table.mapping is not self.mapping:
            raise TableError(
                "Foreign key reference table must belongs to the same mapping")
        foreign_key.columns = self._normalize_columns(foreign_key.columns)
        foreign_key.table = self
        fk_list = self.foreign_keys.setdefault(foreign_key.columns, [])
        fk_list.append(foreign_key)
    def remove(self, item):
        self.mapping.lock.acquire()
        try:
            if not self._freezed: self.__error_freezed()
            if isinstance(item, basestring): self._remove_column(self[item])
            elif isinstance(item, Column): self._remove_column(item)
            elif isinstance(item, Index): self._remove_index(item)
            elif isinstance(item, ForeignKey): self._remove_foreign_key(item)
            else: assert False
        finally: self.mapping.lock.release()
    def _remove_column(self, column):
        if isinstance(column, basestring): column = self[column]
        if column.indexes: raise TableError(
            'Cannot delete column which is part of some indexes: %s '
            '(you must delete all such indexes before)' % column)
        if column.foreign_keys: raise TableError(
            'Cannot delete column which is part of some foreign keys: %s '
            '(you must delete all such foreign keys before)' % column)
        self.columns.remove(column)
        del self[column.name]
        column.table = None
        for i, column in enumerate(self.columns): column.offset = i
    def _remove_index(self, index):
        if index.foreign_keys: raise TableError(
            'Cannot delete index with dependent foreign keys: %s '
            '(you must delete all dependent foreign keys before)' % index)
        if self.primary_key is index: self.primary_key = None
        else: self.indexes.remove(index)
        for column in index.columns: column.indexes.remove(index)
        index.columns = tuple(column.name for column in index.columns)
        index.table = None
    def _remove_foreign_key(self, foreign_key):
        fk_list = self.foreign_keys[foreign_key.columns]
        fk_list.remove(foreign_key)
        if not fk_list: del self.foreign_keys[foreign_key.columns]
        foreign_key.index.foreign_keys.remove(foreign_key)
        for column in foreign_key.columns:
            column.foreign_keys.remove(foreign_key)
        foreign_key.columns = tuple(column.name
                                    for column in foreign_key.columns)
        foreign_key.table = None
    def _normalize_columns(self, columns):
        result = []
        for column in columns:
            if isinstance(column, basestring): result.append(self[column])
            else:
                assert column.table is self
                result.append(column)
        return tuple(result)

    def offsets(self, columns):
        return tuple(map(attrgetter('offset'), columns))
    def new_offsets(self, columns):
        return tuple(map(attrgetter('new_offset'), columns))
    def mask(self, columns):
        return reduce(operator.or_, map(attrgetter('mask'), columns), 0)
    def new_mask(self, columns):
        return reduce(operator.or_, map(attrgetter('new_mask'), columns), 0)
    def mask2columns(self, mask):
        bit = 1
        result = []
        for column in enumerate(self.columns):
            if bit & mask: result.append(column)
            bit << 1
        return tuple(result)
    def new_mask2columns(sel, mask):
        bit = 1 << len(self.columns)
        result = []
        for column in enumerate(self.columns):
            if bit & mask: result.append(column)
            bit << 1
        return tuple(result)

    def freeze(self):
        self.mapping.lock.acquire()
        try:
            if self._freezed: return
            if self.primary_key is None: raise TableError(
                'Primary key for table %s is not defined' % self.name)
            self._freezed = True
            offset, new_offset = 0, len(self.columns)
            mask, new_mask = 1, 1 << new_offset
            for column in self.columns:
                column.offset = offset
                column.new_offset = new_offset
                column.mask = mask
                column.new_mask = new_mask
            for index in self.indexes:
                index.offsets = self.offsets(index.columns)
                index.new_offsets = self.new_offsets(index.columns)
                index.mask = self.mask(index.columns)
                index.new_mask = self.new_mask(index.columns)
        finally:
            self.mapping.lock.release()
    def __error_freezed(self):
        raise TableError("Table %s freezed and it's schema can't be modified"
                         % self.name)
    __setitem__ = __delitem__ = __clear__ = __copy__ = __update__ = \
        __fromkeys__ = __setdefault__ = __pop__ = __popitem__ = _error_method
        
class Column(object):
    def __init__(self, name, type, size=None, prec=None,
                 pk=False, unique=False, not_null=False, fk=None):
        self.table = None
        self.name = name
        self.type = type
        self.size = size
        self.prec = prec
        self.pk = pk
        self.not_null = pk or not_null # or unique ???
        self.unique = unique
        self.fk = fk
        self.indexes = set()
        self.foreign_keys = set()
    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, self)
    def __str__(self):
        if self.table is None: return '?.%s' % self.name or '?'
        return '%s.%s' % (self.table.name, self.name or '?')

class Index(object):
    def __init__(self, columns, primary_key=False, unique=True, name=None):
        self.table = None
        self.columns = columns
        self.primary_key=primary_key
        self.unique = primary_key or unique
        self.name = name
        self.foreign_keys = set()
    def __str__(self):
        if self.table is not None:
            self.columns = self.table._normalize_columns(self.columns)
        columns = ', '.join(column.name for column in self.columns)
        return '%s(%s)' % (self.__class__.__name__, columns)
    __repr__ = __str__

class ForeignKey(object):
    possible_actions = 'RESTRICT', 'CASCADE', 'SETNULL'
    def __init__(self, columns, reference_table, foreign_columns,
                 on_delete='RESTRICT', on_update='RESTRICT'):
        self.table = None
        self.columns = tuple(columns)
        self.reference_table = reference_table
        self.foreign_columns=reference_table._normalize_columns(foreign_columns)
        if self.foreign_columns != reference_table.primary_key.columns:
            r = reference_table
            suitable_indexes = [
                index for index in r.indexes
                if index.unique and index.columns == self.foreign_columns ]
            if not suitable_indexes: raise TableError(
                'Suitable indexes not found for foreign key %s' % self)
            self.index = suitable_indexes[0]
            self.index.foreign_keys.add(self)
        if on_delete not in self.possible_actions: raise TableError(
            'Unknown reference action: %s' % self.on_delete)
        if on_update not in self.possible_actions: raise TableError(
            'Unknown reference action: %s' % self.on_update)
        self.on_delete = on_delete
        self.on_update = on_update
    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, self)
    def __str__(self):
        table_name = self.table is not None and self.table.name or ''
        col_names = [ isinstance(column, basestring) and column or column.name
                         for column in self.columns ]
        ref_table_name = self.reference_table.name
        foreign_col_names = [ column.name for column in self.foreign_columns ]
        return '%s[%s] -> %s[%s]' % (
            table_name, ', '.join(col_names),
            ref_table_name, ', '.join(foreign_col_names))

NOTLOADED = object()

TAILSIZE = 4
ROWSTATUS = -1
LMASK = -2
RMASK = -3
WMASK = -4
        
class TableCache(object):
    # From application code you must not create table caches directly.
    # Use method Transaction.get_cache(table) instead.
    def __init__(self, table, transaction):
        table.freeze()
        self.table = table
        self.transaction = transaction
        self.columns = table.columns
        self.row_size = len(self.columns)*2 + TAILSIZE
        self.empty_row = (NOTLOADED,)*len(self.columns)*2 + (0, 0, 0, 'R')
        self.pk_cache = KeyCache(table.primary_key, self)
        self.key_caches = set()
        for index in table.indexes: self.key_caches.add(KeyCache(index, self))
    def load(self, columns, data, columns_to_read=None):
        table = self.table
        assert columns
        for column in columns: assert column.table is table
        if columns_to_read is not None:
            assert columns_to_read
            for column in columns_to_read: assert column.table is table
        else: columns_to_read = columns

        lmask = table.mask(columns)
        rmask = table.mask(columns_to_read)
        assert lmask | rmask == lmask

        col_count = len(self.columns)
        for values in data:
            assert len(columns) == len(values)
            row = list(self.empty_row)
            for column, value in zip(columns, values):
                row[column.offset] = row[column.new_offset] = value
            row[LMASK] = lmask
            row[RMASK] = rmask
            try: self.pk_cache.load(row)
            except RowAlreadyLoaded, e: old_row = e.args[0]
            else:
                for key_cache in self.key_caches: key_cache.load(row)
                continue
            if old_row[:col_count] == row[:col_count]: continue

##          diff_columns = []
##          new_columns = []
##          for column, a, b in izip(self.columns, old_row, row):
##              if a is NOTLOADED:
##                  if b is not NOTLOADED: new_columns.append(column)
##              elif b is not NOTLOADED and a != b: diff_columns.append(column)

            diff_columns = [ column
                for column, a, b in izip(self.columns, old_row, row)
                if a is not NOTLOADED and b is not NOTLOADED and a != b ]

            diff_mask = self.table.mask(diff_columns)
            
            if old_row[RMASK] & diff_mask: raise ConcurrencyError

            for key_cache in self.key_caches: key_cache.merge(old_row, row)
            old_row[LMASK] = old_row[LMASK] & row[LMASK]
            for column in diff_columns:
                old_row[column.offset] = row[column.offset]
    def _purge(self, row):
        assert not row[ROWSTATUS]
        for key_cache in self.key_caches: key_cache.forget(row)
        self.pk_cache.forget(row)
    def forget(self, columns_to_search, search_values):
        pass
    def exists(self, columns_to_search, search_values):
        pass
    def count(self, columns_to_search, search_values):
        pass
    def read(self, columns_to_search, search_values, columns_to_read):
        pass
    def insert(self, columns, values):
        pass
    def update(self, columns_to_search, search_values,
                     columns_to_update, update_values):
        pass
    def delete(self, columns, values):
        pass
    def flush(self):
        pass
    
class KeyCache(object):
    def init(self, index, table_cache):
        self.index = index
        self.table_cache = table_cache
        self.old = {}
        self.new = {}
    def _purge(self, row):
        old_key = tuple(map(row.__getitem__, self.index.offsets))
        new_key = tuple(map(row.__getitem__, self.index.new_offsets))
        del self.old[old_key], self.new[new_key]

class PrimaryKeyCache(KeyCache):
    def load(self, row):
        key = tuple(map(row.__getitem__, self.index.offsets))
        if NOTLOADED in key:
            offset = self.index.offsets[list(key).index(NOTLOADED)]
            column = self.table_cache.columns[offset]
            raise CacheError('Primary key column is not loaded: %s'%column.name)
        if None in key: raise CacheError('NULL value inside primary key')
        x = self.old.setdefault(key, row)
        if x is not row: raise RowAlreadyLoaded, x
        if self.new.setdefault(key, row) is not row: raise UniqueConstraintError

class SecondaryKeyCache(KeyCache):
    def load(self, row):
        key = tuple(map(row.__getitem__, self.index.offsets))
        if NOTLOADED in key: return
        if None in key:
            if key == (None,) * len(key): return
            else: raise CacheError('Partial key')
        x = self.old.setdefault(key, row)
        if x is not key:
            if x[ROWSTATUS]: raise ConcurrencyError
            self.table._purge(x)
            self.old[key] = row
        if self.new.setdefault(key, row) is not row: raise UniqueConstraintError
    def merge(self, row1, row2):
        key1 = tuple(map(row1.__getitem__, self.index.offsets))
        if NOTLOADED not in key1 and None not in key1:
            assert row1 = self.old[key1]
            del self.old[key1]
        self.load(row2)

next_id = count().next

class Attribute(object):
    def __init__(self, py_type, **options):
        self._id_ = next_id()
        self._initialized_ = False
        self.py_type = py_type
        self.name = self.owner = self.column = None
        self.owner = None
        self.options = options
        self.reverse = options.pop('reverse', None)
        self.column = options.pop('column', None)
        self.table = options.pop('table', None)
    def _init_(self):
        assert not self._initialized_
        self._initialized_ = True
        if not isinstance(self.py_type, type): return
        if not issubclass(self.py_type, Persistent): return

        reverse = self.reverse
        if isinstance(reverse, str):
            reverse = self.py_type.__dict__.get(reverse, None)
            if reverse is None: raise AttributeError(
                'Reverse attribute for %s not found' % self)
        if reverse is self: raise TypeError(
            'Attribute %s cannot be reverse attribute for itself' % self)

        if reverse is None:
            candidates = [ a for a in self.py_type._attrs_
                                if a.py_type is self.owner
                                and a.reverse in (self, self.name, None)
                                and a is not self ]
            for a in candidates:
                if a.reverse in (self, self.name):
                    if reverse is not None: raise AttributeError(
                        'Ambiguous reverse attribute for %s' % self)
                    reverse = attr # This is why reverse above may be not None
            if reverse is None:
                if len(candidates) == 0: raise AttributeError(
                    'Reverse attribute for %s not found' % self)
                if len(candidates) > 1: raise AttributeError(
                    'Ambiguous reverse attribute for %s' % self)
                reverse = candidates[0]

        if not isinstance(reverse, Attribute): raise AttributeError(
            'Incorrect reverse attribute type for %s' % self)
        if (reverse.py_type is not self.owner
            or reverse.reverse not in (self, self.name, None)):
            raise AttributeError('Inconsistent attributes %s and %s'
                                 % (self, reverse))
        self.reverse = reverse
        assert reverse.reverse in (self, self.name, None)
        reverse.reverse = self
    def __str__(self):
        owner_name = self.owner is None and '?' or self.owner.cls.__name__
        return '%s.%s' % (owner_name, self.name or '?')
    def __repr__(self):
        return '<%s: %s>' % (self, self.__class__.__name__)
    def __get__(self, obj, type=None):
        return self
    def __set__(self, obj, value):
        pass
    def __delete__(self, obj):
        pass

class Optional(Attribute): pass
class Required(Attribute): pass

class Unique(Required):
    def __new__(cls, *args, **options):
        if not args: raise TypeError('Invalid count of positional arguments')
        attrs = [ a for a in args if isinstance(a, Attribute) ]
        non_attrs = [ a for a in args if not isinstance(a, Attribute) ]
        if attrs and non_attrs: raise TypeError('Invalid arguments')
        if attrs: result = key = tuple(attrs)
        else:
            result = Required.__new__(cls, *args, **options)
            key = (result,)
        cls_dict = sys._getframe(1).f_locals
        if (issubclass(cls, PrimaryKey)
            and cls_dict.setdefault('_primary_key_', key) != key):
            raise TypeError('Only one primary key can be defined in each class')
        cls_dict.setdefault('_keys_', set()).add(key)
        return result

class PrimaryKey(Unique): pass

class Collection(Attribute):
    pass

class Set(Collection): pass
class List(Collection): pass
class Dict(Collection): pass

class PersistentMeta(type):
    def __init__(cls, name, bases, dict):
        super(PersistentMeta, cls).__init__(name, bases, dict)
        if 'Persistent' not in globals(): return
        outer_dict = sys._getframe(1).f_locals
        persistent_class = outer_dict.setdefault('_persistent_classes_', {})
        uninitialized_attrs = outer_dict.setdefault('_uninitialized_attrs_', {})
        cls._class_init_(persistent_classes, uninitialized_attrs)

class Persistent(object):
    __metaclass__ = PersistentMeta
    @classmethod
    def _class_init_(cls, persistent_classes, uninitialized_attrs):
        persistent_classes[cls.__name__] = cls

        cls._bases_ = [ c for c in cls.__bases__
                          if issubclass(c, Persistent) and c is not Persistent ]
        if cls._bases_:
            roots = set(c._root_ for c in self._bases_)
            if len(roots) > 1: raise TypeError(
                'With multiple inheritance of persistent classes, '
                'inheritance graph must be diamond-like')
            cls._root_ = roots.pop()
        else: cls._root_ = self

        cls._attrs_ = []
        base_attrs = {}
        for c in cls._bases_:
            for a in c._attrs_:
                if base_attrs.setdefault(a.name, a) is not a:
                    raise TypeError('Ambiguous attribute name %s' % a.name)
        for name, a in cls.__dict__.items():
            if not isinstance(a, Attribute): continue
            if a.owner is not None:
                raise TypeError('Duplicate use of attribute %s' % value)
            a.name = name
            a.owner = cls
            if name in base_attrs: raise TypeError(
                'Attribute %s hide base attribute %s' % (a, base_attrs[name]))
            cls._attrs_.append(a)
        cls._attrs_.sort(key=attrgetter('_id_'))

        if '_keys_' not in cls.__dict__: cls._keys_ = set()
        for c in cls._bases_:
            for key in c._keys_: cls._keys_.add(key)
        if cls._bases_:
            if '_primary_key_' in cls.__dict__: raise TypeError(
                'Primary key cannot be redefined for derived classes')
            assert hasattr(cls, '_primary_key_')
        elif not hasattr(cls, '_primary_key_'):
            if hasattr(cls, 'id'): raise TypeError("Name 'id' alredy in use")
            # As side effect, next line creates '_keys_' and '_primary_key_'
            # local variables, which is not used
            id = PrimaryKey(int)
            id.name = 'id'
            id.owner = cls
            cls.id = id
            cls._primary_key_ = (id,)
            cls._keys_.add(cls._primary_key_)

        uninitialized_attr_set = uninitialized_attrs.pop(cls.__name__, set())
        for a in uninitialized_attr_set.copy():
            a.py_type = cls
            if a.reverse is not None:
                uninitialized_attr_set.remove(a)
                a._init_()
        for a in cls._attrs_:
            if isinstance(a.py_type, str):
                try: a.py_type = persistent_classes[a.py_type]
                except KeyError:
                    uninitialized_attrs.setdefault(a.py_type, set()).add(a)
                    continue
                if a.py_type is cls and isinstance(a, Required):
                    raise TypeError('Self-reference may be only optional')
            if a.reverse is not None: attr._init_()

        for a in uninitialized_attr_set: attr._init_()
        for a in cls._attrs_:
            if not isinstance(a.py_type, str) and not a._initialized_:
                a._init_()

