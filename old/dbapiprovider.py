# -*- coding: cp1251 -*-

import datetime

from pony import utils

__all__ = 'Param', 'SimpleSelect'

class Value(object):
    __slots__ = 'value'
    def __init__(self, value):
        self.value = value
    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, repr(self.value))

class Param(object):
    __slots__ = 'key',
    def __init__(self, key):
        self.key = key
    def __hash__(self):
        return hash(key)
    def __cmp__(self, other):
        if other.__class__ is not Param: return NotImplemented
        return cmp(self.key, other.key)
    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, repr(self.key))

class BaseQuery(object):
    _section_delimiter = '\n'
    def _quote_name(self, name):
        return '"%s"' % name.replace('"', '""')

class SimpleSelect(BaseQuery):
    def __init__(self, con, query_type, columns, table, filter, order, range):
        self.connection = connection # This is a subclass of DBAPIConection
        self.type = type
        self.columns = columns
        self.table = table
        self.order = order
        self.range = range
        if __debug__: self._check()
        self.sql, self.params = self._sql()
    def _check(self):
        assert self.type in ('ALLROWS', 'DISTINCTROWS', 'ROWSCOUNT', 'EXISTS')
        if self.type in ('ALLROWS', 'DISTINCTROWS'):
            assert self.columns
        else:
            assert not self.columns and not self.order and self.range is None
        for col_name in self.columns:
            assert isinstance(col_name, basestring)
        for col_name, direction in self.order:
            assert isinstance(col_name, basestring)
            assert direction in ('asc', 'desc')
        table_name, db_name, owner = self.table
        assert isinstance(table_name, basestring)
        assert db_name is None or isinstance(db_name, basestring)
        assert owner is None or isinstance(owner, basestring)
        for or_data in self.filter:
            for and_data in or_data:
                op, col_name = and_data[:2]
                assert isinstance(col_name, basestring)
                if op in ('is null', 'is not null'):
                    assert len(and_data) == 2
                else:
                    assert len(and_data) == 3
                    x = and_data[2]
                    assert op in ('=', '<>', '<', '<=', '>', '>=',
                                  'like', 'not like', 'in', 'not in')
                    if x is None: assert op in ('=', '<>')
                    if op in ('in', 'not in'):
                        assert not hasattr(x, 'key')
                        for item in x:
                            assert isinstance(x, (int, long, basestring,
                                datetime.date, datetime.datetime))
        if self.range is not None:
            limit, offset = self.range
            assert isinstance(limit, Param) or \
                   isinstance(limit, (int, long)) and limit > 0
            assert isinstance(offset, Param) or \
                   isinstance(offset, (int, long)) and offset >= 0
    def _sql(self):
        result = utils.join(self._section_delimiter, [
            self._select(),
            self._from(),
            self._where(),
            self._order_by(),
            self._limit(),
            ])
        sql = ''.join(result)
        params = [ x for x in result if isinstance(x, Params) ]
        return sql, params
    def _select(self):
        result = [ 'select ' ]
        if self.type == 'ROWSCOUNT': result.append('count(*)')
        elif self.type == 'EXISTS':  result.append('*')
        else: result.append(', '.join(map(self._quote_name, self.columns)))
        return result
    def _from(self):
        result = [ 'from ' ]
        table_name, db_name, owner = table
        if db_name is not None:
            result.append(self._quote_name(db_name))
            result.append('.')
        if owner is not None:
            result.append(self._quote_name(owner))
            result.append('.')
        result.append(self._quote_name(table_name))
        return result
    def _where(self):
        if not self.filter: return []
        return [ 'where ' ] + \
               utils.join(' or ', map(self._or, self.filter))
    def _or(self, or_data):
        return utils.join(' and ', map(self._and, or_data))
    def _and(self, and_data):
        op, col_name = and_data[0]
        col_name = self._quote_name(col_name)
        if op in ('is null', 'is not null'):
            return [ col_name, ' ', op ]
        x = self._value(and_data[2])
        if op in ('in', 'not in'):
            return [ col_name, ' ', op, ' (' ] + \
                   utils.join(', ', map(self._value, x)) + [ ')' ]
        if x is None:
            if op == '=': return [ col_name, ' is null' ]
            elif op == '<>': return [ col_name, ' is not null' ]
            else: raise AssertionError
        if x is Param:
            return [ '(', col_name, ' ', op, ' ', x, ' or ',
                     col_name, ' is null and ', x, ' is null)' ]
        return [ col_name, ' ', op, ' ', x ]
    def _value(self, x):
        if isinstance(x, basestring): return "'%s'" % x.replace("'", "''")
        # if isinstance(x, date): pass
        # if isinstance(x, datetime): pass
        return x
    def _order_by(self):
        if not self.order: return []
        order = ', '.join('%s %s' % tuple(x) for x in self.order)
        return [ 'order by ',  order ]
    def _limit(self):
        if self.type in ('ROWSCOUNT', 'EXISTS') or not self.range: return []
        limit, offset = self.range
        result = [ 'limit ', limit ]
        if isinstance(offset, Param) or offset > 0:
            result.extend([ ' offset ', offset ])
        return result




class DBAPIConnection(object):
    Value = Value
    Param = Param
    def __init__(self, *args, **keyargs):
        self._dbapi_connection = self.dbapi_module.connect(*args, **keyargs)
        


            






