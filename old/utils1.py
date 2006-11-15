# -*- coding: cp1251 -*-
import re
from sys import _getframe
from itertools import count

__all__ = ('uppercase_name', 'lowercase_name',
           'camelcase_name', 'mixedcase_name',)

###############################################################################

def error_method(*args, **kwargs):
    raise TypeError

class FrozenDict(dict):
    "Dictionary subclass with __hash__ method. Cannot be modified."
    def __init__(self, x):
        dict.__init__(self, x)
        self._hash = hash(tuple(sorted(self.iteritems())))
    __setitem__ = __delitem__ = clear = update = setdefault = pop = popitem \
        = error_method
    def __hash__(self):
        return self._hash

ident_re = re.compile(r'[A-Za-z_]\w*')
name_parts_re = re.compile(r'''
            [A-Z][A-Z0-9]+(?![a-z]) # ACRONYM
        |   [A-Z][a-z]*             # Capitalized or single capital
        |   [a-z+                   # all-lowercase
        |   [0-9]+                  # numbers
        |   _+                      # underscores
        ''', re.VERBOSE)

def split_name(name):
    "split_name('Some_FUNNYName') -> ['Some', 'FUNNY', 'Name']"
    if not ident_re.match(name):
        raise ValueError('Name is not correct Python identifier')
    list = name_parts_re.findall(name)
    if not (list[0].strip('_') and list[-1].strip('_')):
        raise ValueError('Name must not starting or ending with underscores')
    return [ s for s in list if s.strip('_') ]

def uppercase_name(name):
    "uppercase_name('Some_FUNNYName') -> 'SOME_FUNNY_NAME'"
    return '_'.join(s.upper() for s in split_name(name))

def lowercase_name(name):
    "uppercase_name('Some_FUNNYName') -> 'some_funny_name'"
    return '_'.join(s.lower() for s in split_name(name))

def camelcase_name(name):
    "uppercase_name('Some_FUNNYName') -> 'SomeFunnyName'"
    return ''.join(s.capitalize() for s in split_name(name))

def mixedcase_name(name):
    "mixedcase_name('Some_FUNNYName') -> 'someFunnyName'"
    list = split_name(name)
    return list[0].lower() + ''.join(s.capitalize() for s in list[1:])

def import_module(name):
    "import_module('a.b.c') -> <module a.b.c>"
    mod = __import__(name)
    components = name.split('.')
    for comp in components[1:]:
        mod = getattr(mod, comp)
    return mod

def get_next_index(depth):
    "Used internally by Attribute and PersistentMeta classes"
    dict = _getframe(depth + 1).f_locals
    counter = dict.setdefault('_pony_counter_', count())
    return counter.next()

system_stuff = [ '_pony_counter_' ]

def clear_system_stuff(dict):
    "Used internally by Attribute and PersistentMeta classes"
    for stuff in system_stuff:
        dict.pop(stuff, None)
