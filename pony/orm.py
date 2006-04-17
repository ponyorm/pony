# -*- coding: cp1251 -*-

###############################################################################

class Attribute(object):
    pass

class Optional(Attribute): pass
class Required(Attribute): pass
class Unique(Required): pass
class PrimaryKey(Unique): pass

class Relation(object): pass
class Set(Relation): pass
class List(Relation): pass

###############################################################################

class PersistentMeta(type):
    pass

###############################################################################

class Persistent(object):
    __metaclass__ = PersistentMeta










