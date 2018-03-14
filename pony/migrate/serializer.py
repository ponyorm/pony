from __future__ import unicode_literals
from pony.py23compat import PY2, builtins, int_types, unicode

import collections
import datetime
import decimal
import functools
import math
import types
from importlib import import_module

from .utils import COMPILED_REGEX_TYPE, RegexObject, utc

from pony.orm import core
from pony.orm.decompiling import decompile
from pony.orm.asttranslation import ast2src

try:
    import enum
except ImportError:
    # No support on Python 2 if enum34 isn't installed.
    enum = None

def serialize(value, imports=None, initial_migration=False):
    serializer = Serializer(imports, initial_migration)
    return serializer.serialize(value)

def serialize_entity_declaration(entity, imports=None):
    serializer = Serializer(imports, True)
    return serializer.serialize_entity_declaration(entity)


class Serializer(object):
    def __init__(self, imports=None, initial_migration=False):
        self.imports = imports if imports is not None else set()
        self.initial_migration = initial_migration

    def serialize_basic(self, value, import_=None):
        if import_:
            self.imports.add(import_)
        return repr(value)

    def serialize_byte(self, value):
        # if PY2:
        #     # Prepend the `b` prefix since we're importing unicode_literals
        #     value_repr = 'b' + value_repr
        return repr(value)

    def serialize_datetime(self, value):
        if value.tzinfo is not None and value.tzinfo != utc:
            value = value.astimezone(utc)
        self.imports.add("import datetime")
        if value.tzinfo is not None:
            self.imports.add("from pony.migrate.utils import utc")
        return repr(value).replace("<UTC>", "utc")

    def serialize_deconstructable(self, value):
        path, args, kwargs = value.deconstruct()
        module, name = path.rsplit(".", 1)
        if module == "pony.orm.core":
            self.imports.add('from pony import orm')
            name = "orm.%s" % name
        elif module == "pony.migrate.diagram_ops":
            self.imports.add('from pony.migrate import diagram_ops as op')
            name = "op.%s" % name
        else:
            self.imports.add("import %s" % module)
            name = path

        params = [self.serialize(arg) for arg in args]
        params.extend('%s=%s' % (kw, self.serialize(arg)) for kw, arg in sorted(kwargs.items()))
        return "%s(%s)" % (name, ", ".join(params))

    def serialize_dict(self, value):
        return "{%s}" % (", ".join("%s: %s" % (self.serialize(k), self.serialize(v))
                         for k, v in sorted(value.items())))

    def serialize_enum(self, value):
        enum_class = value.__class__
        return "%s.%s(%s)" % (enum_class.__module__, enum_class.__name__, self.serialize(value.value))

    def serialize_float(self, value):
        if math.isnan(value) or math.isinf(value):
            return 'float("%s")' % value
        return self.serialize(value)

    def serialize_frozenset(self, value):
        return 'frozenset([%s])' % ', '.join(self.serialize(item) for item in value)

    def serialize_function(self, func):
        if getattr(func, '__self__', None) and isinstance(func.__self__, type):
            cls = func.__self__
            module = cls.__module__
            self.imports.add("import %s" % module)
            return "%s.%s.%s" % (module, cls.__name__, func.__name__)
        # Further error checking
        if func.__name__ == '<lambda>':
            func_ast, external_names, cells = decompile(func)
            # result, im = serializer_factory(result, ctx=self.ctx).serialize()
            return 'lambda: %s' % ast2src(func_ast)
        if func.__module__ is None:
            raise ValueError("Cannot serialize function %r: No module" % func)
        # Python 3 is a lot easier, and only uses this branch if it's not local.
        if getattr(func, "__qualname__", None) and getattr(func, "__module__", None):
            if "<" not in func.__qualname__:  # Qualname can include <locals>
                self.imports.add("import %s" % func.__module__)
                return "%s.%s" % (func.__module__, func.__qualname__)
        # Python 2/fallback version
        module_name = func.__module__
        # Make sure it's actually there and not an unbound method
        module = import_module(module_name)
        if not hasattr(module, func.__name__):
            raise ValueError(
                "Could not find function %s in %s.\n"
                "Please note that due to Python 2 limitations, you cannot "
                "serialize unbound method functions (e.g. a method "
                "declared and used in the same class body). Please move "
                "the function into the main module body to use migrations."
                % (func.__name__, module_name)
            )
        # Needed on Python 2 only
        if module_name == '__builtin__':
            return func.__name__
        self.imports.add("import %s" % module_name)
        return "%s.%s" % (module_name, func.__name__)

    def serialize_partial(self, value):
        self.imports.add('import functools')
        s = self.serialize
        return "functools.partial(%s, *%s, **%s)" % (s(value.func), s(value.args), s(value.keywords))

    def serialize_entity(self, entity):
        meta, name, bases, cls_dict = entity.deconstruct()
        return 'db.%s' % name if self.initial_migration else repr(name)

    def serialize_entity_declaration(self, entity):
        meta, name, bases, cls_dict = entity.deconstruct()
        _bases = []
        for cls in bases:
            if issubclass(cls, core.Entity):
                _bases.append(self.serialize_entity(cls))

        lines = [ 'class %s(%s):' % (name, ', '.join(_bases)) ]

        for name, value in cls_dict.items():
            if name in ('__slots__', '__qualname__', '__module__', '_indexes_'): continue
            if isinstance(value, (core.Attribute, types.MethodType, types.FunctionType,
                                  staticmethod, classmethod, property)): continue
            lines.append('%s = %s' % (name, self.serialize(value)))

        for attr in entity._new_attrs_:
            line = '%s = %s' % (attr.name, self.serialize(attr))
            if line in (
                'id = orm.PrimaryKey(int, auto=True)',
                "classtype = orm.Discriminator(str, column='classtype')"
            ): continue
            lines.append(line)

        if len(entity._pk_attrs_) > 1:
            lines.append('orm.PrimaryKey(%s)' % ', '.join(attr.name for attr in entity._pk_attrs_))

        if len(lines) == 1:
            lines.append('pass')
        return '\n    '.join(lines)

    def serialize_sequence(self, value):
        return '[%s]' % ', '.join(self.serialize(item) for item in value)

    def serialize_set(self, value):
        return 'set([%s])' % ', '.join(self.serialize(item) for item in value)

    def serialize_type(self, value):
        if value is decimal.Decimal:
            self.imports.add('from decimal import Decimal')
            return 'Decimal'
        module = value.__module__
        if module == builtins.__name__:
            return value.__name__
        self.imports.add("import %s" % module)
        return "%s.%s" % (module, value.__name__)

    def serialize_tuple(self, value):
        items = [ self.serialize(item) for item in value ]
        return '(%s)' % ', '.join(items) if len(items) != 1 else '(%s,)' % items[0]

    def serialize_unicode(self, value):
        value_repr = repr(value)
        if PY2:
            # Strip the `u` prefix since we're importing unicode_literals
            value_repr = value_repr[1:]
        return value_repr

    def serialize_regex(self, value):
        self.imports.add('import re')
        args = [ serialize(value.pattern) ]
        if value.flags:
            args.append(serialize(value.flags))
        return "re.compile(%s)" % ', '.join(args)

    def serialize(self, value):
        if isinstance(value, core.Attribute):
            return self.serialize_deconstructable(value)
        if isinstance(value, core.EntityMeta):
            return self.serialize_entity(value)
        if isinstance(value, type):
            return self.serialize_type(value)
        # Anything that knows how to deconstruct itself.
        if hasattr(value, 'deconstruct'):
            return self.serialize_deconstructable(value)

        # Unfortunately some of these are order-dependent.
        if isinstance(value, frozenset):
            return self.serialize_frozenset(value)
        if isinstance(value, list):
            return self.serialize_sequence(value)
        if isinstance(value, set):
            return self.serialize_set(value)
        if isinstance(value, tuple):
            return self.serialize_tuple(value)
        if isinstance(value, dict):
            return self.serialize_dict(value)
        if enum and isinstance(value, enum.Enum):
            return self.serialize_enum(value)
        if isinstance(value, datetime.datetime):
            return self.serialize_datetime(value)
        if isinstance(value, (datetime.date, datetime.time, datetime.timedelta)):
            return self.serialize_basic(value, 'import datetime')
        if isinstance(value, float):
            return self.serialize_float(value)
        if isinstance(value, int_types + (bool, type(None))):
            return self.serialize_basic(value)
        if isinstance(value, str if PY2 else bytes):
            return self.serialize_byte(value)
        if isinstance(value, unicode):
            return self.serialize_unicode(value)
        if isinstance(value, decimal.Decimal):
            return self.serialize_basic(value, 'from decimal import Decimal')
        if isinstance(value, functools.partial):
             return self.serialize_partial(value)
        if isinstance(value, (types.FunctionType, types.BuiltinFunctionType)):
            return self.serialize_function(value)
        if isinstance(value, collections.Iterable):
            return self.serialize_tuple(value)
        if isinstance(value, (COMPILED_REGEX_TYPE, RegexObject)):
            return self.serialize_regex(value)
        raise TypeError(type(value))
