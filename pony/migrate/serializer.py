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

try:
    import enum
except ImportError:
    # No support on Python 2 if enum34 isn't installed.
    enum = None


class BaseSerializer(object):
    def __init__(self, value, ctx=None):
        self.value = value
        self.ctx = ctx

    def serialize(self):
        raise NotImplementedError


class BaseSequenceSerializer(BaseSerializer):
    def _format(self):
        raise NotImplementedError('Subclasses of BaseSequenceSerializer must implement the _format() method.')

    def serialize(self):
        imports = set()
        strings = []
        for item in self.value:
            item_string, item_imports = serializer_factory(item).serialize()
            imports.update(item_imports)
            strings.append(item_string)
        value = self._format()
        return value % (", ".join(strings)), imports


class BaseSimpleSerializer(BaseSerializer):
    def serialize(self):
        return repr(self.value), set()


class ByteTypeSerializer(BaseSerializer):
    def serialize(self):
        value_repr = repr(self.value)
        if PY2:
            # Prepend the `b` prefix since we're importing unicode_literals
            value_repr = 'b' + value_repr
        return value_repr, set()


class DatetimeSerializer(BaseSerializer):
    def serialize(self):
        if self.value.tzinfo is not None and self.value.tzinfo != utc:
            self.value = self.value.astimezone(utc)
        value_repr = repr(self.value).replace("<UTC>", "utc")
        imports = ["import datetime"]
        if self.value.tzinfo is not None:
            imports.append("from pony.migrate.utils import utc")
        return value_repr, set(imports)


class DateSerializer(BaseSerializer):
    def serialize(self):
        value_repr = repr(self.value)
        return value_repr, {"import datetime"}


class DecimalSerializer(BaseSerializer):
    def serialize(self):
        return repr(self.value), {"from decimal import Decimal"}


class DeconstructableSerializer(BaseSerializer):

    def _format_args(self, args, kwargs):
        imports = set()
        strings = []
        for arg in args:
            arg_string, arg_imports = serializer_factory(arg, ctx=self.ctx).serialize()
            strings.append(arg_string)
            imports.update(arg_imports)
        for kw, arg in sorted(kwargs.items()):
            arg_string, arg_imports = serializer_factory(arg, ctx=self.ctx).serialize()
            imports.update(arg_imports)
            strings.append("%s=%s" % (kw, arg_string))
        return ", ".join(strings), imports

    def serialize_deconstructed(self, path, args, kwargs):
        # TODO sometimes path is not required:
        # Required(Decimal) gives unused import 'import decimal'
        #
        name, imports = self._serialize_path(path)
        param, im = self._format_args(args, kwargs)
        imports.update(im)
        return "%s(%s)" % (name, param), imports

    def _serialize_path(self, path):
        module, name = path.rsplit(".", 1)
        if module == "pony.orm.core":
            imports = {'from pony import orm'}
            name = "orm.%s" % name
        elif module == "pony.migrate.diagram_ops":
            imports = {'from pony.migrate import diagram_ops as op'}
            name = "op.%s" % name
        else:
            imports = {"import %s" % module}
            name = path
        return name, imports

    def serialize(self):
        serialize_deconstructed = getattr(
            self.value, 'serialize_deconstructed', self.serialize_deconstructed
        )
        return serialize_deconstructed(*self.value.deconstruct())


class DictionarySerializer(BaseSerializer):
    def serialize(self):
        imports = set()
        strings = []
        for k, v in sorted(self.value.items()):
            k_string, k_imports = serializer_factory(k).serialize()
            v_string, v_imports = serializer_factory(v).serialize()
            imports.update(k_imports)
            imports.update(v_imports)
            strings.append((k_string, v_string))
        return "{%s}" % (", ".join("%s: %s" % (k, v) for k, v in strings)), imports


class EnumSerializer(BaseSerializer):
    def serialize(self):
        enum_class = self.value.__class__
        module = enum_class.__module__
        imports = {"import %s" % module}
        v_string, v_imports = serializer_factory(self.value.value).serialize()
        imports.update(v_imports)
        return "%s.%s(%s)" % (module, enum_class.__name__, v_string), imports


class FloatSerializer(BaseSimpleSerializer):
    def serialize(self):
        if math.isnan(self.value) or math.isinf(self.value):
            return 'float("{}")'.format(self.value), set()
        return super(FloatSerializer, self).serialize()


class FrozensetSerializer(BaseSequenceSerializer):
    def _format(self):
        return "frozenset([%s])"


class FunctionTypeSerializer(BaseSerializer):
    def serialize(self):
        if getattr(self.value, "__self__", None) and isinstance(self.value.__self__, type):
            klass = self.value.__self__
            module = klass.__module__
            return "%s.%s.%s" % (module, klass.__name__, self.value.__name__), {"import %s" % module}
        # Further error checking
        if self.value.__name__ == '<lambda>':
            result = self.value()
            if issubclass(result, core.Entity):
                return repr(result.__name__), set()
            result, im = serializer_factory(result, ctx=self.ctx).serialize()
            return "lambda: {}".format(result), im
        if self.value.__module__ is None:
            raise ValueError("Cannot serialize function %r: No module" % self.value)
        # Python 3 is a lot easier, and only uses this branch if it's not local.
        if getattr(self.value, "__qualname__", None) and getattr(self.value, "__module__", None):
            if "<" not in self.value.__qualname__:  # Qualname can include <locals>
                return "%s.%s" % \
                    (self.value.__module__, self.value.__qualname__), {"import %s" % self.value.__module__}
        # Python 2/fallback version
        module_name = self.value.__module__
        # Make sure it's actually there and not an unbound method
        module = import_module(module_name)
        if not hasattr(module, self.value.__name__):
            raise ValueError(
                "Could not find function %s in %s.\n"
                "Please note that due to Python 2 limitations, you cannot "
                "serialize unbound method functions (e.g. a method "
                "declared and used in the same class body). Please move "
                "the function into the main module body to use migrations."
                % (self.value.__name__, module_name)
            )
        # Needed on Python 2 only
        if module_name == '__builtin__':
            return self.value.__name__, set()
        return "%s.%s" % (module_name, self.value.__name__), {"import %s" % module_name}


class FunctoolsPartialSerializer(BaseSerializer):
    def serialize(self):
        imports = {'import functools'}
        # Serialize functools.partial() arguments
        func_string, func_imports = serializer_factory(self.value.func).serialize()
        args_string, args_imports = serializer_factory(self.value.args).serialize()
        keywords_string, keywords_imports = serializer_factory(self.value.keywords).serialize()
        # Add any imports needed by arguments
        imports.update(func_imports)
        imports.update(args_imports)
        imports.update(keywords_imports)
        return (
            "functools.partial(%s, *%s, **%s)" % (
                func_string, args_string, keywords_string,
            ),
            imports,
        )


class IterableSerializer(BaseSerializer):
    def serialize(self):
        imports = set()
        strings = []
        for item in self.value:
            item_string, item_imports = serializer_factory(item).serialize()
            imports.update(item_imports)
            strings.append(item_string)
        # When len(strings)==0, the empty iterable should be serialized as
        # "()", not "(,)" because (,) is invalid Python syntax.
        value = "(%s)" if len(strings) != 1 else "(%s,)"
        return value % (", ".join(strings)), imports


class AttributeSerializer(DeconstructableSerializer):
    def serialize(self):
        path, args, kwargs = self.value.deconstruct()
        # attr_name, path, args, kwargs = self.value.deconstruct()
        return self.serialize_deconstructed(path, args, kwargs)


# TODO remove staticmethods
# TODO iter entities in order, __id__

class EntitySerializer(DeconstructableSerializer):

    def serialize_deconstructed(self, meta, name, bases, cls_dict):
        if self.ctx.get('has_db_var'):
            return 'db.{}'.format(name), set()
        module = self.value.__module__
        imports = {"from  %s import %s" % (module, name)}
        # TODO
        assert self.ctx.get('ref_entities_by_name')
        return repr(name), imports
        # return 'lambda: {}'.format(name), imports


class EntityDeclarationSerializer(DeconstructableSerializer):

    def serialize_deconstructed(self, meta, name, bases, cls_dict):
        imports = set()
        _bases, im = self._serialize_bases(bases)
        imports.update(im)
        cls_dict = {k: v for k, v in cls_dict.items()
                    if self._filter_namespace(k)}
        lines = [
            'class {}({}):'.format(name, ', '.join(_bases))
        ]
        line = None
        for line in self._declare_attrs(cls_dict, imports):
            lines.append('    {}'.format(line))
        if line is None:
            lines.append('    pass')
        return '\n'.join(lines), imports

    def _filter_namespace(self, key):
        if key in (
            '__slots__', '__qualname__', '__module__', '_indexes_',
        ):
            return False
        return True

    def _declare_attrs(self, cls_dict, imports):
        # TODO handle PY3 in a special way
        attrs = []
        regular = []
        for k, v in cls_dict.items():
            if not isinstance(v, core.Attribute):
                regular.append((k, v))
            else:
                attrs.append((k, v))
        attrs = sorted(attrs, key=lambda tupl: tupl[1].id)
        # first go entity attrs
        for k, v in attrs:
            s, im = serializer_factory(v, ctx=self.ctx).serialize()
            imports.update(im)
            yield '{} = {}'.format(k, s)
        # then primary key
        if len(self.value._pk_attrs_) > 1:
            s, im = self._serialize_pk(cls_dict)
            imports.update(im)
            yield s
        # then regular attrs
        for k, v in sorted(regular, key=lambda tupl: tupl[0]):
            s, im = serializer_factory(v, ctx=self.ctx).serialize()
            imports.update(im)
            yield '{} = {}'.format(k, s)

    def _serialize_pk(self, cls_dict):
        imports = set()
        def pk_attrs():
            for pk in self.value._pk_attrs_:
                if pk.name in cls_dict:
                    yield pk.name
                else:
                    s, im = serializer_factory(pk, ctx=self.ctx).serialize()
                    imports.update(im)
                    yield s
        return "orm.PrimaryKey(%s)" % ', '.join(pk_attrs()), imports


    def _serialize_bases(self, bases):
        imports = set()
        ret = []
        for cls in bases:
            if not issubclass(cls, core.Entity):
                s, im = serializer_factory(cls).serialize()
                ret.append(s)
                imports.update(im)
                continue
            s, im = EntitySerializer(cls, ctx=self.ctx).serialize()
            imports.update(im)
            ret.append(s)

        return ret, imports


class RegexSerializer(BaseSerializer):
    def serialize(self):
        imports = {"import re"}
        regex_pattern, pattern_imports = serializer_factory(self.value.pattern).serialize()
        regex_flags, flag_imports = serializer_factory(self.value.flags).serialize()
        imports.update(pattern_imports)
        imports.update(flag_imports)
        args = [regex_pattern]
        if self.value.flags:
            args.append(regex_flags)
        return "re.compile(%s)" % ', '.join(args), imports


class SequenceSerializer(BaseSequenceSerializer):
    def _format(self):
        return "[%s]"


class SetSerializer(BaseSequenceSerializer):
    def _format(self):
        # Don't use the literal "{%s}" as it doesn't support empty set
        return "set([%s])"


class TextTypeSerializer(BaseSerializer):
    def serialize(self):
        value_repr = repr(self.value)
        if PY2:
            # Strip the `u` prefix since we're importing unicode_literals
            value_repr = value_repr[1:]
        return value_repr, set()


class TimedeltaSerializer(BaseSerializer):
    def serialize(self):
        return repr(self.value), {"import datetime"}


class TimeSerializer(BaseSerializer):
    def serialize(self):
        value_repr = repr(self.value)
        # if isinstance(self.value, datetime.time):
        #     value_repr = "datetime.%s" % value_repr
        return value_repr, {"import datetime"}


class TupleSerializer(BaseSequenceSerializer):
    def _format(self):
        # When len(value)==0, the empty tuple should be serialized as "()",
        # not "(,)" because (,) is invalid Python syntax.
        return "(%s)" if len(self.value) != 1 else "(%s,)"


class TypeSerializer(BaseSerializer):
    def serialize(self):
        # special_cases
        if self.value is decimal.Decimal:
            return 'Decimal', {'from decimal import Decimal'}
        module = self.value.__module__
        if module == builtins.__name__:
            return self.value.__name__, set()
        return "%s.%s" % (module, self.value.__name__), {"import %s" % module}


def serializer_factory(value, ctx=None):
    if ctx is None:
        ctx = {}
    if isinstance(value, core.Attribute):
        ctx = dict(ctx, ref_entities_by_name=True)
        return AttributeSerializer(value, ctx=ctx)
    if isinstance(value, core.EntityMeta):
        return EntitySerializer(value, ctx=ctx)
    if isinstance(value, type):
        return TypeSerializer(value, ctx=ctx)
    # Anything that knows how to deconstruct itself.
    if hasattr(value, 'deconstruct'):
        return DeconstructableSerializer(value, ctx=ctx)

    # Unfortunately some of these are order-dependent.
    if isinstance(value, frozenset):
        return FrozensetSerializer(value, ctx=ctx)
    if isinstance(value, list):
        return SequenceSerializer(value, ctx=ctx)
    if isinstance(value, set):
        return SetSerializer(value, ctx=ctx)
    if isinstance(value, tuple):
        return TupleSerializer(value, ctx=ctx)
    if isinstance(value, dict):
        return DictionarySerializer(value, ctx=ctx)
    if enum and isinstance(value, enum.Enum):
        return EnumSerializer(value, ctx=ctx)
    if isinstance(value, datetime.datetime):
        return DatetimeSerializer(value, ctx=ctx)
    if isinstance(value, datetime.date):
        return DateSerializer(value, ctx=ctx)
    if isinstance(value, datetime.time):
        return TimeSerializer(value, ctx=ctx)
    if isinstance(value, datetime.timedelta):
        return TimedeltaSerializer(value, ctx=ctx)
    if isinstance(value, float):
        return FloatSerializer(value, ctx=ctx)
    if isinstance(value, int_types + (bool, type(None))):
        return BaseSimpleSerializer(value, ctx=ctx)
    if isinstance(value, str if PY2 else bytes):
        return ByteTypeSerializer(value, ctx=ctx)
    if isinstance(value, unicode):
        return TextTypeSerializer(value, ctx=ctx)
    if isinstance(value, decimal.Decimal):
        return DecimalSerializer(value, ctx=ctx)
    if isinstance(value, functools.partial):
        return FunctoolsPartialSerializer(value, ctx=ctx)
    if isinstance(value, (types.FunctionType, types.BuiltinFunctionType)):
        ctx = dict(ctx, is_dynamic=True)
        return FunctionTypeSerializer(value, ctx=ctx)
    if isinstance(value, collections.Iterable):
        return IterableSerializer(value, ctx=ctx)
    if isinstance(value, (COMPILED_REGEX_TYPE, RegexObject)):
        return RegexSerializer(value, ctx=ctx)
    raise ValueError