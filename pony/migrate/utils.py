from pony.py23compat import PY2, int_types, unicode, basestring

import re, os, os.path, sys, datetime, inspect
from decimal import Decimal
from runpy import _run_code
import time as _time

from pony.utils import reraise


COMPILED_REGEX_TYPE = type(re.compile(''))


class RegexObject(object):
    def __init__(self, obj):
        self.pattern = obj.pattern
        self.flags = obj.flags

    def __eq__(self, other):
        return self.pattern == other.pattern and self.flags == other.flags


def get_migration_name_timestamp():
    return datetime.datetime.now().strftime("%Y%m%d_%H%M")


def get_func_args(func):
    if PY2:
        argspec = inspect.getargspec(func)
        return argspec.args[1:]  # ignore 'self'

    sig = inspect.signature(func)
    return [
        arg_name for arg_name, param in sig.parameters.items()
        if param.kind == inspect.Parameter.POSITIONAL_OR_KEYWORD
    ]


def module_dir(module):
    """
    Find the name of the directory that contains a module, if possible.

    Raise ValueError otherwise, e.g. for namespace packages that are split
    over several directories.
    """
    # Convert to list because _NamespacePath does not support indexing on 3.3.
    paths = list(getattr(module, '__path__', []))
    if len(paths) == 1:
        return paths[0]
    else:
        filename = getattr(module, '__file__', None)
        if filename is not None:
            return os.path.dirname(filename)
    raise ValueError("Cannot determine directory containing %s" % module)




_PROTECTED_TYPES = int_types + (
    type(None), float, Decimal, datetime.datetime, datetime.date, datetime.time
)


def is_protected_type(obj):
    """Determine if the object instance is of a protected type.

    Objects of protected types are preserved as-is when passed to
    force_text(strings_only=True).
    """
    return isinstance(obj, _PROTECTED_TYPES)


def force_text(s, encoding='utf-8', strings_only=False, errors='strict'):
    """
    Similar to smart_text, except that lazy instances are resolved to
    strings, rather than kept as lazy objects.

    If strings_only is True, don't convert (some) non-string-like objects.
    """
    # Handle the common case first for performance reasons.
    if issubclass(type(s), unicode):
        return s
    if strings_only and is_protected_type(s):
        return s
    try:
        if not issubclass(type(s), basestring):
            if not PY2:
                if isinstance(s, bytes):
                    s = unicode(s, encoding, errors)
                else:
                    s = unicode(s)
            elif hasattr(s, '__unicode__'):
                s = unicode(s)
            else:
                s = unicode(bytes(s), encoding, errors)
        else:
            # Note: We use .decode() here, instead of unicode(s, encoding,
            # errors), so that if s is a SafeBytes, it ends up being a
            # SafeText at the end.
            s = s.decode(encoding, errors)
    except UnicodeDecodeError as e:
        if isinstance(s, Exception):
            # If we get to here, the caller has passed in an Exception
            # subclass populated with non-ASCII bytestring data without a
            # working unicode method. Try to handle this without raising a
            # further exception by individually forcing the exception args
            # to unicode.
            s = ' '.join(force_text(arg, encoding, strings_only, errors)
                         for arg in s)
        else:
            raise
    return s


def upath(path):
    """
    Always return a unicode path.
    """
    if PY2 and not isinstance(path, unicode):
        fs_encoding = sys.getfilesystemencoding() or sys.getdefaultencoding()
        return path.decode(fs_encoding)
    return path


ZERO = datetime.timedelta(0)


class UTC(datetime.tzinfo):
    """
    UTC implementation taken from Python's docs.

    Used only when pytz isn't available.
    """

    def __repr__(self):
        return "<UTC>"

    def utcoffset(self, dt):
        return ZERO

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return ZERO

utc = UTC()


class FixedOffset(datetime.tzinfo):
    """
    Fixed offset in minutes east from UTC. Taken from Python's docs.

    Kept as close as possible to the reference version. __init__ was changed
    to make its arguments optional, according to Python's requirement that
    tzinfo subclasses can be instantiated without arguments.
    """

    def __init__(self, offset=None, name=None):
        if offset is not None:
            self.__offset = datetime.timedelta(minutes=offset)
        if name is not None:
            self.__name = name

    def utcoffset(self, dt):
        return self.__offset

    def tzname(self, dt):
        return self.__name

    def dst(self, dt):
        return ZERO



class ReferenceLocalTimezone(datetime.tzinfo):
    """
    Local time. Taken from Python's docs.

    Used only when pytz isn't available, and most likely inaccurate. If you're
    having trouble with this class, don't waste your time, just install pytz.

    Kept as close as possible to the reference version. __init__ was added to
    delay the computation of STDOFFSET, DSTOFFSET and DSTDIFF which is
    performed at import time in the example.

    Subclasses contain further improvements.
    """

    def __init__(self):
        self.STDOFFSET = datetime.timedelta(seconds=-_time.timezone)
        if _time.daylight:
            self.DSTOFFSET = datetime.timedelta(seconds=-_time.altzone)
        else:
            self.DSTOFFSET = self.STDOFFSET
        self.DSTDIFF = self.DSTOFFSET - self.STDOFFSET
        datetime.tzinfo.__init__(self)

    def utcoffset(self, dt):
        if self._isdst(dt):
            return self.DSTOFFSET
        else:
            return self.STDOFFSET

    def dst(self, dt):
        if self._isdst(dt):
            return self.DSTDIFF
        else:
            return ZERO

    def tzname(self, dt):
        return _time.tzname[self._isdst(dt)]

    def _isdst(self, dt):
        tt = (dt.year, dt.month, dt.day,
              dt.hour, dt.minute, dt.second,
              dt.weekday(), 0, 0)
        stamp = _time.mktime(tt)
        tt = _time.localtime(stamp)
        return tt.tm_isdst > 0


class LocalTimezone(ReferenceLocalTimezone):
    """
    Slightly improved local time implementation focusing on correctness.

    It still crashes on dates before 1970 or after 2038, but at least the
    error message is helpful.
    """

    def tzname(self, dt):
        is_dst = False if dt is None else self._isdst(dt)
        return _time.tzname[is_dst]

    def _isdst(self, dt):
        try:
            return super(LocalTimezone, self)._isdst(dt)
        except (OverflowError, ValueError) as exc:
            exc_type = type(exc)
            exc_value = exc_type(
                "Unsupported value: %r. You should install pytz." % dt)
            exc_value.__cause__ = exc
            if not hasattr(exc, '__traceback__'):
                exc.__traceback__ = sys.exc_info()[2]
            reraise(exc_type, exc_value, sys.exc_info()[2])



def get_fixed_timezone(offset):
    """
    Returns a tzinfo instance with a fixed offset from UTC.
    """
    if isinstance(offset, datetime.timedelta):
        offset = offset.seconds // 60
    sign = '-' if offset < 0 else '+'
    hhmm = '%02d%02d' % divmod(abs(offset), 60)
    name = sign + hhmm
    return FixedOffset(offset, name)


# In order to avoid accessing settings at compile time,
# wrap the logic in a function and cache the result.
def get_default_timezone():
    return LocalTimezone()


def deconstructible(klass):
    """
    Class decorator that allow the decorated class to be serialized
    by the migrations subsystem.
    """

    def __new__(cls, *args, **kwargs):
        # We capture the arguments to make returning them trivial
        obj = super(klass, cls).__new__(cls)
        obj._constructor_args = (args, kwargs)
        return obj

    def deconstruct(obj):
        """
        Returns a 3-tuple of class import path, positional arguments,
        and keyword arguments.
        """
        module_name = obj.__module__
        name = obj.__class__.__name__
        module = sys.modules[module_name]
        if not hasattr(module, name):
            raise ValueError("Could not find object %s in %s. "
                             "Please note that you cannot serialize things like inner classes. "
                             "Please move the object into the main module body to use migrations"
                             % (name, module_name))
        path = '.'.join((obj.__class__.__module__, name))
        args, kwargs = obj._constructor_args
        return path, args, kwargs

    klass.__new__ = staticmethod(__new__)
    klass.deconstruct = deconstruct
    return klass


def run_path(path):
    namespace = {}
    with open(path) as f:
        text = f.read()
    _run_code(text, namespace)
    return namespace